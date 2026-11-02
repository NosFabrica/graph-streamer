package com.nosfabrica.graphstreamer.projection

import com.nosfabrica.graphstreamer.Mirrored
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip40Expiration.expiration

/**
 * The outgoing edges the graph holds per author, and the Redis set each one is copied into.
 * Names are a contract with brainstorm_server and the GrapeRank worker: changing one is a
 * cross-repo change.
 */
enum class Edge(
    val relType: String,
) {
    /** Kind 3. Reverse set `followed_by:<target>`. */
    FOLLOWS("FOLLOWS"),

    /** Kind 10000, public rows only. Reverse set `muted_by:<target>`. */
    MUTES("MUTES"),

    /** Kind 1984, user-level reports only. Reverse set `reported_by:<target>`. */
    REPORTS("REPORTS"),

    /** Kind 10040, public rows only. Forward set `designates:<author>` (brainstorm_server ADR 0007). */
    DESIGNATES("DESIGNATES"),
}

/**
 * Pure: an author's surviving events in, the edges they should have out. The rules are
 * brainstorm_server's (`process_strfry_event.py`, `report_graph_service.py`,
 * `designation_service.py`), so a cutover changes no edge that the server would have written.
 */
object GraphRules {
    private val HEX64 = Regex("^[0-9a-f]{64}$")
    private val DESIGNATION_TAG = Regex("^\\d+(:.*)?$")

    /** The `p` tags' pubkeys, lowercased, deduplicated, in order; anything that isn't 64-hex is dropped. */
    fun pTagPubkeys(tags: Array<Array<String>>): List<String> {
        val seen = LinkedHashSet<String>()
        for (tag in tags) {
            if (tag.size > 1 && tag[0] == "p") {
                val pubkey = tag[1].lowercase()
                if (HEX64.matches(pubkey)) seen.add(pubkey)
            }
        }
        return seen.toList()
    }

    /** NIP-56: a report against a note or media carries an `e` tag; only `p`-only (user) reports count. */
    fun reportTargets(event: Event): List<String> = if (event.tags.any { it.isNotEmpty() && it[0] == "e" }) emptyList() else pTagPubkeys(event.tags)

    /** A kind-10040's public designation rows (`<kind>[:<tag>]`, pubkey, …): providers, never the observer. */
    fun designatedPubkeys(
        tags: Array<Array<String>>,
        observer: String,
    ): List<String> {
        val seen = LinkedHashSet<String>()
        for (tag in tags) {
            if (tag.size < 2 || !DESIGNATION_TAG.matches(tag[0])) continue
            val pubkey = tag[1].lowercase()
            if (HEX64.matches(pubkey) && pubkey != observer) seen.add(pubkey)
        }
        return seen.toList()
    }

    /** NIP-40: an event whose `expiration` is at or before [now] is gone, swept or not. */
    fun isExpired(
        event: Event,
        now: Long,
    ): Boolean = event.expiration()?.let { it <= now } ?: false

    /**
     * The edges [author] should have, from the events the mirror still holds for them. Deleted,
     * vanished and superseded events are already gone from the store; expired ones may not be
     * swept yet, so they are dropped here. No event of a kind means no edges of that kind.
     */
    fun desired(
        author: String,
        events: List<Event>,
        now: Long,
    ): Map<Edge, Set<String>> {
        val live = events.filter { it.pubKey == author && !isExpired(it, now) }

        fun newest(kind: Int) = live.filter { it.kind == kind }.maxWithOrNull(compareBy<Event> { it.createdAt }.thenByDescending { it.id })

        return mapOf(
            Edge.FOLLOWS to newest(Mirrored.FOLLOWS)?.let { pTagPubkeys(it.tags).toSet() }.orEmpty(),
            Edge.MUTES to newest(Mirrored.MUTES)?.let { pTagPubkeys(it.tags).toSet() }.orEmpty(),
            Edge.DESIGNATES to newest(Mirrored.DESIGNATIONS)?.let { designatedPubkeys(it.tags, author).toSet() }.orEmpty(),
            Edge.REPORTS to live.filter { it.kind == Mirrored.REPORTS }.flatMapTo(LinkedHashSet()) { reportTargets(it) },
        )
    }
}
