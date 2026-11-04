package com.nosfabrica.graphstreamer.projection

import com.nosfabrica.graphstreamer.RelayFilters
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip40Expiration.expiration

/**
 * The outgoing edges the graph holds per author. Names are a contract with brainstorm_server
 * and the GrapeRank worker: changing one is a cross-repo change.
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
 * The replaceable lists. Each author has at most one of each; the graph records which event it
 * is on the author's node (`<prop>_event`, `<prop>_at`), and all of that author's edges of
 * [edge] come from it.
 */
enum class ListKind(
    val kind: Int,
    val edge: Edge,
    val prop: String,
) {
    FOLLOWS(3, Edge.FOLLOWS, "follows"),
    MUTES(10000, Edge.MUTES, "mutes"),
    DESIGNATIONS(10040, Edge.DESIGNATES, "designations"),
    ;

    companion object {
        fun of(kind: Int) = entries.firstOrNull { it.kind == kind }
    }
}

/** The event a list's edges came from. */
data class ListHead(
    val id: String,
    val createdAt: Long,
)

/**
 * Everything the graph holds for one author: their edges, the list events those came from, and
 * every report event of theirs (note reports too, which make no edge: the audit compares ids).
 */
data class AuthorState(
    val edges: Map<Edge, Set<String>> = emptyMap(),
    val heads: Map<ListKind, ListHead> = emptyMap(),
    /** Report event id → created_at. */
    val reports: Map<String, Long> = emptyMap(),
) {
    fun edges(edge: Edge): Set<String> = edges[edge].orEmpty()
}

/**
 * Pure: events in, an author's state out. The parsing rules are brainstorm_server's
 * (`process_strfry_event.py`, `report_graph_service.py`, `designation_service.py`).
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

    fun listTargets(
        kind: ListKind,
        event: Event,
    ): Set<String> =
        when (kind) {
            ListKind.FOLLOWS, ListKind.MUTES -> pTagPubkeys(event.tags).toSet()
            ListKind.DESIGNATIONS -> designatedPubkeys(event.tags, event.pubKey).toSet()
        }

    /** NIP-40: an event whose `expiration` is at or before [now] is gone. */
    fun isExpired(
        event: Event,
        now: Long,
    ): Boolean = event.expiration()?.let { it <= now } ?: false

    /** NIP-01: the newer list wins; on the same second, the lower id. */
    fun isNewer(
        event: Event,
        head: ListHead?,
    ): Boolean = head == null || event.createdAt > head.createdAt || (event.createdAt == head.createdAt && event.id < head.id)

    /**
     * [current] after [events] arrive: a list replaces the one held only if it is newer, and a
     * report adds its targets. Nothing is taken away here: a deletion reaches the graph as a
     * recompute ([fromRelay]), never as an event to apply.
     */
    fun apply(
        author: String,
        current: AuthorState,
        events: List<Event>,
        now: Long,
    ): AuthorState {
        val live = events.filter { it.pubKey == author && !isExpired(it, now) }
        val edges = current.edges.toMutableMap()
        val heads = current.heads.toMutableMap()
        val reports = current.reports.toMutableMap()

        for (kind in ListKind.entries) {
            val newest = live.filter { it.kind == kind.kind }.maxWithOrNull(compareBy<Event> { it.createdAt }.thenByDescending { it.id }) ?: continue
            if (isNewer(newest, heads[kind])) {
                heads[kind] = ListHead(newest.id, newest.createdAt)
                edges[kind.edge] = listTargets(kind, newest)
            }
        }

        val newReports = live.filter { it.kind == RelayFilters.REPORTS && it.id !in reports }
        if (newReports.isNotEmpty()) {
            val targets = current.edges(Edge.REPORTS).toMutableSet()
            for (report in newReports) {
                reports[report.id] = report.createdAt
                targets += reportTargets(report)
            }
            edges[Edge.REPORTS] = targets
        }

        return AuthorState(edges, heads, reports)
    }

    /** Why [apply] would not record [event] on top of [held]; for logs. */
    fun whyNotRecorded(
        event: Event,
        held: AuthorState,
        now: Long,
    ): String {
        if (isExpired(event, now)) return "expired at ${event.expiration()}"
        val kind = ListKind.entries.firstOrNull { it.kind == event.kind } ?: return "superseded in the same batch"
        val head = held.heads[kind] ?: return "superseded in the same batch"
        return if (!isNewer(event, head)) "the graph holds a newer list (${head.createdAt} ${head.id.take(12)} vs ${event.createdAt})" else "superseded in the same batch"
    }

    /**
     * What [author] should have, given everything the relay holds for them now. The relay has
     * already applied deletions, vanishes and expiries, so what it serves is what survives.
     */
    fun fromRelay(
        author: String,
        events: List<Event>,
        now: Long,
    ): AuthorState = apply(author, AuthorState(), events, now)
}
