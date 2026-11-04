package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListKind
import com.nosfabrica.graphstreamer.store.AuthorChange
import com.nosfabrica.graphstreamer.store.AuthorSource
import com.nosfabrica.graphstreamer.store.GraphStore
import com.nosfabrica.graphstreamer.store.IncompleteRead
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/** The state node in memory. */
class FakeState : StreamerState {
    val audit = mutableMapOf<String, String>()

    override fun lastAuditFinishedAt() = audit["finished_at"]?.toLong()

    override fun lastAuditEndedAt() = audit["ended_at"]?.toLong()

    var liveSince: Long? = null

    override fun liveSince() = liveSince

    override fun putLiveSince(since: Long) {
        liveSince = since
    }

    override fun putAuditReport(report: Map<String, String>) {
        audit.putAll(report)
    }
}

/** The graph in memory, written the way Neo4jRedisGraph writes it: edge diffs, then the target's bookkeeping. */
class FakeGraph : GraphStore {
    val authors = HashMap<String, AuthorState>()
    val writes = ArrayList<AuthorChange>()

    override fun read(authors: List<String>) = authors.mapNotNull { a -> this.authors[a]?.let { a to it } }.toMap()

    override fun write(changes: List<AuthorChange>) {
        writes += changes
        for (c in changes) {
            val held = authors[c.author] ?: AuthorState()
            val edges = held.edges.mapValues { it.value.toMutableSet() }.toMutableMap()
            for (e in c.edges) {
                val set = edges.getOrPut(e.edge) { mutableSetOf() }
                set.removeAll(e.removed)
                set.addAll(e.added)
            }
            authors[c.author] =
                if (c.bookkeepingChanged) {
                    AuthorState(edges, c.target.heads, c.target.reports)
                } else {
                    held.copy(edges = edges)
                }
        }
    }

    fun edges(
        author: String,
        edge: Edge,
    ): Set<String> = authors[author]?.edges(edge).orEmpty()

    override fun countLists(
        kind: ListKind,
        since: Long,
        until: Long,
    ) = lists(kind, since, until).size

    override fun lists(
        kind: ListKind,
        since: Long,
        until: Long,
    ) = authors.values.mapNotNull { it.heads[kind] }.filter { it.createdAt in since..until }.map { IdAndTime(it.createdAt, it.id) }

    override fun authorsOfLists(
        kind: ListKind,
        ids: List<String>,
    ) = authors.filterValues { it.heads[kind]?.id in ids }.keys.toList()

    override fun reports() = authors.flatMap { (a, s) -> s.reports.map { (id, at) -> a to IdAndTime(at, id) } }

    override fun ensureIndexes() = Unit
}

/** The relay: what it serves for an author is whatever this holds (deletions already applied). */
class FakeRelay : AuthorSource {
    val events = ArrayList<Event>()
    var drains = true
    var reads = 0

    override suspend fun currentEvents(authors: List<String>): List<Event> {
        reads++
        if (!drains) throw IncompleteRead("connection dropped")
        return events.filter { it.pubKey in authors && it.kind in RelayFilters.GRAPH_KINDS }
    }
}

/** A key that signs real events, so ids and authors are what a relay would send. */
class Signer {
    private val signer = NostrSignerSync()
    val pubkey: String = signer.pubKey

    fun event(
        kind: Int,
        tags: List<List<String>> = emptyList(),
        createdAt: Long = System.currentTimeMillis() / 1000,
        content: String = "",
    ): Event = signer.sign(createdAt, kind, tags.map { it.toTypedArray() }.toTypedArray(), content)
}

fun hex(n: Int): String = n.toString(16).padStart(64, '0')
