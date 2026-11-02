package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.store.DirtyAuthor
import com.nosfabrica.graphstreamer.store.EdgeChange
import com.nosfabrica.graphstreamer.store.GraphSink
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync

/** The Redis state in memory, with the same generation semantics as the Lua clear. */
class FakeState : StreamerState {
    val dirty = LinkedHashMap<String, Long>()
    var audit = mutableMapOf<String, String>()

    override fun markDirty(pubkeys: Collection<String>) {
        for (pk in pubkeys) dirty[pk] = (dirty[pk] ?: 0) + 1
    }

    override fun dirty(limit: Int) = dirty.entries.take(limit).map { DirtyAuthor(it.key, it.value) }

    override fun clearDirty(authors: List<DirtyAuthor>): Int = authors.count { a -> (dirty[a.pubkey] == a.generation).also { if (it) dirty.remove(a.pubkey) } }

    override fun dirtyCount() = dirty.size.toLong()

    override fun lastAuditFinishedAt() = audit["finished_at"]?.toLong()

    override fun putAuditReport(report: Map<String, String>) {
        audit.putAll(report)
    }
}

/** The graph in memory: author → edge → targets. */
class FakeGraph : GraphSink {
    val edges = HashMap<String, MutableMap<Edge, MutableSet<String>>>()
    val applied = ArrayList<EdgeChange>()

    override fun current(authors: List<String>): Map<String, Map<Edge, Set<String>>> =
        authors.mapNotNull { a -> edges[a]?.let { m -> a to m.filterValues { it.isNotEmpty() }.mapValues { it.value.toSet() } } }.toMap()

    override fun apply(changes: List<EdgeChange>) {
        applied += changes
        for (c in changes) {
            val set = edges.getOrPut(c.author) { mutableMapOf() }.getOrPut(c.edge) { mutableSetOf() }
            set.removeAll(c.removed)
            set.addAll(c.added)
        }
    }

    fun of(
        author: String,
        edge: Edge,
    ): Set<String> = edges[author]?.get(edge).orEmpty()
}

/** A key that signs real events, so the store's id and author checks see what a relay would send. */
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
