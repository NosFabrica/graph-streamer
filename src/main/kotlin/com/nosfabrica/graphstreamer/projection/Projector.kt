package com.nosfabrica.graphstreamer.projection

import com.nosfabrica.graphstreamer.Mirrored
import com.nosfabrica.graphstreamer.mirror.Mirror
import com.nosfabrica.graphstreamer.store.EdgeChange
import com.nosfabrica.graphstreamer.store.GraphSink
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Turns dirty authors into graph writes. Each author is recomputed from what the mirror holds
 * for them now ([GraphRules.desired]) and diffed against what the graph holds, so the same code
 * handles a new list, a deletion, a vanish, an expiry and an audit's removal, and replaying it
 * is harmless.
 *
 * With [dryRun] the diffs are counted and logged, and the graph is not touched.
 */
class Projector(
    private val mirror: Mirror,
    private val state: StreamerState,
    private val graph: GraphSink,
    private val dryRun: Boolean,
    private val batchSize: Int,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(Projector::class.java)

    val authorsProjected = AtomicLong()
    val edgesAdded = Edge.entries.associateWith { AtomicLong() }
    val edgesRemoved = Edge.entries.associateWith { AtomicLong() }

    /** Projects one batch of dirty authors; returns how many there were (0: nothing to do). */
    suspend fun projectOnce(): Int {
        // Read the dirty set and the store with no write in flight: a write that lands after
        // this bumps its authors' generation, so they stay dirty for the next batch.
        val (dirty, events) =
            mirror.consistent {
                val dirty = state.dirty(batchSize)
                val events =
                    if (dirty.isEmpty()) {
                        emptyList()
                    } else {
                        mirror.store.query<Event>(Filter(authors = dirty.map { it.pubkey }, kinds = Mirrored.GRAPH_KINDS))
                    }
                dirty to events
            }
        if (dirty.isEmpty()) return 0

        val authors = dirty.map { it.pubkey }
        val byAuthor = events.groupBy { it.pubKey }
        val now = clock()
        val current = graph.current(authors)

        val changes =
            authors.flatMap { author ->
                val desired = GraphRules.desired(author, byAuthor[author].orEmpty(), now)
                val held = current[author].orEmpty()
                Edge.entries.mapNotNull { edge ->
                    val want = desired[edge].orEmpty()
                    val have = held[edge].orEmpty()
                    val added = want - have
                    val removed = have - want
                    if (added.isEmpty() && removed.isEmpty()) null else EdgeChange(author, edge, want, added, removed)
                }
            }

        for (c in changes) {
            edgesAdded.getValue(c.edge).addAndGet(c.added.size.toLong())
            edgesRemoved.getValue(c.edge).addAndGet(c.removed.size.toLong())
        }

        if (dryRun) {
            changes.take(DRY_RUN_SAMPLE).forEach {
                log.debug("dry-run {} {}: +{} -{}", it.author, it.edge, it.added.size, it.removed.size)
            }
        } else {
            graph.apply(changes)
        }

        state.clearDirty(dirty)
        authorsProjected.addAndGet(authors.size.toLong())
        return authors.size
    }

    fun stats(): String =
        "authors=${authorsProjected.get()} " +
            Edge.entries.joinToString(" ") { "${it.name.lowercase()}=+${edgesAdded.getValue(it).get()}/-${edgesRemoved.getValue(it).get()}" } +
            if (dryRun) " (dry run)" else ""

    companion object {
        private const val DRY_RUN_SAMPLE = 5
    }
}
