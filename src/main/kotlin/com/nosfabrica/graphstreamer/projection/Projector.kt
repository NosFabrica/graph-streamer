package com.nosfabrica.graphstreamer.projection

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.store.AuthorChange
import com.nosfabrica.graphstreamer.store.AuthorSource
import com.nosfabrica.graphstreamer.store.EdgeChange
import com.nosfabrica.graphstreamer.store.GraphStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * The only writer to the graph. Two ways in:
 *
 * - [apply]: lists and reports as they arrive (live, or downloaded by an audit). A list replaces
 *   the one the graph holds only if it is newer; a report adds its targets.
 * - [recompute]: authors an audit found the relay no longer holds an event of are rebuilt from
 *   what the relay holds now. The relay has already applied the deletion, the vanish or the
 *   expiry, so this is how anything leaves the graph.
 *
 * Every write is read-compute-write under one lock, so the two never interleave on an author.
 * With [dryRun] the changes are counted and logged, and the graph is not touched.
 */
class Projector(
    private val graph: GraphStore,
    private val source: AuthorSource,
    private val dryRun: Boolean,
    /** Authors recomputed from the relay per read. */
    val batchSize: Int,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(Projector::class.java)
    private val lock = Mutex()

    val eventsApplied = AtomicLong()
    val authorsRecomputed = AtomicLong()
    val authorsWritten = AtomicLong()
    val edgesAdded = Edge.entries.associateWith { AtomicLong() }
    val edgesRemoved = Edge.entries.associateWith { AtomicLong() }

    /**
     * Applies [events]; returns the ones the graph did not record, with why: an audit downloads
     * those again every time, so it counts them.
     */
    suspend fun apply(events: List<Event>): List<Ignored> {
        val graphEvents = events.filter { it.kind in RelayFilters.GRAPH_KINDS }.distinctBy { it.id }
        if (graphEvents.isEmpty()) return emptyList()
        val byAuthor = graphEvents.groupBy { it.pubKey }
        val ignored = ArrayList<Ignored>()
        lock.withLock {
            val current = graph.read(byAuthor.keys.toList())
            val now = clock()
            commit(
                byAuthor.mapNotNull { (author, mine) ->
                    val held = current[author] ?: AuthorState()
                    val target = GraphRules.apply(author, held, mine, now)
                    for (e in mine) {
                        val recorded = target.heads.values.any { it.id == e.id } || e.id in target.reports
                        if (!recorded) ignored += Ignored(e, GraphRules.whyNotRecorded(e, held, now))
                    }
                    change(author, held, target)
                },
            )
            eventsApplied.addAndGet(graphEvents.size.toLong())
        }
        return ignored
    }

    /** An event [apply] did not record. */
    data class Ignored(
        val event: Event,
        val reason: String,
    )

    /**
     * Rebuilds [authors] (at most [batchSize]) from what the relay holds for them now; returns how
     * many. Throws, writing nothing, unless the relay's answer drained.
     */
    suspend fun recompute(authors: List<String>): Int =
        lock.withLock {
            if (authors.isEmpty()) return@withLock 0
            // Throws IncompleteRead unless the relay drained: half an answer would read as deletions.
            val byAuthor = source.currentEvents(authors).groupBy { it.pubKey }
            val current = graph.read(authors)
            val now = clock()
            commit(
                authors.mapNotNull { author ->
                    change(author, current[author] ?: AuthorState(), GraphRules.fromRelay(author, byAuthor[author].orEmpty(), now))
                },
            )
            authorsRecomputed.addAndGet(authors.size.toLong())
            authors.size
        }

    private fun change(
        author: String,
        held: AuthorState,
        target: AuthorState,
    ): AuthorChange? {
        val edges =
            Edge.entries.mapNotNull { edge ->
                val want = target.edges(edge)
                val have = held.edges(edge)
                val added = want - have
                val removed = have - want
                if (added.isEmpty() && removed.isEmpty()) null else EdgeChange(author, edge, want, added, removed)
            }
        val bookkeepingChanged = held.heads != target.heads || held.reports != target.reports
        return if (edges.isEmpty() && !bookkeepingChanged) null else AuthorChange(author, edges, target, bookkeepingChanged)
    }

    private fun commit(changes: List<AuthorChange>) {
        if (changes.isEmpty()) return
        for (c in changes) {
            for (e in c.edges) {
                edgesAdded.getValue(e.edge).addAndGet(e.added.size.toLong())
                edgesRemoved.getValue(e.edge).addAndGet(e.removed.size.toLong())
            }
        }
        authorsWritten.addAndGet(changes.size.toLong())
        if (dryRun) {
            changes.take(DRY_RUN_SAMPLE).forEach { c ->
                log.debug("dry-run {}: {}", c.author, c.edges.joinToString { "${it.edge} +${it.added.size} -${it.removed.size}" })
            }
        } else {
            graph.write(changes)
        }
    }

    fun stats(): String =
        "applied=${eventsApplied.get()} recomputed=${authorsRecomputed.get()} authorsWritten=${authorsWritten.get()} " +
            Edge.entries.joinToString(" ") { "${it.name.lowercase()}=+${edgesAdded.getValue(it).get()}/-${edgesRemoved.getValue(it).get()}" } +
            if (dryRun) " (dry run)" else ""

    companion object {
        private const val DRY_RUN_SAMPLE = 5
    }
}
