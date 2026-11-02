package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/** An author waiting to be recomputed from the relay, and the generation it was marked at. */
data class DirtyAuthor(
    val pubkey: String,
    val generation: Long,
)

/**
 * The service's own small state: Redis in production.
 *
 * The dirty set holds authors to recompute from the relay: someone whose kind 5 or kind 62
 * arrived, or whose events an audit found the relay no longer holds. A mark is cleared only
 * after the author is written, and only if nobody marked them again meanwhile (the generation
 * still matches), so a crash or a mark during a recompute leaves them marked.
 */
interface StreamerState {
    fun markDirty(pubkeys: Collection<String>)

    /** Up to [limit] marked authors, in no particular order (one that keeps failing can't block the rest). */
    fun dirty(limit: Int): List<DirtyAuthor>

    /** Clears each author whose generation is still the one read; returns how many were cleared. */
    fun clearDirty(authors: List<DirtyAuthor>): Int

    fun dirtyCount(): Long

    fun lastAuditFinishedAt(): Long?

    /** Where the live stream resumes after a restart: the last `since` it would have reconnected from. */
    fun liveSince(): Long?

    fun putLiveSince(since: Long)

    fun putAuditReport(report: Map<String, String>)
}

/** One author's edges of one type moving from what the graph holds to what their events say. */
data class EdgeChange(
    val author: String,
    val edge: Edge,
    val desired: Set<String>,
    val added: Set<String>,
    val removed: Set<String>,
)

/**
 * One author's write: the edge changes, and, when [bookkeepingChanged], the list heads and report
 * ids of [target] to record on their node.
 */
data class AuthorChange(
    val author: String,
    val edges: List<EdgeChange>,
    val target: AuthorState,
    val bookkeepingChanged: Boolean,
)

/** The graph: Neo4j edges and bookkeeping, plus the Redis sets the GrapeRank worker reads. */
interface GraphStore {
    /** What the graph holds for each of [authors]; an author it knows nothing of is absent. */
    fun read(authors: List<String>): Map<String, AuthorState>

    /** Writes the changes: Neo4j in one transaction, then the Redis copies. */
    fun write(changes: List<AuthorChange>)

    /** How many authors' [kind] list was created in `[since, until]`. */
    fun countLists(
        kind: ListKind,
        since: Long,
        until: Long,
    ): Int

    /** The `(created_at, id)` of every [kind] list created in `[since, until]`. */
    fun lists(
        kind: ListKind,
        since: Long,
        until: Long,
    ): List<IdAndTime>

    /** The authors whose recorded [kind] list is one of [ids]. */
    fun authorsOfLists(
        kind: ListKind,
        ids: List<String>,
    ): List<String>

    /** Every recorded report, with its author. Reports are few, so this is read whole. */
    fun reports(): List<Pair<String, IdAndTime>>

    /** Creates the indexes the audit reads lists through. */
    fun ensureIndexes()
}

/** The relay, read by author. */
interface AuthorSource {
    /** Everything the relay holds for [authors] in the graph kinds. Throws [IncompleteRead] unless the relay drained. */
    suspend fun currentEvents(authors: List<String>): List<Event>
}

/** A read that did not finish: acting on it would read as deletions. */
class IncompleteRead(
    message: String,
) : Exception(message)
