package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/**
 * The service's own small state: where the live stream resumes after a restart, and the last
 * audit's report. One node in the same Neo4j as the graph in production ([Neo4jStreamerState]).
 */
interface StreamerState {
    /** When the last complete audit finished, epoch seconds: the next is due [Config.auditEveryHours] later. */
    fun lastAuditFinishedAt(): Long?

    /** When the last audit ended, complete or not: an incomplete one is retried [Config.auditRetryMinutes] later. */
    fun lastAuditEndedAt(): Long?

    /** Where the live stream resumes after a restart: the last `since` it would have reconnected from. */
    fun liveSince(): Long?

    fun putLiveSince(since: Long)

    fun putAuditReport(report: Map<String, String>)

    /** When Redis was last reconciled with Neo4j, epoch seconds. */
    fun lastRedisReconcileAt(): Long?

    fun putRedisReport(report: Map<String, String>)
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

    /** Creates the pubkey constraint every write merges on, and the indexes the audit reads lists through. */
    fun ensureIndexes()

    /** Replays the Redis writes a crash cut off (the outbox); returns how many authors. */
    fun repairPendingRedis(): Int

    /**
     * Makes every Redis set match Neo4j: [underLock] runs each batch's compare-and-replace under
     * the writer's lock, so a set is never replaced with members a concurrent write just changed.
     */
    fun reconcileRedis(underLock: (() -> Unit) -> Unit): RedisRepair
}

/** What [GraphStore.reconcileRedis] did: keys checked, sets replaced, sets deleted (no edge left). */
data class RedisRepair(
    val keys: Long,
    val replaced: Long,
    val deleted: Long,
)

/** The relay, read by author. */
interface AuthorSource {
    /** Everything the relay holds for [authors] in the graph kinds. Throws [IncompleteRead] unless the relay drained. */
    suspend fun currentEvents(authors: List<String>): List<Event>
}

/** A read that did not finish: acting on it would read as deletions. */
class IncompleteRead(
    message: String,
) : Exception(message)
