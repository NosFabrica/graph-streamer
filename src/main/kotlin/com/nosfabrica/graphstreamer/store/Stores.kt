package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.Edge

/** An author waiting to be projected, and the generation it was marked at. */
data class DirtyAuthor(
    val pubkey: String,
    val generation: Long,
)

/**
 * The service's own small, durable state: Redis in production.
 *
 * The dirty set is what makes the projection crash-safe. An author is marked BEFORE their
 * events are written to the mirror, and cleared only after their edges are written, and only if
 * nobody marked them again meanwhile (the generation still matches). A crash anywhere leaves
 * them marked, and the next start projects them.
 */
interface StreamerState {
    fun markDirty(pubkeys: Collection<String>)

    /** Up to [limit] marked authors, in no particular order (one that keeps failing can't block the rest). */
    fun dirty(limit: Int): List<DirtyAuthor>

    /** Clears each author whose generation is still the one read; returns how many were cleared. */
    fun clearDirty(authors: List<DirtyAuthor>): Int

    fun dirtyCount(): Long

    fun lastAuditFinishedAt(): Long?

    fun putAuditReport(report: Map<String, String>)
}

/** The graph: Neo4j edges plus the Redis sets the GrapeRank worker reads. */
interface GraphSink {
    /** The outgoing edges each of [authors] has now. Authors with none are absent. */
    fun current(authors: List<String>): Map<String, Map<Edge, Set<String>>>

    /** Writes the changes: Neo4j first, then the Redis copies (a crash between leaves the author dirty). */
    fun apply(changes: List<EdgeChange>)
}

/** One author's edges of one type moving from what the graph holds to what their events say. */
data class EdgeChange(
    val author: String,
    val edge: Edge,
    val desired: Set<String>,
    val added: Set<String>,
    val removed: Set<String>,
)
