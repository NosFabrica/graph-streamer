package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/** What is read from the search relay, and how. */
object RelayFilters {
    const val REPORTS = 1984
    const val DELETION = 5
    const val VANISH = 62

    /** The kinds the graph is built from. */
    val GRAPH_KINDS = ListKind.entries.map { it.kind } + REPORTS

    /**
     * Unranked reads. The search relay refuses an unauthenticated read (REQ and NEG-OPEN alike)
     * that names no web-of-trust lens; `include:spam` waives it.
     */
    const val UNRANKED = "include:spam"

    /**
     * The live REQ, from [since]; once caught up, `subscribeEvents` moves it forward. Lists and
     * reports are applied as they arrive. Kind 5 and kind 62 are only triggers: the relay has already applied them, so
     * their author is recomputed from it. Only
     * kind-5s whose `k` names one of our kinds are asked for; any other deletion of ours shows up
     * at the next audit, when the relay no longer holds what it deleted.
     */
    fun live(since: Long): List<Filter> =
        listOf(
            Filter(kinds = GRAPH_KINDS, since = since, search = UNRANKED),
            Filter(kinds = listOf(DELETION), tags = mapOf("k" to GRAPH_KINDS.map { it.toString() }), since = since, search = UNRANKED),
            Filter(kinds = listOf(VANISH), since = since, search = UNRANKED),
        )

    /** One audit filter per kind, so each is compared against its own bookkeeping in the graph. */
    fun audit(
        kind: Int,
        until: Long,
    ): Filter = Filter(kinds = listOf(kind), until = until, search = UNRANKED)

    /** Everything the relay holds now for [authors]: what a recompute rebuilds them from. */
    fun byAuthors(authors: List<String>): List<Filter> = listOf(Filter(authors = authors, kinds = GRAPH_KINDS, search = UNRANKED))
}
