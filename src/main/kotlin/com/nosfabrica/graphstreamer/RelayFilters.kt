package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/** What is read from the search relay, and how. */
object RelayFilters {
    const val REPORTS = 1984

    /** The kinds the graph is built from. */
    val GRAPH_KINDS = ListKind.entries.map { it.kind } + REPORTS

    /**
     * Unranked reads. The search relay refuses an unauthenticated read (REQ and NEG-OPEN alike)
     * that names no web-of-trust lens; `include:spam` waives it.
     */
    const val UNRANKED = "include:spam"

    /**
     * The live REQ, from [since]; once caught up, `subscribeEvents` moves it forward. Lists and
     * reports are applied as they arrive. Deletions (kind 5) and vanishes (kind 62) aren't asked
     * for: most clients leave the `k` tag off a kind 5, so a filter for ours would miss most of
     * them, and every kind 5 would be far more to download. The relay applies them, and the next
     * audit finds what it no longer holds.
     */
    fun live(since: Long): List<Filter> = listOf(Filter(kinds = GRAPH_KINDS, since = since, search = UNRANKED))

    /** One audit filter per kind, so each is compared against its own bookkeeping in the graph. */
    fun audit(
        kind: Int,
        until: Long,
        since: Long? = null,
    ): Filter = Filter(kinds = listOf(kind), since = since, until = until, search = UNRANKED)

    /** Everything the relay holds now for [authors]: what a recompute rebuilds them from. */
    fun byAuthors(authors: List<String>): List<Filter> = listOf(Filter(authors = authors, kinds = GRAPH_KINDS, search = UNRANKED))
}
