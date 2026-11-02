package com.nosfabrica.graphstreamer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * What the mirror holds, as relay filters. The same filters drive the live REQ and the
 * negentropy audit, and the local store answers them with the same semantics, so an audit
 * compares like with like. Changing them changes what an audit reconciles: a new filter's past
 * is downloaded whole on the next audit.
 */
object Mirrored {
    const val FOLLOWS = 3
    const val REPORTS = 1984
    const val MUTES = 10000
    const val DESIGNATIONS = 10040
    const val DELETION = 5
    const val VANISH = 62

    /** Replaceable lists: one per author, newest wins. */
    val LISTS = listOf(FOLLOWS, MUTES, DESIGNATIONS)

    /** The kinds the graph is projected from. */
    val GRAPH_KINDS = listOf(FOLLOWS, REPORTS, MUTES, DESIGNATIONS)

    /**
     * Unranked reads. The search relay refuses an unauthenticated read (REQ and NEG-OPEN alike)
     * that names no lens; `include:spam` waives it. The local store strips extension tokens, so
     * the same filter matches the same set on both sides.
     */
    const val UNRANKED = "include:spam"

    fun filters(
        since: Long? = null,
        until: Long? = null,
    ): List<Filter> =
        listOf(
            Filter(kinds = LISTS, since = since, until = until, search = UNRANKED),
            Filter(kinds = listOf(REPORTS), since = since, until = until, search = UNRANKED),
            // Only deletions that say they delete one of our kinds: they make a deletion take
            // effect the moment it streams in. One that names no `k` takes effect at the next
            // audit instead, when the relay no longer holds what it deleted.
            Filter(kinds = listOf(DELETION), tags = mapOf("k" to GRAPH_KINDS.map { it.toString() }), since = since, until = until, search = UNRANKED),
            Filter(kinds = listOf(VANISH), since = since, until = until, search = UNRANKED),
        )

    /** Whether [event] belongs in the mirror: exactly what [filters] ask for. */
    fun keeps(event: Event): Boolean = filters().any { it.match(event) }
}
