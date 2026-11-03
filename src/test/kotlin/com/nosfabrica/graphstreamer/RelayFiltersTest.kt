package com.nosfabrica.graphstreamer

import kotlin.test.Test
import kotlin.test.assertEquals

class RelayFiltersTest {
    @Test
    fun theLiveReqAsksOnlyForTheGraphKinds() {
        // Deletions and vanishes are left to the audit: most kind 5s carry no `k` tag to filter on.
        val filters = RelayFilters.live(since = 1_000)
        assertEquals(1, filters.size)
        assertEquals(RelayFilters.GRAPH_KINDS.sorted(), filters.single().kinds?.sorted())
        assertEquals(1_000L, filters.single().since)
        assertEquals(RelayFilters.UNRANKED, filters.single().search)
    }
}
