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

    @Test
    fun anAuditSinceBoundsTheRelaysSide() {
        assertEquals(null, RelayFilters.audit(3, until = 2_000).since)
        val bounded = RelayFilters.audit(3, until = 2_000, since = 1_000)
        assertEquals(1_000L, bounded.since)
        assertEquals(2_000L, bounded.until)
        assertEquals(RelayFilters.UNRANKED, bounded.search)
    }

    @Test
    fun auditSinceIsReadFromTheEnvironment() {
        val base = mapOf("NEO4J_URL" to "bolt://x", "NEO4J_USERNAME" to "u", "NEO4J_PASSWORD" to "p", "REDIS_HOST" to "r", "REDIS_PORT" to "6379")
        assertEquals(null, Config.fromEnv(base).auditSince)
        assertEquals(1_767_225_600L, Config.fromEnv(base + ("AUDIT_SINCE" to "1767225600")).auditSince)
    }
}
