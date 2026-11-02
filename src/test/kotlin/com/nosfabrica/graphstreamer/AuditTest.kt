package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.relay.AbsenceGuard
import com.nosfabrica.graphstreamer.relay.Audit
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AuditTest {
    private val guard = AbsenceGuard(maxAbsent = 100)

    @Test
    fun absenceIsActedOnOnlyAfterACompletedReconcile() {
        assertTrue(guard.decide(absent = 5, completed = true, windows = 3).act)
        assertFalse(guard.decide(absent = 5, completed = false, windows = 3).act)
        assertFalse(guard.decide(absent = 5, completed = true, windows = 0).act)
    }

    @Test
    fun aMassAbsenceIsHeldUnlessTheGuardIsOff() {
        assertTrue(guard.decide(absent = 100, completed = true, windows = 1).act)
        assertFalse(guard.decide(absent = 101, completed = true, windows = 1).act)
        assertTrue(AbsenceGuard(maxAbsent = 0).decide(absent = 1_000_000, completed = true, windows = 1).act)
    }

    @Test
    fun reportsNewerThanTheAuditWindowAreNotCompared() {
        val recorded = listOf("a" to IdAndTime(100, "x"), "b" to IdAndTime(200, "y"))
        assertEquals(listOf(IdAndTime(100, "x")), Audit.localReportEntries(recorded, until = 150))
    }

    @Test
    fun reportBookkeepingRoundTrips() {
        assertEquals(mapOf("abc" to 100L, "def" to 200L), Neo4jRedisGraph.parseReports(listOf("100:abc", "200:def", "garbage", ":x")))
    }
}
