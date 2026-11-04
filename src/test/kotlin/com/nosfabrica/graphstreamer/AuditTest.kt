package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.relay.AbsenceGuard
import com.nosfabrica.graphstreamer.relay.Audit
import com.nosfabrica.graphstreamer.relay.BothSidesIndex
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.test.runTest
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
    fun reportsOlderThanAuditSinceAreNotCompared() {
        val recorded = listOf("a" to IdAndTime(100, "x"), "b" to IdAndTime(200, "y"), "c" to IdAndTime(300, "z"))
        assertEquals(listOf(IdAndTime(200, "y")), Audit.localReportEntries(recorded, until = 250, since = 150))
    }

    @Test
    fun reportBookkeepingRoundTrips() {
        assertEquals(mapOf("abc" to 100L, "def" to 200L), Neo4jRedisGraph.parseReports(listOf("100:abc", "200:def", "garbage", ":x")))
    }

    @Test
    fun aWindowIsAsLargeAsTheLargerSide() =
        runTest {
            val empty = NegentropyLocalIndex.Empty
            val w = Filter(kinds = listOf(3))
            assertEquals(15_000, BothSidesIndex(empty) { 15_000 }.count(w), "a first audit: the relay's count")
            assertEquals(0, BothSidesIndex(empty) { null }.count(w), "a relay that won't count: ours")
            val mine = NegentropyLocalIndex.of(List(10) { IdAndTime(it.toLong(), "id$it") })
            assertEquals(10, BothSidesIndex(mine) { 3 }.count(w))
        }
}
