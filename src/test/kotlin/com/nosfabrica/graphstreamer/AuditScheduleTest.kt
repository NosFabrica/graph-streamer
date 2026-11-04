package com.nosfabrica.graphstreamer

import kotlin.test.Test
import kotlin.test.assertEquals

class AuditScheduleTest {
    private val day = 86_400L
    private val retry = 1_800L

    @Test
    fun theFirstAuditRunsAtOnce() {
        assertEquals(0, AuditSchedule.waitSeconds(now = 1_000, finishedAt = null, endedAt = null, everySeconds = day, retrySeconds = retry))
    }

    @Test
    fun aCompleteAuditWaitsTheFullInterval() {
        assertEquals(day - 100, AuditSchedule.waitSeconds(now = 1_100, finishedAt = 1_000, endedAt = 1_000, everySeconds = day, retrySeconds = retry))
    }

    @Test
    fun anIncompleteAuditRetriesSoon() {
        // The first rebuild never completed: no finished_at, only an end.
        assertEquals(retry - 100, AuditSchedule.waitSeconds(now = 1_100, finishedAt = null, endedAt = 1_000, everySeconds = day, retrySeconds = retry))
        // An older complete audit doesn't hide a newer incomplete one, across a restart too.
        assertEquals(retry - 100, AuditSchedule.waitSeconds(now = 5_100, finishedAt = 1_000, endedAt = 5_000, everySeconds = day, retrySeconds = retry))
        assertEquals(0, AuditSchedule.waitSeconds(now = 9_000, finishedAt = null, endedAt = 1_000, everySeconds = day, retrySeconds = retry))
    }
}
