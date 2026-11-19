package com.nosfabrica.graphstreamer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class ConfigTest {
    private val base = mapOf("NEO4J_URL" to "bolt://x", "NEO4J_USERNAME" to "u", "NEO4J_PASSWORD" to "p", "REDIS_HOST" to "r", "REDIS_PORT" to "6379")
    private val scorecard =
        mapOf(
            "SCORECARD_DB_HOST" to "scorecard-postgres",
            "SCORECARD_DB_PORT" to "5432",
            "SCORECARD_DB_NAME" to "scorecard",
            "SCORECARD_DB_USER" to "postgres",
            "SCORECARD_DB_PASSWORD" to "pw",
        )

    @Test
    fun aDryRunReadsNoScorecardDatabase() {
        assertNull(Config.fromEnv(base).scorecardDb)
        assertNull(Config.fromEnv(base + scorecard + ("DRY_RUN" to "true")).scorecardDb)
    }

    @Test
    fun writingReadsTheScorecardConnection() {
        val db = Config.fromEnv(base + scorecard + ("DRY_RUN" to "false")).scorecardDb!!
        assertEquals("jdbc:postgresql://scorecard-postgres:5432/scorecard", db.jdbcUrl)
        assertEquals("postgres" to "pw", db.user to db.password)
    }

    @Test
    fun writingWithoutAScorecardConnectionRefusesToStart() {
        for (missing in scorecard.keys) {
            val error = assertFailsWith<IllegalStateException> { Config.fromEnv(base + (scorecard - missing) + ("DRY_RUN" to "false")) }
            assertEquals("$missing is not set", error.message)
        }
    }
}
