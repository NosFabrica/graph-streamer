package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.store.Neo4jStreamerState
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The state node against a real Neo4j; skipped unless NEO4J_TEST_URL is set (with
 * NEO4J_TEST_USERNAME and NEO4J_TEST_PASSWORD). It deletes the state node it writes, so point it
 * at a scratch database.
 */
class Neo4jStreamerStateTest {
    private val url = System.getenv("NEO4J_TEST_URL")

    @Test
    fun keepsTheResumePointAndTheAuditReportOnOneNode() {
        if (url == null) return
        GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: "")).use { driver ->
            fun clear() = driver.session().use { it.run("MATCH (s:${Neo4jStreamerState.LABEL}) DETACH DELETE s").consume() }
            clear()
            val state = Neo4jStreamerState(driver)

            assertNull(state.liveSince())
            assertNull(state.lastAuditFinishedAt())

            state.putLiveSince(1_700_000_000)
            state.putLiveSince(1_700_000_060)
            assertEquals(1_700_000_060L, state.liveSince())

            state.putAuditReport(mapOf("started_at" to "1700000100", "finished_at" to "1700000200", "follows.need" to "12", "follows.note" to ""))
            assertEquals(1_700_000_200L, state.lastAuditFinishedAt())
            assertEquals(1_700_000_060L, state.liveSince(), "a report leaves the resume point alone")

            driver.session().use { session ->
                val rows = session.run("MATCH (s:${Neo4jStreamerState.LABEL}) RETURN s.name AS name, s.audit_follows_need AS need, s.audit_follows_note AS note").list()
                assertEquals(1, rows.size, "one node, however many writes")
                assertEquals(Neo4jStreamerState.NAME, rows.single()["name"].asString())
                assertEquals("12", rows.single()["need"].asString())
                assertEquals("", rows.single()["note"].asString())
            }
            clear()
        }
    }
}
