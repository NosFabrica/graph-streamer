package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.store.Neo4jStreamerState
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import java.util.concurrent.CountDownLatch
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

    @Test
    fun concurrentFirstWritesMakeOneNode() {
        if (url == null) return
        GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: "")).use { driver ->
            fun reset() =
                driver.session().use {
                    it.run("MATCH (s:${Neo4jStreamerState.LABEL}) DETACH DELETE s").consume()
                    it.run("DROP CONSTRAINT graph_streamer_state_name IF EXISTS").consume()
                }
            repeat(20) { round ->
                reset()
                // A fresh process: the live stream, the audit and the Redis loop writing at once.
                val states = List(8) { Neo4jStreamerState(driver) }
                val start = CountDownLatch(1)
                val threads =
                    states.mapIndexed { i, state ->
                        Thread {
                            start.await()
                            if (i % 2 == 0) state.putLiveSince(1_700_000_000L + i) else state.putAuditReport(mapOf("finished_at" to "${1_700_000_000L + i}"))
                        }.also { it.start() }
                    }
                start.countDown()
                threads.forEach { it.join() }
                val nodes = driver.session().use { it.run("MATCH (s:${Neo4jStreamerState.LABEL}) RETURN count(s) AS n").single()["n"].asLong() }
                assertEquals(1L, nodes, "round $round: one state node however many first writes race")
            }
            reset()
        }
    }

    @Test
    fun duplicatesAnEarlierVersionLeftAreFoldedIntoOne() {
        if (url == null) return
        GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: "")).use { driver ->
            driver.session().use {
                it.run("MATCH (s:${Neo4jStreamerState.LABEL}) DETACH DELETE s").consume()
                it.run("DROP CONSTRAINT graph_streamer_state_name IF EXISTS").consume()
                it.run("CREATE (:${Neo4jStreamerState.LABEL} {name: \$n, live_since: '1'}), (:${Neo4jStreamerState.LABEL} {name: \$n, live_since: '2', audit_finished_at: '3'})", mapOf("n" to Neo4jStreamerState.NAME)).consume()
            }
            val state = Neo4jStreamerState(driver)
            state.putLiveSince(5)
            driver.session().use { s ->
                assertEquals(1L, s.run("MATCH (n:${Neo4jStreamerState.LABEL}) RETURN count(n) AS n").single()["n"].asLong())
            }
            assertEquals(3L, state.lastAuditFinishedAt(), "the fuller node is the one kept")
            assertEquals(5L, state.liveSince())
            driver.session().use { it.run("MATCH (s:${Neo4jStreamerState.LABEL}) DETACH DELETE s").consume() }
        }
    }
}
