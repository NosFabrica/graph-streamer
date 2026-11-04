package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import redis.clients.jedis.JedisPool
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The schema graph-streamer creates on a real Neo4j; skipped unless NEO4J_TEST_URL is set (with
 * NEO4J_TEST_USERNAME and NEO4J_TEST_PASSWORD). Point it at a scratch database: it drops the
 * constraint first, to prove it is the streamer that creates it.
 */
class Neo4jSchemaTest {
    private val url = System.getenv("NEO4J_TEST_URL")

    @Test
    fun ensureIndexesCreatesTheServersPubkeyConstraint() {
        if (url == null) return
        val driver = GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: ""))
        Neo4jRedisGraph(driver, JedisPool("127.0.0.1", 1)).use { graph ->
            driver.session().use { it.run("DROP CONSTRAINT nostr_user_pubkey IF EXISTS").consume() }
            graph.ensureIndexes()
            graph.ensureIndexes() // idempotent, like the server's
            driver.session().use { session ->
                val row =
                    session
                        .run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties WHERE name = 'nostr_user_pubkey' RETURN type, labelsOrTypes, properties")
                        .list()
                        .single()
                assertEquals("UNIQUENESS", row["type"].asString())
                assertEquals(listOf("NostrUser"), row["labelsOrTypes"].asList { it.asString() })
                assertEquals(listOf("pubkey"), row["properties"].asList { it.asString() })
                val indexes = session.run("SHOW INDEXES YIELD name RETURN collect(name) AS names").single()["names"].asList { it.asString() }
                assertTrue(indexes.containsAll(listOf("graph_streamer_follows_at", "graph_streamer_follows_event")), "$indexes")
            }
        }
    }
}
