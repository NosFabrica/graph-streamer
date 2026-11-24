package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.store.AuthorChange
import com.nosfabrica.graphstreamer.store.EdgeChange
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.nosfabrica.graphstreamer.store.UserIds
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import redis.clients.jedis.JedisPool
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The schema and edge writes on a real Neo4j; skipped unless NEO4J_TEST_URL is set (with
 * NEO4J_TEST_USERNAME and NEO4J_TEST_PASSWORD), and the edge test also unless REDIS_TEST_PORT is
 * (Redis on localhost). Point them at scratch databases: the schema test drops the constraint
 * first, to prove it is the streamer that creates it.
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

    @Test
    fun writingAnEdgeTheGraphHoldsDoesNotDoubleIt() {
        val redisPort = System.getenv("REDIS_TEST_PORT")?.toInt()
        if (url == null || redisPort == null) return
        val driver = GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: ""))
        val author = "test-author-edges"
        val targets = (1..3).map { "test-target-edges-$it" }
        // Ids no real user has, so the sets below are this test's alone.
        val ids = (listOf(author) + targets).withIndex().associate { (i, pubkey) -> pubkey to 2_000_000_000 + i }
        val pool = JedisPool("127.0.0.1", redisPort)
        Neo4jRedisGraph(driver, pool, UserIds { wanted -> wanted.associateWith(ids::getValue) }).use { graph ->
            fun clear() {
                driver.session().use { it.run("MATCH (u:NostrUser) WHERE u.pubkey STARTS WITH 'test-' DETACH DELETE u").consume() }
                pool.resource.use { r -> ids.values.forEach { r.del("followed_by:$it", "designates:$it") } }
            }
            clear()
            try {
                graph.ensureIndexes()
                writeTwice(graph, driver, author, targets)
                theSetsNameUsersById(graph, pool, author, targets, ids)
            } finally {
                // A failed assertion must not leave a doubled edge in a shared scratch database.
                clear()
            }
        }
    }

    /** The server's naming with SCORES_IN_POSTGRES: the same prefixes, ids in key and member. */
    private fun theSetsNameUsersById(
        graph: Neo4jRedisGraph,
        pool: JedisPool,
        author: String,
        targets: List<String>,
        ids: Map<String, Int>,
    ) {
        val me = ids.getValue(author).toString()
        val named = targets.map { ids.getValue(it).toString() }.toSet()
        graph.write(listOf(AuthorChange(author, listOf(EdgeChange(author, Edge.DESIGNATES, targets.toSet(), targets.toSet(), emptySet())), AuthorState(), false)))
        pool.resource.use { r ->
            for (target in named) assertEquals(setOf(me), r.smembers("followed_by:$target"))
            assertEquals(named, r.smembers("designates:$me"))
            assertEquals(emptySet(), r.keys("followed_by:test-*") + r.keys("designates:test-*"))
        }
        // One follow leaves: its set loses the author, the others keep them.
        val kept = targets.drop(1).toSet()
        graph.write(listOf(AuthorChange(author, listOf(EdgeChange(author, Edge.FOLLOWS, kept, emptySet(), setOf(targets[0]))), AuthorState(), false)))
        pool.resource.use { r ->
            assertEquals(emptySet(), r.smembers("followed_by:${ids.getValue(targets[0])}"))
            assertEquals(setOf(me), r.smembers("followed_by:${ids.getValue(targets[1])}"))
        }
    }

    private fun writeTwice(
        graph: Neo4jRedisGraph,
        driver: Driver,
        author: String,
        targets: List<String>,
    ) {
        fun add(added: Set<String>) =
            graph.write(listOf(AuthorChange(author, listOf(EdgeChange(author, Edge.FOLLOWS, targets.toSet(), added, emptySet())), AuthorState(), false)))

        add(setOf(targets[0]))
        // As a second writer would: the first target again, beside two new ones.
        add(targets.toSet())

        driver.session().use { session ->
            val counts =
                session
                    .run("MATCH (:NostrUser {pubkey: \$a})-[r:FOLLOWS]->(t:NostrUser) RETURN t.pubkey AS t, count(r) AS n", mapOf("a" to author))
                    .list()
                    .associate { it["t"].asString() to it["n"].asLong() }
            assertEquals(targets.associateWith { 1L }, counts)
        }
    }
}
