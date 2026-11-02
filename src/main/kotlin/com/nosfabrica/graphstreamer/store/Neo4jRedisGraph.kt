package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.Edge
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import redis.clients.jedis.JedisPool

/**
 * The graph as brainstorm_server has kept it: `(:NostrUser {pubkey})` nodes with outgoing
 * `FOLLOWS` / `MUTES` / `REPORTS` / `DESIGNATES` edges, plus the Redis copies the GrapeRank
 * worker reads: the reverse sets `followed_by:` / `muted_by:` / `reported_by:<target>` and the
 * forward set `designates:<author>`. Neo4j is written first, then Redis, as the server does.
 *
 * Relationship types are interpolated from [Edge], never from data: four fixed query texts, so
 * Neo4j caches four plans. Writes are managed transactions, so the driver retries transient
 * errors (the GrapeRank write-back locks the same nodes).
 */
class Neo4jRedisGraph(
    private val driver: Driver,
    private val redis: JedisPool,
) : GraphSink,
    AutoCloseable {
    constructor(url: String, username: String, password: String, redis: JedisPool) :
        this(GraphDatabase.driver(url, AuthTokens.basic(username, password)), redis)

    override fun close() = driver.close()

    override fun current(authors: List<String>): Map<String, Map<Edge, Set<String>>> {
        if (authors.isEmpty()) return emptyMap()
        val byType = Edge.entries.associateBy { it.relType }
        val out = HashMap<String, HashMap<Edge, Set<String>>>()
        driver.session().use { session ->
            session.executeRead { tx ->
                tx
                    .run(
                        """
                        UNWIND ${'$'}authors AS a
                        MATCH (:NostrUser {pubkey: a})-[r:FOLLOWS|MUTES|REPORTS|DESIGNATES]->(t:NostrUser)
                        RETURN a AS author, type(r) AS rel, collect(t.pubkey) AS targets
                        """,
                        mapOf("authors" to authors),
                    ).list()
                    .forEach { rec ->
                        val edge = byType.getValue(rec["rel"].asString())
                        out.getOrPut(rec["author"].asString()) { HashMap() }[edge] = rec["targets"].asList { it.asString() }.toSet()
                    }
            }
        }
        return out
    }

    override fun apply(changes: List<EdgeChange>) {
        if (changes.isEmpty()) return
        val byEdge = changes.groupBy { it.edge }
        driver.session().use { session ->
            for ((edge, rows) in byEdge) {
                val removals = rows.filter { it.removed.isNotEmpty() }.map { mapOf("author" to it.author, "targets" to it.removed.toList()) }
                val additions = rows.filter { it.added.isNotEmpty() }.map { mapOf("author" to it.author, "targets" to it.added.toList()) }
                session.executeWrite { tx ->
                    if (removals.isNotEmpty()) {
                        tx.run(
                            """
                            UNWIND ${'$'}rows AS row
                            MATCH (:NostrUser {pubkey: row.author})-[r:${edge.relType}]->(t:NostrUser)
                            WHERE t.pubkey IN row.targets
                            DELETE r
                            """,
                            mapOf("rows" to removals),
                        )
                    }
                    if (additions.isNotEmpty()) {
                        tx.run(
                            """
                            UNWIND ${'$'}rows AS row
                            MERGE (a:NostrUser {pubkey: row.author})
                            WITH a, row
                            UNWIND row.targets AS tp
                                MERGE (t:NostrUser {pubkey: tp})
                                MERGE (a)-[:${edge.relType}]->(t)
                            """,
                            mapOf("rows" to additions),
                        )
                    }
                }
            }
        }
        writeRedis(changes)
    }

    private fun writeRedis(changes: List<EdgeChange>) {
        redis.resource.use { r ->
            val pipe = r.pipelined()
            for (c in changes) {
                val prefix = REVERSE_PREFIX[c.edge] ?: continue
                for (target in c.added) pipe.sadd(prefix + target, c.author)
                for (target in c.removed) pipe.srem(prefix + target, c.author)
            }
            pipe.sync()
            // designates:<author> is replaced whole, in one MULTI, so a reader never sees half a list.
            for (c in changes) {
                if (c.edge != Edge.DESIGNATES) continue
                val key = DESIGNATES_PREFIX + c.author
                r.multi().apply {
                    del(key)
                    if (c.desired.isNotEmpty()) sadd(key, *c.desired.toTypedArray())
                    exec()
                }
            }
        }
    }

    companion object {
        const val DESIGNATES_PREFIX = "designates:"

        val REVERSE_PREFIX =
            mapOf(
                Edge.FOLLOWS to "followed_by:",
                Edge.MUTES to "muted_by:",
                Edge.REPORTS to "reported_by:",
            )
    }
}
