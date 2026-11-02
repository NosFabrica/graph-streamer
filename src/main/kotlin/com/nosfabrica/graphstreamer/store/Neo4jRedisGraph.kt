package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListHead
import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.TransactionContext
import redis.clients.jedis.JedisPool

/**
 * The graph as brainstorm_server has kept it: `(:NostrUser {pubkey})` nodes with outgoing
 * `FOLLOWS` / `MUTES` / `REPORTS` / `DESIGNATES` edges, plus the Redis copies the GrapeRank
 * worker reads: the reverse sets `followed_by:` / `muted_by:` / `reported_by:<target>` and the
 * forward set `designates:<author>`.
 *
 * And the bookkeeping the audit compares against the relay, on the author's node:
 * `follows_event`/`follows_at`, `mutes_event`/`mutes_at`, `designations_event`/`designations_at`
 * (the list the edges came from) and `report_events` (every report of theirs, `"<created_at>:<id>"`).
 * Edges and bookkeeping are written in one transaction, so they can't disagree; Redis after.
 *
 * Relationship types and property names come from [Edge] and [ListKind], never from data: a
 * fixed set of query texts, so Neo4j caches a fixed set of plans. Writes are managed
 * transactions, so the driver retries transient errors (the GrapeRank write-back locks the same nodes).
 */
class Neo4jRedisGraph(
    private val driver: Driver,
    private val redis: JedisPool,
) : GraphStore,
    AutoCloseable {
    constructor(url: String, username: String, password: String, redis: JedisPool) :
        this(GraphDatabase.driver(url, AuthTokens.basic(username, password)), redis)

    override fun close() = driver.close()

    override fun read(authors: List<String>): Map<String, AuthorState> {
        if (authors.isEmpty()) return emptyMap()
        val byType = Edge.entries.associateBy { it.relType }
        val heads = HashMap<String, Pair<Map<ListKind, ListHead>, Map<String, Long>>>()
        val edges = HashMap<String, HashMap<Edge, Set<String>>>()
        driver.session().use { session ->
            session.executeRead { tx ->
                tx.run(READ_BOOKKEEPING, mapOf("authors" to authors)).list().forEach { rec ->
                    val lists =
                        ListKind.entries
                            .mapNotNull { kind ->
                                val id = rec["${kind.prop}_event"]
                                val at = rec["${kind.prop}_at"]
                                if (id.isNull || at.isNull) null else kind to ListHead(id.asString(), at.asLong())
                            }.toMap()
                    val reports = rec["report_events"].let { if (it.isNull) emptyMap() else parseReports(it.asList { v -> v.asString() }) }
                    heads[rec["author"].asString()] = lists to reports
                }
                tx.run(READ_EDGES, mapOf("authors" to authors)).list().forEach { rec ->
                    val edge = byType.getValue(rec["rel"].asString())
                    edges.getOrPut(rec["author"].asString()) { HashMap() }[edge] = rec["targets"].asList { it.asString() }.toSet()
                }
            }
        }
        return (heads.keys + edges.keys).associateWith { a ->
            AuthorState(edges[a].orEmpty(), heads[a]?.first.orEmpty(), heads[a]?.second.orEmpty())
        }
    }

    override fun write(changes: List<AuthorChange>) {
        if (changes.isEmpty()) return
        driver.session().use { session ->
            session.executeWrite { tx ->
                for ((edge, rows) in changes.flatMap { it.edges }.groupBy { it.edge }) writeEdges(tx, edge, rows)
                val bookkeeping = changes.filter { it.bookkeepingChanged }.map(::bookkeepingRow)
                if (bookkeeping.isNotEmpty()) tx.run(WRITE_BOOKKEEPING, mapOf("rows" to bookkeeping))
            }
        }
        writeRedis(changes.flatMap { it.edges })
    }

    private fun writeEdges(
        tx: TransactionContext,
        edge: Edge,
        rows: List<EdgeChange>,
    ) {
        val removals = rows.filter { it.removed.isNotEmpty() }.map { mapOf("author" to it.author, "targets" to it.removed.toList()) }
        val additions = rows.filter { it.added.isNotEmpty() }.map { mapOf("author" to it.author, "targets" to it.added.toList()) }
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

    private fun bookkeepingRow(c: AuthorChange): Map<String, Any?> {
        val row = HashMap<String, Any?>()
        row["author"] = c.author
        for (kind in ListKind.entries) {
            val head = c.target.heads[kind]
            row["${kind.prop}_event"] = head?.id
            row["${kind.prop}_at"] = head?.createdAt
        }
        // Null removes the property, so an author with no reports carries none.
        row["report_events"] = c.target.reports.entries.map { (id, at) -> "$at:$id" }.sorted().ifEmpty { null }
        return row
    }

    private fun writeRedis(changes: List<EdgeChange>) {
        if (changes.isEmpty()) return
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

    override fun countLists(
        kind: ListKind,
        since: Long,
        until: Long,
    ): Int =
        driver.session().use { session ->
            session.executeRead { tx ->
                tx
                    .run(
                        "MATCH (u:NostrUser) WHERE u.${kind.prop}_at >= \$since AND u.${kind.prop}_at <= \$until RETURN count(u) AS n",
                        mapOf("since" to since, "until" to until),
                    ).single()["n"]
                    .asInt()
            }
        }

    override fun lists(
        kind: ListKind,
        since: Long,
        until: Long,
    ): List<IdAndTime> =
        driver.session().use { session ->
            session.executeRead { tx ->
                tx
                    .run(
                        "MATCH (u:NostrUser) WHERE u.${kind.prop}_at >= \$since AND u.${kind.prop}_at <= \$until " +
                            "RETURN u.${kind.prop}_event AS id, u.${kind.prop}_at AS at",
                        mapOf("since" to since, "until" to until),
                    ).list { IdAndTime(it["at"].asLong(), it["id"].asString()) }
            }
        }

    override fun authorsOfLists(
        kind: ListKind,
        ids: List<String>,
    ): List<String> =
        ids.chunked(ID_CHUNK).flatMap { chunk ->
            driver.session().use { session ->
                session.executeRead { tx ->
                    tx
                        .run("MATCH (u:NostrUser) WHERE u.${kind.prop}_event IN \$ids RETURN u.pubkey AS pubkey", mapOf("ids" to chunk))
                        .list { it["pubkey"].asString() }
                }
            }
        }

    override fun reports(): List<Pair<String, IdAndTime>> =
        driver.session().use { session ->
            session.executeRead { tx ->
                tx
                    .run("MATCH (u:NostrUser) WHERE u.report_events IS NOT NULL RETURN u.pubkey AS author, u.report_events AS events")
                    .list { rec ->
                        val author = rec["author"].asString()
                        parseReports(rec["events"].asList { it.asString() }).map { (id, at) -> author to IdAndTime(at, id) }
                    }.flatten()
            }
        }

    override fun ensureIndexes() {
        driver.session().use { session ->
            for (kind in ListKind.entries) {
                for (suffix in listOf("at", "event")) {
                    val prop = "${kind.prop}_$suffix"
                    session.run("CREATE RANGE INDEX graph_streamer_$prop IF NOT EXISTS FOR (u:NostrUser) ON (u.$prop)").consume()
                }
            }
        }
    }

    companion object {
        const val DESIGNATES_PREFIX = "designates:"
        private const val ID_CHUNK = 1_000

        val REVERSE_PREFIX =
            mapOf(
                Edge.FOLLOWS to "followed_by:",
                Edge.MUTES to "muted_by:",
                Edge.REPORTS to "reported_by:",
            )

        private val READ_BOOKKEEPING =
            """
            UNWIND ${'$'}authors AS a
            MATCH (u:NostrUser {pubkey: a})
            RETURN a AS author,
                   u.follows_event AS follows_event, u.follows_at AS follows_at,
                   u.mutes_event AS mutes_event, u.mutes_at AS mutes_at,
                   u.designations_event AS designations_event, u.designations_at AS designations_at,
                   u.report_events AS report_events
            """.trimIndent()

        private val READ_EDGES =
            """
            UNWIND ${'$'}authors AS a
            MATCH (:NostrUser {pubkey: a})-[r:FOLLOWS|MUTES|REPORTS|DESIGNATES]->(t:NostrUser)
            RETURN a AS author, type(r) AS rel, collect(t.pubkey) AS targets
            """.trimIndent()

        private val WRITE_BOOKKEEPING =
            """
            UNWIND ${'$'}rows AS row
            MERGE (u:NostrUser {pubkey: row.author})
            SET u.follows_event = row.follows_event, u.follows_at = row.follows_at,
                u.mutes_event = row.mutes_event, u.mutes_at = row.mutes_at,
                u.designations_event = row.designations_event, u.designations_at = row.designations_at,
                u.report_events = row.report_events
            """.trimIndent()

        /** `"<created_at>:<id>"` entries → id → created_at; anything malformed is skipped. */
        fun parseReports(entries: List<String>): Map<String, Long> =
            entries
                .mapNotNull { e ->
                    val colon = e.indexOf(':')
                    val at = if (colon > 0) e.substring(0, colon).toLongOrNull() else null
                    if (at == null) null else e.substring(colon + 1) to at
                }.toMap()
    }
}
