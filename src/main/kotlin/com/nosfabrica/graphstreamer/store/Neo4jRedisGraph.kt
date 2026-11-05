package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListHead
import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import org.neo4j.driver.Driver
import org.neo4j.driver.TransactionContext
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams

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
 * Redis can still fall behind Neo4j: a crash between the commit and the Redis write, an eviction
 * (production runs allkeys-lru), a Redis that came up empty, or drift inherited from the server's
 * consumer. Two repairs, both from Neo4j, which is what Redis copies:
 *
 * - **The outbox.** The transaction that writes an author's edges also labels them
 *   `GraphStreamerRedisPending` and records the targets they lost (`redis_pending_removed`). The
 *   label goes once Redis has the change. [repairPendingRedis] replays whoever still has it: their
 *   current targets added, the recorded ones removed. Exact, and only for the authors a crash caught.
 * - **The reconcile.** [reconcileRedis] compares every set's size with its key's edge count in
 *   Neo4j, replaces the sets that differ, and deletes the sets whose key has no edge. A batch at a
 *   time, under the caller's lock.
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
        val pending = changes.filter { it.edges.isNotEmpty() }.map { c -> mapOf("author" to c.author, "removed" to c.edges.flatMap { e -> e.removed.map { "${e.edge.relType}|$it" } }) }
        driver.session().use { session ->
            session.executeWrite { tx ->
                for ((edge, rows) in changes.flatMap { it.edges }.groupBy { it.edge }) writeEdges(tx, edge, rows)
                val bookkeeping = changes.filter { it.bookkeepingChanged }.map(::bookkeepingRow)
                if (bookkeeping.isNotEmpty()) tx.run(WRITE_BOOKKEEPING, mapOf("rows" to bookkeeping))
                if (pending.isNotEmpty()) tx.run(MARK_PENDING, mapOf("rows" to pending))
            }
        }
        writeRedis(changes.flatMap { it.edges })
        if (pending.isNotEmpty()) clearPending(pending.map { it["author"] as String })
    }

    private fun clearPending(authors: List<String>) {
        driver.session().use { session -> session.executeWriteWithoutResult { tx -> tx.run(CLEAR_PENDING, mapOf("authors" to authors)).consume() } }
    }

    override fun orphanAuthors(
        edge: Edge,
        limit: Int,
    ): List<String> {
        val unrecorded = ListKind.entries.firstOrNull { it.edge == edge }?.let { "u.${it.prop}_event IS NULL" } ?: "u.report_events IS NULL"
        return driver.session().use { s ->
            s.executeRead { tx ->
                tx
                    .run("MATCH (u:NostrUser) WHERE $unrecorded AND EXISTS { (u)-[:${edge.relType}]->() } RETURN u.pubkey AS a LIMIT \$limit", mapOf("limit" to limit))
                    .list { it["a"].asString() }
            }
        }
    }

    override fun repairPendingRedis(): Int {
        var repaired = 0
        while (true) {
            val batch =
                driver.session().use { s ->
                    s.executeRead { tx ->
                        tx.run("MATCH (u:$PENDING_LABEL) RETURN u.pubkey AS a, u.redis_pending_removed AS removed LIMIT 500").list { rec ->
                            rec["a"].asString() to (if (rec["removed"].isNull) emptyList() else rec["removed"].asList { it.asString() })
                        }
                    }
                }
            if (batch.isEmpty()) return repaired
            val current = read(batch.map { it.first })
            redis.resource.use { r ->
                val pipe = r.pipelined()
                for ((author, removed) in batch) {
                    val state = current[author]
                    for ((edge, prefix) in REVERSE_PREFIX) for (t in state?.edges(edge).orEmpty()) pipe.sadd(prefix + t, author)
                    for (entry in removed) {
                        val (rel, target) = entry.split('|', limit = 2).takeIf { it.size == 2 } ?: continue
                        val edge = Edge.entries.firstOrNull { it.relType == rel } ?: continue
                        val prefix = REVERSE_PREFIX[edge] ?: continue
                        if (state?.edges(edge)?.contains(target) != true) pipe.srem(prefix + target, author)
                    }
                    pipe.del(DESIGNATES_PREFIX + author)
                    val designated = state?.edges(Edge.DESIGNATES).orEmpty()
                    if (designated.isNotEmpty()) pipe.sadd(DESIGNATES_PREFIX + author, *designated.toTypedArray())
                }
                pipe.sync()
            }
            clearPending(batch.map { it.first })
            repaired += batch.size
        }
    }

    override fun reconcileRedis(underLock: (() -> Unit) -> Unit): RedisRepair {
        var keys = 0L
        var replaced = 0L
        var deleted = 0L
        for (edge in Edge.entries) {
            val reverse = edge != Edge.DESIGNATES
            val prefix = REVERSE_PREFIX[edge] ?: DESIGNATES_PREFIX
            // Pass 1: every key with an edge, its count against its set's size.
            val degreeQuery =
                if (reverse) {
                    "MATCH (u:NostrUser) WITH u, COUNT { (u)<-[:${edge.relType}]-() } AS n WHERE n > 0 RETURN u.pubkey AS k, n"
                } else {
                    "MATCH (u:NostrUser) WITH u, COUNT { (u)-[:${edge.relType}]->() } AS n WHERE n > 0 RETURN u.pubkey AS k, n"
                }
            driver.session().use { s ->
                val result = s.run(degreeQuery)
                val batch = ArrayList<Pair<String, Long>>(RECONCILE_BATCH)
                fun flush() {
                    if (batch.isEmpty()) return
                    underLock { replaced += replaceIfSizeDiffers(edge, prefix, reverse, batch) }
                    keys += batch.size
                    batch.clear()
                }
                while (result.hasNext()) {
                    val rec = result.next()
                    batch += rec["k"].asString() to rec["n"].asLong()
                    if (batch.size >= RECONCILE_BATCH) flush()
                }
                flush()
            }
            // Pass 2: sets whose key has no edge left.
            redis.resource.use { r ->
                var cursor = ScanParams.SCAN_POINTER_START
                val params = ScanParams().match("$prefix*").count(RECONCILE_BATCH)
                do {
                    val page = r.scan(cursor, params)
                    cursor = page.cursor
                    val found = page.result.map { it.removePrefix(prefix) }
                    if (found.isNotEmpty()) underLock { deleted += deleteKeysWithoutEdges(edge, prefix, reverse, found) }
                } while (cursor != ScanParams.SCAN_POINTER_START)
            }
        }
        return RedisRepair(keys, replaced, deleted)
    }

    /** Re-reads both sides (the caller holds the write lock) and replaces each set whose size differs. */
    private fun replaceIfSizeDiffers(
        edge: Edge,
        prefix: String,
        reverse: Boolean,
        batch: List<Pair<String, Long>>,
    ): Long {
        val sizes =
            redis.resource.use { r ->
                val pipe = r.pipelined()
                val responses = batch.map { pipe.scard(prefix + it.first) }
                pipe.sync()
                responses.map { it.get() }
            }
        val keys = batch.map { it.first }
        val degrees = degreesOf(edge, reverse, keys)
        val differ = keys.filterIndexed { i, k -> sizes[i] != (degrees[k] ?: 0L) }
        if (differ.isEmpty()) return 0
        val members = membersOf(edge, reverse, differ)
        redis.resource.use { r ->
            for (k in differ) {
                r.multi().apply {
                    del(prefix + k)
                    val m = members[k].orEmpty()
                    if (m.isNotEmpty()) sadd(prefix + k, *m.toTypedArray())
                    exec()
                }
            }
        }
        return differ.size.toLong()
    }

    private fun deleteKeysWithoutEdges(
        edge: Edge,
        prefix: String,
        reverse: Boolean,
        keys: List<String>,
    ): Long {
        val withEdges = degreesOf(edge, reverse, keys).filterValues { it > 0 }.keys
        val orphans = keys.filter { it !in withEdges }
        if (orphans.isNotEmpty()) redis.resource.use { r -> r.del(*orphans.map { prefix + it }.toTypedArray()) }
        return orphans.size.toLong()
    }

    /** How many members [keys] should have; a key with no node is absent. */
    private fun degreesOf(
        edge: Edge,
        reverse: Boolean,
        keys: List<String>,
    ): Map<String, Long> {
        val count = if (reverse) "COUNT { (k)<-[:${edge.relType}]-() }" else "COUNT { (k)-[:${edge.relType}]->() }"
        return driver.session().use { s ->
            s.executeRead { tx ->
                tx.run("UNWIND \$keys AS key MATCH (k:NostrUser {pubkey: key}) RETURN key, $count AS n", mapOf("keys" to keys)).list { rec ->
                    rec["key"].asString() to rec["n"].asLong()
                }.toMap()
            }
        }
    }

    /** The Redis members [keys] should have: their followers (reverse sets) or their designees. */
    private fun membersOf(
        edge: Edge,
        reverse: Boolean,
        keys: List<String>,
    ): Map<String, List<String>> {
        val pattern = if (reverse) "(k:NostrUser {pubkey: key})<-[:${edge.relType}]-(m:NostrUser)" else "(k:NostrUser {pubkey: key})-[:${edge.relType}]->(m:NostrUser)"
        return driver.session().use { s ->
            s.executeRead { tx ->
                tx.run("UNWIND \$keys AS key MATCH $pattern RETURN key, collect(m.pubkey) AS members", mapOf("keys" to keys)).list { rec ->
                    rec["key"].asString() to rec["members"].asList { it.asString() }
                }.toMap()
            }
        }
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
            // Not MERGE on the edge: it checks each edge against the author's, which grow with every
            // insert, so a list costs the square of its length (measured: 4,000 follows 1.2 s,
            // 8,000 2.7 s; a batch of 500 large lists over five minutes). The author's targets are
            // read once instead, and only the missing ones created: 0.1 s and 0.25 s. The added set
            // already excludes what the graph held; the check keeps a second writer from doubling an edge.
            tx.run(
                """
                UNWIND ${'$'}rows AS row
                MERGE (a:NostrUser {pubkey: row.author})
                WITH a, row
                OPTIONAL MATCH (a)-[:${edge.relType}]->(held:NostrUser)
                WITH a, row, collect(held.pubkey) AS heldTargets
                UNWIND [tp IN row.targets WHERE NOT tp IN heldTargets] AS tp
                    MERGE (t:NostrUser {pubkey: tp})
                    CREATE (a)-[:${edge.relType}]->(t)
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
            // Every write MERGEs on the pubkey, so without this each one scans every node. The
            // server creates the same constraint, under the same name, so whichever starts first wins.
            session.run(PUBKEY_CONSTRAINT).consume()
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

        /** brainstorm_server's `create_pubkey_index`, verbatim in name and definition. */
        const val PUBKEY_CONSTRAINT = "CREATE CONSTRAINT nostr_user_pubkey IF NOT EXISTS FOR (u:NostrUser) REQUIRE u.pubkey IS UNIQUE"

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

        /** The outbox: authors whose Redis copy their last write may not have reached. */
        const val PENDING_LABEL = "GraphStreamerRedisPending"
        private const val RECONCILE_BATCH = 5_000

        private val MARK_PENDING =
            """
            UNWIND ${'$'}rows AS row
            MATCH (u:NostrUser {pubkey: row.author})
            SET u:$PENDING_LABEL, u.redis_pending_removed = row.removed
            """.trimIndent()

        private val CLEAR_PENDING =
            """
            UNWIND ${'$'}authors AS a
            MATCH (u:NostrUser {pubkey: a})
            REMOVE u:$PENDING_LABEL, u.redis_pending_removed
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
