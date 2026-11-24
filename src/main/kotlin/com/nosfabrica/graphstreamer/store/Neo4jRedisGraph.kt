package com.nosfabrica.graphstreamer.store

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListHead
import com.nosfabrica.graphstreamer.projection.ListKind
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import org.neo4j.driver.Driver
import org.neo4j.driver.TransactionContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams

/**
 * The graph as brainstorm_server has kept it: `(:NostrUser {pubkey})` nodes with outgoing
 * `FOLLOWS` / `MUTES` / `REPORTS` / `DESIGNATES` edges, plus the Redis copies the GrapeRank
 * worker reads: the reverse sets `followed_by:` / `muted_by:` / `reported_by:<target>` and the
 * forward set `designates:<author>`. Redis names every user by User id, in the key and in the
 * members, as brainstorm_server does (`relationship_sets.py`); Neo4j and the outbox keep pubkeys.
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
 *
 * Every node a write may create gets its `user_id` by the id rule: the ids of everyone the batch
 * names are committed in the Scorecard database ([userIds]) before the Neo4j transaction opens,
 * and set on each node that has none. An id is never overwritten. Without [userIds] (a dry run)
 * nothing can be written.
 */
class Neo4jRedisGraph(
    private val driver: Driver,
    private val redis: JedisPool,
    private val userIds: UserIds? = null,
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
        // Before the transaction: an id must be durable before a node carries it.
        val ids = ids(named(changes))
        val mismatched = ArrayList<String>()
        driver.session().use { session ->
            session.executeWrite { tx ->
                mismatched.clear()
                for ((edge, rows) in changes.flatMap { it.edges }.groupBy { it.edge }) mismatched += writeEdges(tx, edge, rows, ids)
                val bookkeeping = changes.filter { it.bookkeepingChanged }.map { c -> bookkeepingRow(c) + ("user_id" to ids.getValue(c.author)) }
                if (bookkeeping.isNotEmpty()) {
                    mismatched += tx.run(WRITE_BOOKKEEPING, mapOf("rows" to bookkeeping)).single()["mismatched"].asList { it.asString() }
                }
                if (pending.isNotEmpty()) tx.run(MARK_PENDING, mapOf("rows" to pending))
            }
        }
        if (mismatched.isNotEmpty()) {
            log.error("{} nodes hold a user_id other than nostr_user's; left as they are: {}", mismatched.toSet().size, mismatched.toSet().take(5))
        }
        writeRedis(changes.flatMap { it.edges }, ids)
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
            val lost =
                batch.associate { (author, removed) ->
                    author to
                        removed.mapNotNull { entry ->
                            val (rel, target) = entry.split('|', limit = 2).takeIf { it.size == 2 } ?: return@mapNotNull null
                            val edge = Edge.entries.firstOrNull { it.relType == rel } ?: return@mapNotNull null
                            if (current[author]?.edges(edge)?.contains(target) == true) null else edge to target
                        }
                }
            val name =
                names(
                    batch.flatMapTo(HashSet()) { (author, _) ->
                        current[author]?.edges?.values.orEmpty().flatten() + lost.getValue(author).map { it.second } + author
                    },
                )
            redis.resource.use { r ->
                val pipe = r.pipelined()
                for ((author, _) in batch) {
                    val state = current[author]
                    val me = name.getValue(author)
                    for ((edge, prefix) in REVERSE_PREFIX) for (t in state?.edges(edge).orEmpty()) pipe.sadd(prefix + name.getValue(t), me)
                    for ((edge, target) in lost.getValue(author)) {
                        val prefix = REVERSE_PREFIX[edge] ?: continue
                        pipe.srem(prefix + name.getValue(target), me)
                    }
                    pipe.del(DESIGNATES_PREFIX + me)
                    val designated = state?.edges(Edge.DESIGNATES).orEmpty().map(name::getValue)
                    if (designated.isNotEmpty()) pipe.sadd(DESIGNATES_PREFIX + me, *designated.toTypedArray())
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
                    "MATCH (u:NostrUser) WHERE u.user_id IS NOT NULL WITH u, COUNT { (u)<-[:${edge.relType}]-() } AS n WHERE n > 0 RETURN u.user_id AS k, n"
                } else {
                    "MATCH (u:NostrUser) WHERE u.user_id IS NOT NULL WITH u, COUNT { (u)-[:${edge.relType}]->() } AS n WHERE n > 0 RETURN u.user_id AS k, n"
                }
            driver.session().use { s ->
                val result = s.run(degreeQuery)
                val batch = ArrayList<Pair<Long, Long>>(RECONCILE_BATCH)
                fun flush() {
                    if (batch.isEmpty()) return
                    underLock { replaced += replaceIfSizeDiffers(edge, prefix, reverse, batch) }
                    keys += batch.size
                    batch.clear()
                }
                while (result.hasNext()) {
                    val rec = result.next()
                    batch += rec["k"].asLong() to rec["n"].asLong()
                    if (batch.size >= RECONCILE_BATCH) flush()
                }
                flush()
            }
            // Pass 2: sets whose key has no edge left. A key that is not a User id is not ours to judge.
            redis.resource.use { r ->
                var cursor = ScanParams.SCAN_POINTER_START
                val params = ScanParams().match("$prefix*").count(RECONCILE_BATCH)
                do {
                    val page = r.scan(cursor, params)
                    cursor = page.cursor
                    val found = page.result.map { it.removePrefix(prefix) }.filter(USER_ID::matches).map(String::toLong)
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
        batch: List<Pair<Long, Long>>,
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
        keys: List<Long>,
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
        keys: List<Long>,
    ): Map<Long, Long> {
        val count = if (reverse) "COUNT { (k)<-[:${edge.relType}]-() }" else "COUNT { (k)-[:${edge.relType}]->() }"
        return driver.session().use { s ->
            s.executeRead { tx ->
                tx.run("UNWIND \$keys AS key MATCH (k:NostrUser {user_id: key}) RETURN key, $count AS n", mapOf("keys" to keys)).list { rec ->
                    rec["key"].asLong() to rec["n"].asLong()
                }.toMap()
            }
        }
    }

    /** The Redis members [keys] should have: their followers (reverse sets) or their designees, by User id. */
    private fun membersOf(
        edge: Edge,
        reverse: Boolean,
        keys: List<Long>,
    ): Map<Long, List<String>> {
        val pattern = if (reverse) "(k:NostrUser {user_id: key})<-[:${edge.relType}]-(m:NostrUser)" else "(k:NostrUser {user_id: key})-[:${edge.relType}]->(m:NostrUser)"
        return driver.session().use { s ->
            s.executeRead { tx ->
                tx.run("UNWIND \$keys AS key MATCH $pattern RETURN key, collect(toString(m.user_id)) AS members", mapOf("keys" to keys)).list { rec ->
                    rec["key"].asLong() to rec["members"].asList { it.asString() }
                }.toMap()
            }
        }
    }

    /** Everyone a write names, in Neo4j (authors it merges, targets it adds) or in Redis. */
    private fun named(changes: List<AuthorChange>): Set<String> {
        val out = HashSet<String>()
        for (c in changes) {
            if (c.bookkeepingChanged) out += c.author
            for (e in c.edges) {
                out += e.author
                out += e.added
                out += e.removed
                if (e.edge == Edge.DESIGNATES) out += e.desired
            }
        }
        return out
    }

    private fun ids(pubkeys: Collection<String>): Map<String, Int> =
        (userIds ?: error("no Scorecard database: the graph cannot be written")).idsFor(pubkeys)

    /** What each pubkey is called in the Redis sets. */
    private fun names(pubkeys: Collection<String>): Map<String, String> = ids(pubkeys).mapValues { it.value.toString() }

    /** Returns the pubkeys whose node already held a different `user_id`. */
    private fun writeEdges(
        tx: TransactionContext,
        edge: Edge,
        rows: List<EdgeChange>,
        ids: Map<String, Int>,
    ): List<String> {
        val removals = rows.filter { it.removed.isNotEmpty() }.map { mapOf("author" to it.author, "targets" to it.removed.toList()) }
        val added = rows.filter { it.added.isNotEmpty() }
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
        if (added.isEmpty()) return emptyList()
        // Not MERGE on the edge: it checks each edge against the author's, which grow with every
        // insert, so a list costs the square of its length (measured: 4,000 follows 1.2 s,
        // 8,000 2.7 s; a batch of 500 large lists over five minutes). The author's targets are
        // read once instead, and only the missing ones created: 0.1 s and 0.25 s. The added set
        // already excludes what the graph held; the check keeps a second writer from doubling an edge.
        // Each node is given its id where it has none.
        return tx
            .run(
                """
                UNWIND ${'$'}rows AS row
                MERGE (a:NostrUser {pubkey: row.author})
                FOREACH (_ IN CASE WHEN a.user_id IS NULL THEN [1] ELSE [] END | SET a.user_id = row.author_id)
                WITH a, row
                OPTIONAL MATCH (a)-[:${edge.relType}]->(held:NostrUser)
                WITH a, row, collect(held.pubkey) AS heldTargets
                UNWIND [t IN row.targets WHERE NOT t.pubkey IN heldTargets] AS tr
                    MERGE (t:NostrUser {pubkey: tr.pubkey})
                    FOREACH (_ IN CASE WHEN t.user_id IS NULL THEN [1] ELSE [] END | SET t.user_id = tr.id)
                    CREATE (a)-[:${edge.relType}]->(t)
                RETURN collect(DISTINCT CASE WHEN t.user_id <> tr.id THEN t.pubkey END) +
                       collect(DISTINCT CASE WHEN a.user_id <> row.author_id THEN a.pubkey END) AS mismatched
                """,
                mapOf(
                    "rows" to
                        added.map { c ->
                            mapOf(
                                "author" to c.author,
                                "author_id" to ids.getValue(c.author),
                                "targets" to c.added.map { mapOf("pubkey" to it, "id" to ids.getValue(it)) },
                            )
                        },
                ),
            ).single()["mismatched"]
            .asList { it.asString() }
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

    private fun writeRedis(
        changes: List<EdgeChange>,
        ids: Map<String, Int>,
    ) {
        if (changes.isEmpty()) return
        redis.resource.use { r ->
            val pipe = r.pipelined()
            for (c in changes) {
                val prefix = REVERSE_PREFIX[c.edge] ?: continue
                val author = ids.getValue(c.author).toString()
                for (target in c.added) pipe.sadd(prefix + ids.getValue(target), author)
                for (target in c.removed) pipe.srem(prefix + ids.getValue(target), author)
            }
            pipe.sync()
            // designates:<author> is replaced whole, in one MULTI, so a reader never sees half a list.
            for (c in changes) {
                if (c.edge != Edge.DESIGNATES) continue
                val key = DESIGNATES_PREFIX + ids.getValue(c.author)
                r.multi().apply {
                    del(key)
                    if (c.desired.isNotEmpty()) sadd(key, *c.desired.map { ids.getValue(it).toString() }.toTypedArray())
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
            session.run(USER_ID_CONSTRAINT).consume()
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

        /** A User id as a set name; brainstorm_server's `_USER_ID`. */
        private val USER_ID = Regex("[0-9]{1,10}")

        /** brainstorm_server's `create_pubkey_index`, verbatim in name and definition. */
        const val PUBKEY_CONSTRAINT = "CREATE CONSTRAINT nostr_user_pubkey IF NOT EXISTS FOR (u:NostrUser) REQUIRE u.pubkey IS UNIQUE"

        /** brainstorm_server's constraint on the id, verbatim in name and definition. */
        const val USER_ID_CONSTRAINT = "CREATE CONSTRAINT nostr_user_user_id IF NOT EXISTS FOR (u:NostrUser) REQUIRE u.user_id IS UNIQUE"

        private val log = LoggerFactory.getLogger(Neo4jRedisGraph::class.java)

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
            FOREACH (_ IN CASE WHEN u.user_id IS NULL THEN [1] ELSE [] END | SET u.user_id = row.user_id)
            SET u.follows_event = row.follows_event, u.follows_at = row.follows_at,
                u.mutes_event = row.mutes_event, u.mutes_at = row.mutes_at,
                u.designations_event = row.designations_event, u.designations_at = row.designations_at,
                u.report_events = row.report_events
            RETURN collect(DISTINCT CASE WHEN u.user_id <> row.user_id THEN u.pubkey END) AS mismatched
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
