package com.nosfabrica.graphstreamer.store

import org.neo4j.driver.Driver

/**
 * [StreamerState] as one `(:GraphStreamerState {name: "graph-streamer"})` node in the graph's
 * Neo4j, beside the graph it describes. Durable where the graph is, backed up with it, and
 * nothing a cache could evict.
 *
 * - `live_since`: epoch seconds the live stream resumes from after a restart.
 * - `audit_started_at`, `audit_ended_at`, `audit_complete`, and per audited kind
 *   `audit_<kind>_<counter>` (`need`, `have`, `downloaded`, `recomputed`, `complete`, `note`): the
 *   last audit's report. `audit_finished_at` is when the last complete one ended.
 * - `adopted_<kind>`: "true" once an orphan sweep of that kind found none (see [com.nosfabrica.graphstreamer.relay.Audit]).
 * - `redis_reconciled_at`, `redis_keys`, `redis_replaced`, `redis_deleted`, `redis_outbox`: the last
 *   Redis reconcile (sets checked, replaced, deleted) and the authors the outbox last replayed.
 *
 * Written in a dry run too: it is not the graph, and without it every restart would run a full
 * dry-run audit, which downloads the relay's whole set.
 *
 * Its loops (the live stream's resume point, the audit, the Redis reconcile) write it at once, and
 * a MERGE is atomic only under a uniqueness constraint: without one, two first writes made two
 * nodes, and the schedule read the one without `audit_finished_at` and audited again. The
 * constraint is created before the first write, in a dry run too (the label is ours, not the
 * graph's), after folding any duplicates an earlier version left into one.
 */
class Neo4jStreamerState(
    private val driver: Driver,
) : StreamerState {
    @Volatile private var constrained = false
    private val constraining = Any()

    private fun ensureConstraint() {
        if (constrained) return
        synchronized(constraining) {
            if (constrained) return
            driver.session().use { s ->
                // The node with the most properties is the one that was written to longest.
                s.run(
                    "MATCH (s:$LABEL) WITH s.name AS name, s ORDER BY size(keys(s)) DESC " +
                        "WITH name, collect(s) AS nodes WHERE size(nodes) > 1 " +
                        "UNWIND nodes[1..] AS extra DETACH DELETE extra",
                ).consume()
                s.run("CREATE CONSTRAINT graph_streamer_state_name IF NOT EXISTS FOR (s:$LABEL) REQUIRE s.name IS UNIQUE").consume()
            }
            constrained = true
        }
    }
    override fun lastAuditFinishedAt(): Long? = read("audit_finished_at")?.toLongOrNull()

    override fun lastAuditEndedAt(): Long? = read("audit_ended_at")?.toLongOrNull()

    override fun liveSince(): Long? = read("live_since")?.toLongOrNull()

    override fun putLiveSince(since: Long) = write(mapOf("live_since" to since.toString()))

    override fun lastRedisReconcileAt(): Long? = read("redis_reconciled_at")?.toLongOrNull()

    override fun putRedisReport(report: Map<String, String>) = write(report.mapKeys { (key, _) -> "redis_$key" })

    override fun adopted(name: String): Boolean = read("adopted_$name") == "true"

    override fun markAdopted(name: String) = write(mapOf("adopted_$name" to "true"))

    override fun putAuditReport(report: Map<String, String>) = write(report.mapKeys { (key, _) -> "audit_" + key.replace('.', '_') })

    private fun read(property: String): String? =
        driver.session().use { session ->
            session.executeRead { tx ->
                tx
                    .run("MATCH (s:$LABEL {name: \$name}) RETURN s[\$property] AS value", mapOf("name" to NAME, "property" to property))
                    .list()
                    .firstOrNull()
                    ?.get("value")
                    ?.takeUnless { it.isNull }
                    ?.asString()
            }
        }

    /** Every value is a string: two numbers and a report, read back the way they were written. */
    private fun write(properties: Map<String, String>) {
        ensureConstraint()
        driver.session().use { session ->
            session.executeWriteWithoutResult { tx ->
                tx.run("MERGE (s:$LABEL {name: \$name}) SET s += \$properties", mapOf("name" to NAME, "properties" to properties)).consume()
            }
        }
    }

    companion object {
        const val LABEL = "GraphStreamerState"
        const val NAME = "graph-streamer"
    }
}
