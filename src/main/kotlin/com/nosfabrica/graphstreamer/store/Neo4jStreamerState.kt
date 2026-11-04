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
 *
 * Written in a dry run too: it is not the graph, and without it every restart would run a full
 * dry-run audit, which downloads the relay's whole set.
 */
class Neo4jStreamerState(
    private val driver: Driver,
) : StreamerState {
    override fun lastAuditFinishedAt(): Long? = read("audit_finished_at")?.toLongOrNull()

    override fun lastAuditEndedAt(): Long? = read("audit_ended_at")?.toLongOrNull()

    override fun liveSince(): Long? = read("live_since")?.toLongOrNull()

    override fun putLiveSince(since: Long) = write(mapOf("live_since" to since.toString()))

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
