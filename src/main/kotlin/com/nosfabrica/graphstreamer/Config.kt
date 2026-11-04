package com.nosfabrica.graphstreamer

/** Everything the service reads from the environment. Graph and Redis names match the GrapeRank worker's. */
data class Config(
    val neo4jUrl: String,
    val neo4jUsername: String,
    val neo4jPassword: String,
    val redisHost: String,
    val redisPort: Int,
    /** Our search relay: it mirrors these kinds from every certified relay and verifies what it stores. */
    val searchRelayUrl: String = "wss://search.brainstorm.world",
    /**
     * True: project and log the diffs against the graph, write nothing to Neo4j or the graph's
     * Redis sets. The default, so a fresh deploy beside the server's consumer can't fight it.
     */
    val dryRun: Boolean = true,
    /** A negentropy audit of every graph kind against the relay this often; 0 turns it off. */
    val auditEveryHours: Long = 24,
    /**
     * An audit that did not complete (a reconcile failed) runs again this long after it ended,
     * not [auditEveryHours]: it resumes as a delta, and a first rebuild shouldn't sit half done for a day.
     */
    val auditRetryMinutes: Long = 30,
    /**
     * When set, audits compare only events created at or after this (epoch seconds), on both
     * sides. For a rehearsal or a partial rebuild; older events are neither downloaded nor checked.
     */
    val auditSince: Long? = null,
    /** Events per `created_at` window an audit reconciles at once (both sides read windowed). */
    val auditWindow: Int = 50_000,
    /**
     * An audit recomputes the authors of events the relay no longer holds, but not when more
     * than this many are absent in one filter (likelier a relay fault than real retractions);
     * 0 = no limit.
     */
    val auditMaxDeletes: Int = 10_000,
    /**
     * How often the live stream's resume point (the newest `created_at` it was handed, minus
     * [liveOverlapSeconds]) is saved for a restart. A reconnect needs no saving: Quartz resumes
     * the REQ it re-sends from that point.
     */
    val liveResumePointSeconds: Long = 60,
    /**
     * How far before the resume point a reconnect, or a restart, asks from. 0: from the point
     * itself. Events still queued for the graph at a restart are then left to the next audit.
     */
    val liveOverlapSeconds: Long = 0,
    /** Authors recomputed from the relay per batch. */
    val projectBatch: Int = 200,
) {
    companion object {
        fun fromEnv(env: Map<String, String> = System.getenv()): Config {
            fun required(name: String) = env[name]?.takeIf { it.isNotBlank() } ?: error("$name is not set")

            fun optional(name: String) = env[name]?.takeIf { it.isNotBlank() }

            val defaults = Config("", "", "", "", 0)
            return Config(
                neo4jUrl = required("NEO4J_URL"),
                neo4jUsername = required("NEO4J_USERNAME"),
                neo4jPassword = required("NEO4J_PASSWORD"),
                redisHost = required("REDIS_HOST"),
                redisPort = required("REDIS_PORT").toInt(),
                searchRelayUrl = optional("SEARCH_RELAY_URL") ?: defaults.searchRelayUrl,
                dryRun = optional("DRY_RUN")?.let(::parseSwitch) ?: defaults.dryRun,
                auditEveryHours = optional("AUDIT_EVERY_HOURS")?.toLong() ?: defaults.auditEveryHours,
                auditRetryMinutes = optional("AUDIT_RETRY_MINUTES")?.toLong() ?: defaults.auditRetryMinutes,
                auditSince = optional("AUDIT_SINCE")?.toLong(),
                auditWindow = optional("AUDIT_WINDOW")?.toInt() ?: defaults.auditWindow,
                auditMaxDeletes = optional("AUDIT_MAX_DELETES")?.toInt() ?: defaults.auditMaxDeletes,
                liveResumePointSeconds = optional("LIVE_RESUME_POINT_SECONDS")?.toLong() ?: defaults.liveResumePointSeconds,
                liveOverlapSeconds = optional("LIVE_OVERLAP_SECONDS")?.toLong() ?: defaults.liveOverlapSeconds,
                projectBatch = optional("PROJECT_BATCH")?.toInt() ?: defaults.projectBatch,
            )
        }

        /** A typo must not silently mean "write to the graph": anything unrecognised stops the boot. */
        fun parseSwitch(value: String): Boolean =
            when (value.trim().lowercase()) {
                "true", "1", "yes", "on" -> true
                "false", "0", "no", "off" -> false
                else -> error("not a switch: '$value' (true/false)")
            }
    }
}
