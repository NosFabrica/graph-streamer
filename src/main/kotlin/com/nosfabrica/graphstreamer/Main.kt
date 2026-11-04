package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.relay.AbsenceGuard
import com.nosfabrica.graphstreamer.relay.Audit
import com.nosfabrica.graphstreamer.relay.LiveStream
import com.nosfabrica.graphstreamer.relay.SearchRelayReader
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.nosfabrica.graphstreamer.store.Neo4jStreamerState
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger("graph-streamer")

/**
 * Long-running and stateless: one replica, because it is the graph's one writer for these edges.
 * Any loop that dies takes the process with it, and Kubernetes restarts it; the graph's
 * bookkeeping and its state node in Neo4j carry everything across.
 */
fun main() {
    val config = Config.fromEnv()
    val ok =
        try {
            // The Neo4j and Redis clients block. On runBlocking's one thread, a long write froze
            // every loop (live stream, resume point, stats) until it returned; IO threads don't.
            runBlocking(Dispatchers.IO) { run(config) }
            true
        } catch (e: Throwable) {
            log.error("graph-streamer stopped", e)
            false
        }
    exitProcess(if (ok) 0 else 1)
}

private suspend fun run(config: Config) {
    val relay = RelayUrlNormalizer.normalize(config.searchRelayUrl)
    log.info("graph-streamer: relay={} dryRun={} auditSince={}", relay.url, config.dryRun, config.auditSince)

    // Redis holds only the sets other services read: the GrapeRank worker and the server.
    val redis = JedisPool(config.redisHost, config.redisPort)
    val neo4j = GraphDatabase.driver(config.neo4jUrl, AuthTokens.basic(config.neo4jUsername, config.neo4jPassword))
    val state = Neo4jStreamerState(neo4j)
    val http =
        OkHttpClient
            .Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.SECONDS) // a live REQ can be quiet for minutes
            .pingInterval(30, TimeUnit.SECONDS)
            .build()
    val client = NostrClient(BasicOkHttpWebSocket.Builder { http })

    Neo4jRedisGraph(neo4j, redis).use { graph ->
        // A dry run changes nothing in the graph, schema included; it never reads these. It does
        // keep its own state node (Neo4jStreamerState).
        if (!config.dryRun) graph.ensureIndexes()

        val projector = Projector(graph, SearchRelayReader(client, relay), config.dryRun, config.projectBatch)
        val live = LiveStream(client, relay, projector, state, config.liveOverlapSeconds, config.liveResumePointSeconds * 1000)
        val audit = Audit(client, relay, graph, projector, config.dryRun, config.auditWindow, AbsenceGuard(config.auditMaxDeletes), since = config.auditSince)

        try {
            coroutineScope {
                launch { live.run() }
                if (config.auditEveryHours > 0) launch { auditForever(audit, state, config.auditEveryHours * 3600, config.auditRetryMinutes * 60) }
                launch {
                    while (isActive) {
                        delay(STATS_EVERY_MS)
                        log.info("stats: {}", projector.stats())
                    }
                }
            }
        } finally {
            client.close()
            redis.close()
        }
    }
}

private suspend fun auditForever(
    audit: Audit,
    state: StreamerState,
    everySeconds: Long,
    retrySeconds: Long,
) {
    while (true) {
        val now = System.currentTimeMillis() / 1000
        val wait = AuditSchedule.waitSeconds(now, state.lastAuditFinishedAt(), state.lastAuditEndedAt(), everySeconds, retrySeconds)
        if (wait > 0) delay(wait * 1000)

        val startedAt = System.currentTimeMillis() / 1000
        try {
            val reports = audit.run()
            val endedAt = (System.currentTimeMillis() / 1000).toString()
            val complete = reports.all { it.complete }
            val fields = mutableMapOf("started_at" to startedAt.toString(), "ended_at" to endedAt, "complete" to complete.toString())
            if (complete) fields["finished_at"] = endedAt
            for (r in reports) {
                fields["${r.name}.need"] = r.need.toString()
                fields["${r.name}.have"] = r.have.toString()
                fields["${r.name}.downloaded"] = r.downloaded.toString()
                fields["${r.name}.recomputed"] = r.recomputed.toString()
                fields["${r.name}.complete"] = r.complete.toString()
                fields["${r.name}.note"] = r.note ?: ""
            }
            state.putAuditReport(fields)
            if (!complete) log.warn("audit incomplete; it runs again in {} min", retrySeconds / 60)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("audit failed; next try in an hour", e)
            delay(3_600_000)
        }
    }
}

/** When the next audit is due. */
object AuditSchedule {
    /**
     * Seconds to wait from [now]: [everySeconds] after the last complete audit ([finishedAt]),
     * or [retrySeconds] after the last audit ([endedAt]) when that one did not complete. None yet: now.
     */
    fun waitSeconds(
        now: Long,
        finishedAt: Long?,
        endedAt: Long?,
        everySeconds: Long,
        retrySeconds: Long,
    ): Long {
        val due =
            when {
                endedAt != null && (finishedAt == null || endedAt > finishedAt) -> endedAt + retrySeconds
                finishedAt != null -> finishedAt + everySeconds
                else -> now
            }
        return (due - now).coerceAtLeast(0)
    }
}

private const val STATS_EVERY_MS = 60_000L
