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
            runBlocking { run(config) }
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
                if (config.auditEveryHours > 0) launch { auditForever(audit, state, config.auditEveryHours * 3600) }
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
) {
    while (true) {
        val now = System.currentTimeMillis() / 1000
        val last = state.lastAuditFinishedAt()
        val wait = if (last == null) 0 else (last + everySeconds - now).coerceAtLeast(0)
        if (wait > 0) delay(wait * 1000)

        val startedAt = System.currentTimeMillis() / 1000
        try {
            val reports = audit.run()
            val fields = mutableMapOf("started_at" to startedAt.toString(), "finished_at" to (System.currentTimeMillis() / 1000).toString())
            for (r in reports) {
                fields["${r.name}.need"] = r.need.toString()
                fields["${r.name}.have"] = r.have.toString()
                fields["${r.name}.downloaded"] = r.downloaded.toString()
                fields["${r.name}.recomputed"] = r.recomputed.toString()
                fields["${r.name}.complete"] = r.complete.toString()
                fields["${r.name}.note"] = r.note ?: ""
            }
            state.putAuditReport(fields)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("audit failed; next try in an hour", e)
            delay(3_600_000)
        }
    }
}

private const val STATS_EVERY_MS = 60_000L
