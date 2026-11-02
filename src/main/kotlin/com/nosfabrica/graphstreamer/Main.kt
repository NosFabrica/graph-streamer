package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.mirror.Audit
import com.nosfabrica.graphstreamer.mirror.GraphIndexing
import com.nosfabrica.graphstreamer.mirror.LiveStream
import com.nosfabrica.graphstreamer.mirror.Mirror
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.nosfabrica.graphstreamer.store.RedisStreamerState
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger("graph-streamer")

/**
 * Long-running: one replica (the mirror is a single SQLite file and the dirty set assumes one
 * projector). Any loop that dies takes the process with it, and Kubernetes restarts it; the
 * dirty set and the mirror carry everything across the restart.
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
    log.info("graph-streamer: relay={} store={} dryRun={}", relay.url, config.storePath, config.dryRun)

    // The store's own relay URL scopes NIP-62: a vanish request naming the search relay, or
    // ALL_RELAYS, is applied; one naming any other relay is kept and does nothing.
    val store = EventStore(dbName = config.storePath, relay = relay, indexStrategy = GraphIndexing())
    val redis = JedisPool(config.redisHost, config.redisPort)
    val state = RedisStreamerState(redis)
    val http =
        OkHttpClient
            .Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.SECONDS) // a live REQ can be quiet for minutes
            .pingInterval(30, TimeUnit.SECONDS)
            .build()
    val client = NostrClient(BasicOkHttpWebSocket.Builder { http })

    Neo4jRedisGraph(config.neo4jUrl, config.neo4jUsername, config.neo4jPassword, redis).use { graph ->
        val mirror = Mirror(store, state)
        val projector = Projector(mirror, state, graph, config.dryRun, config.projectBatch)
        val live = LiveStream(client, relay, mirror, config.liveRefreshSeconds, config.liveOverlapSeconds)
        val audit = Audit(client, relay, mirror, config.auditWindow, config.auditMaxDeletes)

        try {
            coroutineScope {
                launch { live.run() }
                launch { projectForever(projector) }
                if (config.auditEveryHours > 0) launch { auditForever(audit, state, config.auditEveryHours * 3600) }
                launch {
                    while (isActive) {
                        delay(STATS_EVERY_MS)
                        log.info(
                            "stats: mirror accepted={} rejected={} duplicates={} removed={} dirty={} projected {}",
                            mirror.accepted.get(),
                            mirror.rejected.get(),
                            mirror.duplicates.get(),
                            mirror.removed.get(),
                            state.dirtyCount(),
                            projector.stats(),
                        )
                    }
                }
            }
        } finally {
            client.close()
            redis.close()
        }
    }
}

private suspend fun projectForever(projector: Projector) {
    var failures = 0
    while (true) {
        try {
            if (projector.projectOnce() == 0) delay(IDLE_MS)
            failures = 0
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // The batch stays dirty and is retried; a graph that stays down stops the process.
            failures++
            log.error("projection failed ({} in a row)", failures, e)
            if (failures >= MAX_PROJECTION_FAILURES) throw e
            delay(minOf(60_000L, 1_000L shl minOf(failures, 6)))
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
                fields["${r.name}.deleted"] = r.deleted.toString()
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

private const val IDLE_MS = 1_000L
private const val STATS_EVERY_MS = 60_000L
private const val MAX_PROJECTION_FAILURES = 20
