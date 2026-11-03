package com.nosfabrica.graphstreamer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SubscriptionClosedException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.subscribeEvents
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.CaughtUpResume
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.Command
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Read-only, against the real search relay; skipped unless LIVE_SEARCH_RELAY is set. Tails the
 * live filters, forces a reconnect, and checks a refused REQ ends its flow.
 */
class SearchRelayLiveProbeTest {
    private val enabled = System.getenv("LIVE_SEARCH_RELAY") != null
    private val relay = RelayUrlNormalizer.normalize(System.getenv("LIVE_SEARCH_RELAY")?.takeIf { it.startsWith("wss://") } ?: "wss://search.brainstorm.world")
    private val http =
        OkHttpClient
            .Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.SECONDS)
            .pingInterval(30, TimeUnit.SECONDS)
            .build()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    /** The `since`s of every REQ the client writes on a socket. */
    private val reqs = CopyOnWriteArrayList<List<Long?>>()
    private val recording =
        object : WebsocketBuilder {
            private val delegate = BasicOkHttpWebSocket.Builder { http }

            override fun build(
                url: NormalizedRelayUrl,
                out: WebSocketListener,
            ): WebSocket {
                val socket = delegate.build(url, out)
                return object : WebSocket by socket {
                    override fun send(msg: String): Boolean {
                        if (msg.startsWith("[\"REQ\"")) (runCatching { Command.fromJson(msg) }.getOrNull() as? ReqCmd)?.let { reqs += it.filters.map { f -> f.since } }
                        return socket.send(msg)
                    }
                }
            }
        }
    private val base = NostrClient(recording, scope)

    private fun now() = System.currentTimeMillis() / 1000

    @Test
    fun tailsTheRelaySurvivesAReconnectAndEndsOnARefusal() {
        if (!enabled) return
        runBlocking {
            val started = now()
            val ids = ConcurrentHashMap.newKeySet<String>()
            val kinds = ConcurrentHashMap<Int, AtomicInteger>()
            val received = AtomicInteger()
            val resume = CaughtUpResume()

            val stream =
                scope.async {
                    runCatching {
                        base
                            .subscribeEvents(
                                relay = relay,
                                filters = RelayFilters.live(since = started - 600),
                                resume = resume,
                            ).collect { e: Event ->
                                received.incrementAndGet()
                                ids += e.id
                                kinds.getOrPut(e.kind) { AtomicInteger() }.incrementAndGet()
                            }
                    }
                }

            // The point is the newest event handed over, so it exists once a stored one has been.
            withTimeout(120_000) { while (resume.caughtUpAt(relay) == null) delay(200) }
            println("EOSE after ${now() - started}s: ${received.get()} stored events in the last 10 min, by kind ${kinds.mapValues { it.value.get() }}")

            delay(60_000)
            val beforeReconnect = received.get()
            println("after 60s live: $beforeReconnect received total, resume point ${resume.caughtUpAt(relay)?.minus(started)}")
            assertTrue(stream.isActive, "the stream is still open: ${if (stream.isCompleted) stream.await().exceptionOrNull() else ""}")

            val lastPoint = resume.caughtUpAt(relay)!!
            base.reconnect(onlyIfChanged = false)
            withTimeout(60_000) { while (reqs.size < 2) delay(200) }
            println("the re-sent REQ asks from ${reqs[1].map { it?.minus(started) }} (resume point ${lastPoint - started}, relative to start)")
            assertTrue(reqs[1].all { it != null && it >= lastPoint }, "the re-sent REQ asks from the resume point")

            delay(45_000)
            val afterReconnect = received.get() - beforeReconnect
            println("45s after the reconnect: $afterReconnect more received (${received.get() - ids.size} duplicates overall, the overlap re-read)")
            assertTrue(stream.isActive, "the stream survived the reconnect: ${if (stream.isCompleted) stream.await().exceptionOrNull() else ""}")
            assertTrue(afterReconnect > 0, "events still arrive after the reconnect")
            assertEquals(2, reqs.size, "one REQ per connection: the resumed one isn't undone at its EOSE")
            stream.cancel()

            // No lens token: the relay refuses an unauthenticated read, and this client can't AUTH.
            val refusedAt = now()
            val outcome = withTimeout(30_000) { runCatching { base.subscribeEvents(relay, listOf(Filter(kinds = listOf(3), limit = 1))).collect { } }.exceptionOrNull() }
            println("a REQ without a lens: ${outcome?.message} after ${now() - refusedAt}s")
            assertIs<SubscriptionClosedException>(outcome)
        }
        base.close()
        scope.cancel()
    }

    /** How many graph events each relay holds from the last 10 minutes: what the live stream should see. */
    @Test
    fun comparesTheSearchRelaysRecentGraphEventsWithPublicRelays() {
        if (!enabled) return
        runBlocking {
            val since = now() - 600
            val relays = listOf(relay.url, "wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net")
            for (url in relays) {
                val target = RelayUrlNormalizer.normalize(url)
                val search = if (target == relay) RelayFilters.UNRANKED else null
                val counts = linkedMapOf<Int, Int>()
                val ages = mutableListOf<String>()
                for (kind in RelayFilters.GRAPH_KINDS + RelayFilters.VANISH) {
                    var n = 0
                    val result =
                        base.fetchAllPages(target, listOf(Filter(kinds = listOf(kind), since = since, search = search)), idleTimeoutMs = 20_000) {
                            n++
                            if (target == relay) ages += "k$kind ${it.id.take(8)} created ${now() - it.createdAt}s ago"
                        }
                    counts[kind] = n
                    if (result.end.name != "DRAINED") counts[-kind] = -1
                }
                println("last 10 min on $url: ${counts.filterKeys { it > 0 }} ${if (counts.keys.any { it < 0 }) "(some walks did not drain)" else ""}")
                if (ages.isNotEmpty()) println("  $ages")
            }
        }
        base.close()
        scope.cancel()
    }
}
