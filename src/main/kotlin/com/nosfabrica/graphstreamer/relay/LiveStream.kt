package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.Projector
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

/**
 * One long-lived REQ on the search relay ([RelayFilters.live]). The relay fans its sync's writes
 * out to live subscribers like any other publish, so this sees each new list, report, deletion
 * and vanish as the relay stores it.
 *
 * Quartz re-sends the REQ itself after a reconnect, with the filters it was last given. So the
 * REQ is given a fresh `since` (now minus [overlapSeconds]) every [refreshSeconds]: a reconnect
 * re-reads at most that much, and re-applying an event changes nothing. A longer gap, and
 * anything the relay stores late with an old `created_at`, is the audit's job.
 */
class LiveStream(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val projector: Projector,
    private val refreshSeconds: Long,
    private val overlapSeconds: Long,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(LiveStream::class.java)

    suspend fun run(): Unit =
        coroutineScope {
            val inbox = Channel<Event>(INBOX)
            val listener =
                object : SubscriptionListener {
                    override suspend fun onEvent(
                        event: Event,
                        isLive: Boolean,
                        relay: NormalizedRelayUrl,
                        forFilters: List<Filter>?,
                    ) {
                        inbox.send(event)
                    }

                    override fun onClosed(
                        message: String,
                        relay: NormalizedRelayUrl,
                        forFilters: List<Filter>?,
                    ) {
                        // `auth-required:` here means the filters lost their lens token: nothing will arrive.
                        log.error("live REQ closed by {}: {}", relay.url, message)
                    }

                    override fun onCannotConnect(
                        relay: NormalizedRelayUrl,
                        message: String,
                        forFilters: List<Filter>?,
                    ) {
                        log.warn("cannot connect to {}: {}", relay.url, message)
                    }
                }

            launch {
                val batch = ArrayList<Event>(BATCH)
                while (isActive) {
                    batch.add(inbox.receive())
                    while (batch.size < BATCH) batch.add(inbox.tryReceive().getOrNull() ?: break)
                    try {
                        projector.apply(batch)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        // Lost to this stream, not to the graph: the next audit downloads them again.
                        log.error("applying {} live events failed", batch.size, e)
                    }
                    batch.clear()
                }
            }

            try {
                while (isActive) {
                    client.subscribe(SUB_ID, mapOf(relay to RelayFilters.live(since = clock() - overlapSeconds)), listener)
                    delay(refreshSeconds * 1000)
                }
            } finally {
                client.unsubscribe(SUB_ID)
            }
        }

    companion object {
        const val SUB_ID = "graph-streamer-live"
        private const val INBOX = 10_000
        private const val BATCH = 500
    }
}
