package com.nosfabrica.graphstreamer.mirror

import com.nosfabrica.graphstreamer.Mirrored
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

/**
 * One long-lived REQ on the search relay for the mirrored filters. The relay fans its sync's
 * writes out to live subscribers like any other publish, so this sees each new list, report,
 * deletion and vanish as the relay stores it.
 *
 * Quartz re-sends the REQ itself after a reconnect, with the filters it was last given. So the
 * REQ is given a fresh `since` (now minus [overlapSeconds]) every [refreshSeconds]: a reconnect
 * re-reads at most that much, and the mirror drops what it already holds. A gap longer than the
 * overlap, and anything the relay stores late with an old `created_at`, is the audit's job.
 */
class LiveStream(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val mirror: Mirror,
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
                        mirror.ingest(batch)
                    } catch (e: Exception) {
                        // The events are lost to this stream, not to the mirror: the audit re-reads them.
                        log.error("ingest of {} live events failed", batch.size, e)
                    }
                    batch.clear()
                }
            }

            try {
                while (isActive) {
                    val since = clock() - overlapSeconds
                    client.subscribe(SUB_ID, mapOf(relay to Mirrored.filters(since = since)), listener)
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
