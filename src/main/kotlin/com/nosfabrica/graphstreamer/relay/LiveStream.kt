package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.subscribeEventBatches
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.CaughtUpResume
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.sample
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

/**
 * The live tail of the search relay ([RelayFilters.live]), written to the graph a batch at a time.
 * Deletions and vanishes aren't streamed: the audit catches them, and anything this misses.
 *
 * Quartz's `subscribeEventBatches` does the subscription (reconnects, backpressure, the CLOSEDs
 * that end it: those stop the process). This decides the rest:
 * - **Where a restart resumes.** [CaughtUpResume]'s point counts only batches written here; it is
 *   saved every [resumePointIntervalMs] and the next process asks from it. A reconnect resumes
 *   from it on its own.
 * - **A failed write is retried** with backoff for [RETRY_FOR] (a store restarting), then left to
 *   the next audit. Not for ever: while a batch waits, so does every message on the relay's
 *   connection, the audit's included.
 */
class LiveStream(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val projector: Projector,
    private val state: StreamerState,
    private val resumePointIntervalMs: Long,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(LiveStream::class.java)

    @OptIn(FlowPreview::class)
    suspend fun run(): Unit =
        coroutineScope {
            val resume = CaughtUpResume(nowInSeconds = clock)
            launch {
                resume.caughtUpAtFlow(relay).filterNotNull().sample(resumePointIntervalMs).collect { state.putLiveSince(it) }
            }
            client
                .subscribeEventBatches(relay, RelayFilters.live(since = state.liveSince() ?: clock()), resume, maxBatch = BATCH)
                .collect { batch -> write(batch) }
        }

    private suspend fun write(batch: List<Event>) {
        val giveUp = TimeSource.Monotonic.markNow() + RETRY_FOR
        var wait = 1_000L
        while (true) {
            try {
                projector.apply(batch)
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (giveUp.hasPassedNow()) {
                    log.error("applying {} live events failed; left to the next audit", batch.size, e)
                    return
                }
                log.warn("applying {} live events failed; trying again in {} ms", batch.size, wait, e)
                delay(wait)
                wait = minOf(wait * 2, 30_000)
            }
        }
    }

    companion object {
        private const val BATCH = 500
        private val RETRY_FOR = 2.minutes
    }
}
