package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.subscribeEvents
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.CaughtUpResume
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.ResumePolicy
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
 * The live tail of the search relay ([RelayFilters.live]), applied in batches. The relay fans
 * its sync's writes out to live subscribers like any other publish, so this sees each new list,
 * report, deletion and vanish as the relay stores it.
 *
 * Built on Quartz's `subscribeEvents`:
 * - **Reconnects resume.** The client reconnects on its own, and each time it writes the REQ again
 *   Quartz's [CaughtUpResume] moves its `since` to the newest `created_at` this stream was handed
 *   (stored events counted at EOSE, capped at this machine's clock), minus [overlapSeconds]
 *   (default 0). So the re-sent REQ asks from where the outage began. Re-applying an event
 *   writes nothing.
 * - **Restarts resume.** That point ([CaughtUpResume.caughtUpAt], minus [overlapSeconds]) is saved
 *   every [resumePointIntervalMs] ([StreamerState.putLiveSince]) and the next process starts from
 *   it. It covers what reached this stream, which can run ahead of what the projector applied by
 *   the inbox's backlog; an overlap covers a backlog shorter than it, and the audit anything beyond.
 * - **A CLOSED ends the stream, and the process.** After a CLOSED the client doesn't send the REQ
 *   again on the same connection, only on the next reconnect or AUTH (and never once the relay
 *   has refused the same filters three times), so the stream could go quiet for hours. This client
 *   has no NIP-42 responder, so even an `auth-required:` one ends [run] at once. Two kinds don't
 *   end it, because the client recovers from them itself: a rate limit with a stated back-off, and
 *   an `error:` or unprefixed CLOSED followed within 3 s by the connection dropping (a relay
 *   restarting), unless that happens three times running. Main lets the rest stop the process,
 *   and Kubernetes restarts it with a fresh client.
 *
 * What the relay stores late with an old `created_at` never streams; the audit downloads it.
 */
class LiveStream(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val projector: Projector,
    private val state: StreamerState,
    private val overlapSeconds: Long,
    private val resumePointIntervalMs: Long,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(LiveStream::class.java)

    suspend fun run(): Unit =
        coroutineScope {
            val inbox = Channel<Event>(INBOX)
            val caughtUp = CaughtUpResume(nowInSeconds = clock)
            launch {
                client
                    .subscribeEvents(
                        relay = relay,
                        filters = RelayFilters.live(since = state.liveSince() ?: (clock() - overlapSeconds)),
                        resume = if (overlapSeconds == 0L) caughtUp else Overlap(caughtUp, overlapSeconds),
                    ).collect { inbox.send(it) }
            }
            // Saves the resume point for a restart, on this service's own schedule.
            launch {
                var saved: Long? = null
                while (isActive) {
                    delay(resumePointIntervalMs)
                    val point = caughtUp.caughtUpAt(relay)?.minus(overlapSeconds) ?: continue
                    if (point != saved) {
                        state.putLiveSince(point)
                        saved = point
                    }
                }
            }

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

    /** [inner]'s resume point, [seconds] earlier: `LIVE_OVERLAP_SECONDS`. */
    private class Overlap(
        private val inner: CaughtUpResume,
        private val seconds: Long,
    ) : ResumePolicy by inner {
        override fun resumeFrom(
            relay: NormalizedRelayUrl,
            filters: List<Filter>,
        ): Long? = inner.resumeFrom(relay, filters)?.minus(seconds)
    }

    companion object {
        private const val INBOX = 10_000
        private const val BATCH = 500
    }
}
