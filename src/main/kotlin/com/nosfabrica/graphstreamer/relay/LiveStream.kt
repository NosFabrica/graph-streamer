package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
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
 * Built on [subscribeEvents]:
 * - **Reconnects resume.** The client reconnects on its own. When the connection drops, the
 *   stream rewrites its `since` to when it was last caught up (EOSE, live events), minus
 *   [overlapSeconds] (default 0), so the re-sent REQ asks from when the outage began. Re-applying
 *   an event writes nothing.
 * - **Restarts resume.** The resume point ([CaughtUpResume.resumeFrom]) is saved every
 *   [resumePointIntervalMs] ([StreamerState.putLiveSince]) and the next process starts from it.
 *   It covers what reached this stream, which can run ahead of what the projector applied by the
 *   inbox's backlog; an overlap covers a backlog shorter than it, and the audit anything beyond.
 * - **A CLOSED ends the stream, and the process.** After a CLOSED the client doesn't send the REQ
 *   again on the same connection, only on the next reconnect or AUTH (and never once the relay
 *   has refused the same filters three times), so the stream could go quiet for hours. This client has no NIP-42
 *   responder, so even an `auth-required:` one ends [run] at once; only a rate limit with a
 *   stated back-off, which the client retries, doesn't. Main lets it stop the process, and
 *   Kubernetes restarts it with a fresh client.
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
            val resume = CaughtUpResume(overlapSeconds, nowInSeconds = clock)
            launch {
                client
                    .subscribeEvents(
                        relay = relay,
                        filters = RelayFilters.live(since = state.liveSince() ?: (clock() - overlapSeconds)),
                        resume = resume,
                    ).collect { inbox.send(it) }
            }
            // Saves the resume point for a restart, on this service's own schedule.
            launch {
                var saved: Long? = null
                while (isActive) {
                    delay(resumePointIntervalMs)
                    val point = resume.resumeFrom() ?: continue
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

    companion object {
        private const val INBOX = 10_000
        private const val BATCH = 500
    }
}
