package com.nosfabrica.graphstreamer.relay

// A copy of Quartz's ResumePolicy.kt (quartz nip01Core/relay/client/accessories/, vitorpamplona/quartz#82),
// not released yet. Delete this file and import Quartz's once graph-streamer depends on the release
// that has it; nothing else changes.

import com.vitorpamplona.quartz.utils.TimeUtils
import kotlin.concurrent.Volatile

/**
 * Where a [subscribeEvents] subscription resumes after its connection drops. The subscription
 * only reports and asks; the policy decides.
 *
 * Both calls come from the client's own threads: [onCaughtUp] from the connection's delivery
 * coroutine, which every subscription on that relay shares, and [resumeFrom] from its disconnect
 * callback. Keep both cheap and non-blocking; save state elsewhere, on your own schedule.
 */
interface ResumePolicy {
    /** At EOSE and at each live event: the subscription is caught up with the relay as of now. */
    fun onCaughtUp()

    /**
     * On a disconnect, before the client re-sends the REQ: the `since` to ask from, or null to keep
     * the filters as they are. A filter is never moved earlier than its own `since`, nor earlier
     * than a point already applied.
     */
    fun resumeFrom(): Long?
}

/**
 * The default [ResumePolicy]: the last moment the subscription was caught up, minus
 * [overlapSeconds]. A connection that drops for an hour asks again from just before the drop.
 *
 * `since` filters on `created_at`, so an event the relay stores while the connection is down but
 * created before the resume point (a relay mirroring others does this all the time) is not sent
 * again, nor is anything past the relay's cap on stored results for one REQ. The overlap narrows
 * the first; if every event matters, reconcile the gap too (`negentropySync`).
 *
 * To resume after a restart, read [resumeFrom] on your own schedule, persist it, and set it as the
 * filters' `since` next time. It covers what was handed to the flow, so it can run ahead of what a
 * lagging collector has processed by up to its buffer; the overlap covers a lag shorter than it.
 */
class CaughtUpResume(
    val overlapSeconds: Long = 60,
    private val nowInSeconds: () -> Long = TimeUtils::now,
) : ResumePolicy {
    /** Epoch seconds the subscription was last caught up; 0 = not yet. */
    @Volatile
    var caughtUpAt: Long = 0L
        private set

    override fun onCaughtUp() {
        caughtUpAt = nowInSeconds()
    }

    override fun resumeFrom(): Long? = caughtUpAt.takeIf { it > 0L }?.minus(overlapSeconds)
}
