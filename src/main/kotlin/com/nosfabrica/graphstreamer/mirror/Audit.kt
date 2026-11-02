package com.nosfabrica.graphstreamer.mirror

import com.nosfabrica.graphstreamer.Mirrored
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPagesResuming
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchByIds
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import org.slf4j.LoggerFactory

/**
 * The search relay is the source of truth for the mirror. An audit reconciles every mirrored
 * filter over NIP-77 and makes the mirror equal to the relay:
 *
 * - **need** (the relay has it, we don't): downloaded by id. Late arrivals, missed live events,
 *   and on the first run the whole past.
 * - **have** (we have it, the relay doesn't): deleted here, for whatever reason the relay dropped
 *   it — a NIP-09 deletion without a `k` tag, a vanish, an expiry, a superseded list, a ban.
 *   The same absence-based retraction as vespa-relay's `deleteMissing`.
 *
 * Absence is acted on only when it can be trusted:
 * - **The reconcile completed.** A window negentropy can't reconcile is paged instead (the mirror
 *   still fills) and that filter deletes nothing this audit. The relay itself refuses a Vespa
 *   answer with partial coverage, so a content node that is down fails the reconcile rather than
 *   looking like a third of the corpus was deleted.
 * - **At least one window was compared.**
 * - **Not the newest [graceSeconds].** The audit stops at `until = now − grace`: an event the
 *   live stream delivered may not be visible to the relay's id reads yet.
 * - **No more than [maxDeletes] per filter**, unless that guard is off (0). Unlike a provider's
 *   own relay, this one is a mirror that has lost data before (OOMs); a mass absence is likelier
 *   its fault than a mass retraction. Raise or disable it to let a real one through.
 */
class Audit(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val mirror: Mirror,
    private val targetWindow: Int,
    private val maxDeletes: Int,
    private val graceSeconds: Long = 3_600,
    private val idleTimeoutMs: Long = 120_000,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(Audit::class.java)

    data class FilterReport(
        val name: String,
        val need: Int,
        val have: Int,
        val downloaded: Int,
        val deleted: Int,
        val complete: Boolean,
        val note: String?,
    )

    suspend fun run(): List<FilterReport> {
        // No local expiry sweep: an expired event is dropped as a have here, which marks its
        // author dirty. A sweep would remove it silently and leave the author's edges standing.
        val until = clock() - graceSeconds
        return Mirrored.filters(until = until).mapIndexed { i, filter -> audit(NAMES[i], filter) }
    }

    private suspend fun audit(
        name: String,
        filter: Filter,
    ): FilterReport {
        val haves = ArrayList<String>()
        var downloaded = 0
        var complete = true
        var note: String? = null
        var need = 0

        try {
            val result =
                client.negentropyReconcile(
                    relay = relay,
                    filter = filter,
                    localIndex = NegentropyLocalIndex.of(mirror.store),
                    targetWindow = targetWindow,
                    idleTimeoutMs = idleTimeoutMs,
                    onUnreconcilableWindow = { window ->
                        complete = false
                        note = "a window could not be reconciled and was paged"
                        downloaded += page(window)
                    },
                    onHaveIds = { ids -> haves.addAll(ids) },
                    onNeedIds = { ids ->
                        val events = client.fetchByIds(relay, ids, idleTimeoutMs, reconciledFrom = filter)
                        mirror.ingest(events)
                        downloaded += events.size
                    },
                )
            need = result.needCount
            if (result.windows == 0) {
                complete = false
                note = "no window was compared"
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            complete = false
            note = "reconcile failed (${e.message}); paged instead"
            log.warn("audit {}: reconcile failed, paging the filter", name, e)
            downloaded += page(filter)
        }

        var deleted = 0
        when {
            haves.isEmpty() -> Unit
            !complete -> log.warn("audit {}: {} events the relay lacks kept: the reconcile did not complete", name, haves.size)
            maxDeletes in 1 until haves.size -> {
                note = "${haves.size} absent events exceed AUDIT_MAX_DELETES=$maxDeletes; nothing deleted"
                log.error("audit {}: {}", name, note)
            }
            else -> {
                mirror.remove(haves)
                deleted = haves.size
            }
        }

        val report = FilterReport(name, need, haves.size, downloaded, deleted, complete, note)
        log.info("audit {}", report)
        return report
    }

    /** Pages [filter] into the mirror when negentropy can't compare it. Fills only; deletes nothing. */
    private suspend fun page(filter: Filter): Int {
        val buffer = ArrayList<Event>(PAGE)
        var count = 0
        client.fetchAllPagesResuming(relay = relay, filters = listOf(filter), pageSize = PAGE) { event ->
            buffer.add(event)
            if (buffer.size >= PAGE) {
                mirror.ingest(buffer)
                count += buffer.size
                buffer.clear()
            }
        }
        mirror.ingest(buffer)
        return count + buffer.size
    }

    companion object {
        /** One per [Mirrored.filters] entry, in order. */
        val NAMES = listOf("lists", "reports", "deletions", "vanish")
        private const val PAGE = 500
    }
}
