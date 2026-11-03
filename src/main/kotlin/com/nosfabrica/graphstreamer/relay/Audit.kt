package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.ListKind
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.GraphStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPagesResuming
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchByIds
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CancellationException
import org.slf4j.LoggerFactory

/**
 * The search relay is the source of truth for the graph. An audit reconciles each graph kind
 * over NIP-77 against the event ids the graph records (list heads on author nodes, report ids):
 *
 * - **need** (the relay has it, the graph doesn't): downloaded and applied. Late arrivals, missed
 *   live events and, on the first run, the whole past.
 * - **have** (the graph has it, the relay doesn't): its author is recomputed from the relay, here
 *   and now, in [Projector.batchSize] batches, for
 *   whatever reason the relay dropped it — a NIP-09 deletion without a `k` tag, a vanish, an
 *   expiry, a ban. The same absence-based retraction as vespa-relay's `deleteMissing`.
 *
 * Absence is acted on only when [AbsenceGuard] trusts it.
 *
 * In a dry run the graph records nothing, so every audit compares against an empty side and
 * downloads the relay's whole set: the diffs it counts are how far the graph is from the relay.
 */
class Audit(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val graph: GraphStore,
    private val projector: Projector,
    private val dryRun: Boolean,
    private val targetWindow: Int,
    private val guard: AbsenceGuard,
    private val graceSeconds: Long = 3_600,
    private val idleTimeoutMs: Long = 120_000,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    private val log = LoggerFactory.getLogger(Audit::class.java)

    data class Report(
        val name: String,
        val need: Int,
        val have: Int,
        val downloaded: Int,
        val recomputed: Int,
        val complete: Boolean,
        val note: String?,
    )

    suspend fun run(): List<Report> {
        val until = clock() - graceSeconds
        return ListKind.entries.map { auditList(it, until) } + auditReports(until)
    }

    private suspend fun auditList(
        kind: ListKind,
        until: Long,
    ): Report {
        val local = if (dryRun) NegentropyLocalIndex.Empty else GraphListIndex(graph, kind)
        return audit(kind.prop, RelayFilters.audit(kind.kind, until), local, emptyList()) { haves -> graph.authorsOfLists(kind, haves) }
    }

    private suspend fun auditReports(until: Long): Report {
        val recorded = if (dryRun) emptyList() else graph.reports()
        val entries = localReportEntries(recorded, until)
        val authorOf = recorded.associate { (author, entry) -> entry.id to author }
        return audit("reports", RelayFilters.audit(RelayFilters.REPORTS, until), null, entries) { haves -> haves.mapNotNull(authorOf::get).distinct() }
    }

    private suspend fun audit(
        name: String,
        filter: Filter,
        localIndex: NegentropyLocalIndex?,
        localEntries: List<IdAndTime>,
        authorsOf: suspend (List<String>) -> List<String>,
    ): Report {
        val haves = ArrayList<String>()
        var downloaded = 0
        var unreconciled = false
        var failure: String? = null
        var need = 0
        var windows = 0

        try {
            val result =
                client.negentropyReconcile(
                    relay = relay,
                    filter = filter,
                    localEntries = localEntries,
                    localIndex = localIndex,
                    targetWindow = targetWindow,
                    idleTimeoutMs = idleTimeoutMs,
                    onUnreconcilableWindow = { window ->
                        unreconciled = true
                        downloaded += page(window)
                    },
                    onHaveIds = { ids -> haves.addAll(ids) },
                    onNeedIds = { ids ->
                        val events = client.fetchByIds(relay, ids, idleTimeoutMs, reconciledFrom = filter)
                        projector.apply(events)
                        downloaded += events.size
                    },
                )
            need = result.needCount
            windows = result.windows
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            failure = e.message ?: e.javaClass.simpleName
            log.warn("audit {}: reconcile failed, paging the filter", name, e)
            downloaded += page(filter)
        }

        val verdict = guard.decide(haves.size, completed = failure == null && !unreconciled, windows = windows)
        var recomputed = 0
        var recomputeFailure: String? = null
        if (verdict.act) {
            // A batch the relay can't answer in full writes nothing. The rest wait for the next
            // audit, which finds the same events still missing.
            try {
                for (batch in authorsOf(haves).chunked(projector.batchSize)) recomputed += projector.recompute(batch)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                recomputeFailure = e.message ?: e.javaClass.simpleName
                log.warn("audit {}: recompute stopped after {} authors", name, recomputed, e)
            }
        } else if (haves.isNotEmpty()) {
            log.warn("audit {}: {} events the relay lacks kept: {}", name, haves.size, verdict.reason)
        }

        val note =
            listOfNotNull(
                failure?.let { "reconcile failed ($it); paged instead" },
                verdict.reason.takeIf { haves.isNotEmpty() && !verdict.act },
                recomputeFailure?.let { "recompute stopped after $recomputed ($it); the rest wait for the next audit" },
            ).joinToString("; ").ifEmpty { null }
        val report = Report(name, need, haves.size, downloaded, recomputed, complete = failure == null && !unreconciled && windows > 0, note = note)
        log.info("audit {}", report)
        return report
    }

    /** Pages [filter] into the graph when negentropy can't compare it. Fills only; recomputes nobody. */
    private suspend fun page(filter: Filter): Int {
        val buffer = ArrayList<Event>(PAGE)
        var count = 0
        client.fetchAllPagesResuming(relay = relay, filters = listOf(filter), pageSize = PAGE) { event ->
            buffer.add(event)
            if (buffer.size >= PAGE) {
                projector.apply(buffer)
                count += buffer.size
                buffer.clear()
            }
        }
        projector.apply(buffer)
        return count + buffer.size
    }

    companion object {
        private const val PAGE = 500

        /** The recorded reports an audit up to [until] compares: newer ones would read as false haves. */
        fun localReportEntries(
            recorded: List<Pair<String, IdAndTime>>,
            until: Long,
        ): List<IdAndTime> = recorded.map { it.second }.filter { it.createdAt <= until }
    }
}

/**
 * When the relay's silence about an event may cost the graph an edge.
 *
 * - **The reconcile completed.** A window negentropy can't reconcile is paged instead and the
 *   filter recomputes nobody. The relay refuses a Vespa answer with partial coverage, so a
 *   content node that is down fails the reconcile instead of hiding a third of the corpus.
 * - **At least one window was compared.**
 * - **No more than [maxAbsent] absent events per filter**, unless that guard is off (0). Unlike a
 *   provider's own relay, this one is a mirror that has lost data before (OOMs); a mass absence
 *   is likelier its fault than a mass retraction. Raise or disable it to let a real one through.
 *
 * The audit also stops an hour short of now ([Audit] `graceSeconds`): an event the live stream
 * just delivered may not be visible to the relay's id reads yet.
 */
class AbsenceGuard(
    private val maxAbsent: Int,
) {
    data class Verdict(
        val act: Boolean,
        val reason: String,
    )

    fun decide(
        absent: Int,
        completed: Boolean,
        windows: Int,
    ): Verdict =
        when {
            absent == 0 -> Verdict(false, "nothing absent")
            !completed -> Verdict(false, "the reconcile did not complete")
            windows == 0 -> Verdict(false, "no window was compared")
            maxAbsent in 1 until absent -> Verdict(false, "$absent absent events exceed AUDIT_MAX_DELETES=$maxAbsent")
            else -> Verdict(true, "$absent absent events; their authors are recomputed")
        }
}

/** The graph's recorded lists of one kind, read one `created_at` window at a time. */
class GraphListIndex(
    private val graph: GraphStore,
    private val kind: ListKind,
) : NegentropyLocalIndex {
    override suspend fun count(window: Filter): Int? = graph.countLists(kind, window.since ?: 0, window.until ?: Long.MAX_VALUE)

    override suspend fun entriesFor(window: Filter): List<IdAndTime> = graph.lists(kind, window.since ?: 0, window.until ?: Long.MAX_VALUE)
}
