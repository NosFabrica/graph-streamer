package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListKind
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.GraphStore
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.count
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
 * A reconcile that fails (the relay disconnects, times out, errors) is reported incomplete and
 * pages nothing: the next audit re-runs it as a delta against the bookkeeping, which already holds
 * everything applied so far. Paging the whole filter instead would download every list again,
 * the whole corpus on a full audit. Only a window handed to `onUnreconcilableWindow`, which is
 * bounded, is paged.
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
    /** [com.nosfabrica.graphstreamer.Config.auditSince]: both sides are compared from here on. */
    private val since: Long? = null,
    private val idleTimeoutMs: Long = 120_000,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
    /** Where the orphan sweep records that a kind is adopted; null: never sweep. */
    private val state: StreamerState? = null,
) {
    private val log = LoggerFactory.getLogger(Audit::class.java)

    data class Report(
        val name: String,
        val need: Int,
        val have: Int,
        /** Authors with this kind's edges and no bookkeeping, found by the sweep (-1: not swept). */
        val orphans: Int,
        /** Downloaded and not recorded: the next audit downloads these again. */
        val ignored: Int,
        val windows: Int,
        val downloaded: Int,
        val recomputed: Int,
        val complete: Boolean,
        val note: String?,
    )

    suspend fun run(): List<Report> {
        val until = clock() - graceSeconds
        return ListKind.entries.map { sweep(auditList(it, until), it.edge) } + sweep(auditReports(until), Edge.REPORTS)
    }

    /**
     * After a complete reconcile, the authors with [edge] edges and no bookkeeping: edges another
     * writer made (the server's consumer, before cutover) that no audit compares, because the
     * relay holds no event for them or the graph never recorded one. Each is rebuilt from the
     * relay, behind the same guard as absences. A sweep that finds none marks the kind adopted:
     * from then on every edge of it has bookkeeping, and the sweep, a full scan, stops. Not with
     * AUDIT_SINCE, where an author without bookkeeping may only have an older list. A dry run
     * counts and changes nothing.
     */
    private suspend fun sweep(
        report: Report,
        edge: Edge,
    ): Report {
        if (state == null || since != null || !report.complete || (!dryRun && state.adopted(report.name))) return report
        val cap = guard.maxAbsent
        val orphans = graph.orphanAuthors(edge, if (cap > 0) cap + 1 else Int.MAX_VALUE)
        if (dryRun) return report.copy(orphans = orphans.size)
        if (orphans.isEmpty()) {
            state.markAdopted(report.name)
            return report.copy(orphans = 0)
        }
        if (cap in 1 until orphans.size) {
            val note = "more than $cap authors have ${edge.relType} edges and no bookkeeping; raise AUDIT_MAX_DELETES to rebuild them"
            log.warn("audit {}: {}", report.name, note)
            return report.copy(orphans = orphans.size, note = listOfNotNull(report.note, note).joinToString("; "))
        }
        var recomputed = 0
        try {
            for (batch in orphans.chunked(projector.batchSize)) recomputed += projector.recompute(batch)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warn("audit {}: orphan rebuild stopped after {}", report.name, recomputed, e)
            return report.copy(orphans = orphans.size, recomputed = report.recomputed + recomputed, note = listOfNotNull(report.note, "orphan rebuild stopped after $recomputed").joinToString("; "))
        }
        log.info("audit {}: rebuilt {} authors with edges and no bookkeeping", report.name, recomputed)
        return report.copy(orphans = orphans.size, recomputed = report.recomputed + recomputed)
    }

    private suspend fun auditList(
        kind: ListKind,
        until: Long,
    ): Report {
        val local = if (dryRun) NegentropyLocalIndex.Empty else GraphListIndex(graph, kind)
        return audit(kind.prop, RelayFilters.audit(kind.kind, until, since), bothSides(local), emptyList()) { haves -> graph.authorsOfLists(kind, haves) }
    }

    private suspend fun auditReports(until: Long): Report {
        val recorded = if (dryRun) emptyList() else graph.reports()
        val entries = localReportEntries(recorded, until, since)
        val authorOf = recorded.associate { (author, entry) -> entry.id to author }
        return audit("reports", RelayFilters.audit(RelayFilters.REPORTS, until, since), bothSides(NegentropyLocalIndex.of(entries)), emptyList()) { haves ->
            haves.mapNotNull(authorOf::get).distinct()
        }
    }

    /**
     * [local], with each window's count raised to the relay's when that is larger (NIP-45 COUNT):
     * Quartz splits a window when the count is over the target, and only our side's count
     * reached it. On a first audit or a migration our side is empty, so the whole corpus went to
     * the relay as one window (the rehearsal: 21,556 mute lists in one; in production, every
     * follow list). A relay that doesn't answer the COUNT leaves our side's count.
     */
    private fun bothSides(local: NegentropyLocalIndex): NegentropyLocalIndex = BothSidesIndex(local) { window -> client.count(relay, window)?.count }

    private suspend fun audit(
        name: String,
        filter: Filter,
        localIndex: NegentropyLocalIndex?,
        localEntries: List<IdAndTime>,
        authorsOf: suspend (List<String>) -> List<String>,
    ): Report {
        // First, the relay's count of the whole filter, waiting as long as the reconcile would. After
        // a long outage the client reconnects with backoff: a window's COUNT given up before the
        // connection is back leaves that window to our count alone, and the reconcile, which waits,
        // then sends it unsplit; on a first audit that is the whole corpus. Answered, the relay is
        // connected and counting. Unanswered, nothing is reconciled and the next audit retries,
        // sooner than waiting out a reconcile against a relay that is down.
        val relayHolds = client.count(relay, filter, idleTimeoutMs)?.count
        if (relayHolds == null) {
            log.warn("audit {}: the relay answered no COUNT; the next audit retries", name)
            return Report(name, 0, 0, -1, 0, 0, 0, 0, complete = false, note = "the relay answered no COUNT; the next audit retries")
                .also { log.info("audit {}", it) }
        }

        val haves = ArrayList<String>()
        var downloaded = 0
        var unreconciled = false
        var failure: String? = null
        var need = 0
        var windows = 0
        var ignored = 0

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
                        val notRecorded = projector.apply(events)
                        if (notRecorded.isNotEmpty()) {
                            ignored += notRecorded.size
                            log.info(
                                "audit {}: {} downloaded events not recorded, e.g. {}",
                                name,
                                notRecorded.size,
                                notRecorded.take(5).joinToString { "${it.event.id} (${it.reason})" },
                            )
                        }
                        downloaded += events.size
                    },
                )
            need = result.needCount
            windows = result.windows
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            failure = e.message ?: e.javaClass.simpleName
            log.warn("audit {}: reconcile failed after {} downloaded; the next audit resumes it", name, downloaded, e)
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
                failure?.let { "reconcile failed ($it); the next audit resumes it" },
                verdict.reason.takeIf { haves.isNotEmpty() && !verdict.act },
                recomputeFailure?.let { "recompute stopped after $recomputed ($it); the rest wait for the next audit" },
            ).joinToString("; ").ifEmpty { null }
        val report = Report(name, need, haves.size, -1, ignored, windows, downloaded, recomputed, complete = failure == null && !unreconciled && windows > 0, note = note)
        log.info("audit {}", report)
        return report
    }

    /** Pages a window negentropy can't compare into the graph. Fills only; recomputes nobody. */
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

        /**
         * The recorded reports an audit of `[since, until]` compares: any outside the relay's
         * filter would read as false haves, and their authors would be recomputed for nothing.
         */
        fun localReportEntries(
            recorded: List<Pair<String, IdAndTime>>,
            until: Long,
            since: Long? = null,
        ): List<IdAndTime> = recorded.map { it.second }.filter { it.createdAt <= until && (since == null || it.createdAt >= since) }
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
    val maxAbsent: Int,
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

/** [local]'s entries; for its count, the larger of [local]'s and [remoteCount]'s. */
class BothSidesIndex(
    private val local: NegentropyLocalIndex,
    private val remoteCount: suspend (Filter) -> Int?,
) : NegentropyLocalIndex {
    var remoteCounts = 0
        private set

    override suspend fun count(window: Filter): Int? {
        val mine = local.count(window)
        val theirs = remoteCount(window).also { remoteCounts++ }
        return if (mine == null && theirs == null) null else maxOf(mine ?: 0, theirs ?: 0)
    }

    override suspend fun entriesFor(window: Filter): List<IdAndTime> = local.entriesFor(window)
}

/** The graph's recorded lists of one kind, read one `created_at` window at a time. */
class GraphListIndex(
    private val graph: GraphStore,
    private val kind: ListKind,
) : NegentropyLocalIndex {
    override suspend fun count(window: Filter): Int? = graph.countLists(kind, window.since ?: 0, window.until ?: Long.MAX_VALUE)

    override suspend fun entriesFor(window: Filter): List<IdAndTime> = graph.lists(kind, window.since ?: 0, window.until ?: Long.MAX_VALUE)
}
