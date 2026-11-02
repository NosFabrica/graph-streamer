package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.mirror.GraphIndexing
import com.nosfabrica.graphstreamer.mirror.Mirror
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * An audit diffs the relay's answer to [Mirrored.filters] against the store's own answer to the
 * same filters. If the store read `include:spam` as a text search (and matched nothing), every
 * event the relay has would look missing here and be downloaded again on every audit.
 */
class AuditFiltersTest {
    private val store = EventStore(dbName = null, relay = RelayUrlNormalizer.normalize("wss://search.brainstorm.world"), indexStrategy = GraphIndexing())
    private val mirror = Mirror(store, FakeState())
    private val alice = Signer()
    private val t0 = System.currentTimeMillis() / 1000 - 1000

    @Test
    fun theStoreAnswersEachMirroredFilterWithWhatItHolds() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            val report = alice.event(1984, listOf(listOf("p", hex(2))), createdAt = t0)
            val deletion = alice.event(5, listOf(listOf("e", hex(3)), listOf("k", "1984")), createdAt = t0)
            val vanish = alice.event(62, listOf(listOf("relay", "wss://elsewhere.example/")), createdAt = t0)
            mirror.ingest(listOf(list, report, deletion, vanish))

            val answers = Mirrored.filters().map { f -> store.snapshotIdsForNegentropy(listOf(f)).map { it.id }.toSet() }
            assertEquals(listOf(setOf(list.id), setOf(report.id), setOf(deletion.id), setOf(vanish.id)), answers)
        }

    @Test
    fun theAuditWindowStopsAtUntil() =
        runTest {
            val old = alice.event(1984, listOf(listOf("p", hex(1))), createdAt = t0)
            val fresh = alice.event(1984, listOf(listOf("p", hex(2))), createdAt = t0 + 500)
            mirror.ingest(listOf(old, fresh))
            val reports = Mirrored.filters(until = t0 + 100)[1]
            assertEquals(setOf(old.id), store.snapshotIdsForNegentropy(listOf(reports)).map { it.id }.toSet())
        }
}
