package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.mirror.GraphIndexing
import com.nosfabrica.graphstreamer.mirror.Mirror
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.Projector
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The store applies the deletions; the projector only reads what survives. These run the real
 * Quartz SQLite store (in memory) under the mirror and check the edges that come out.
 */
class ProjectionTest {
    private val searchRelay = RelayUrlNormalizer.normalize("wss://search.brainstorm.world")
    private val store = EventStore(dbName = null, relay = searchRelay, indexStrategy = GraphIndexing())
    private val state = FakeState()
    private val graph = FakeGraph()
    private val mirror = Mirror(store, state)
    private var now = System.currentTimeMillis() / 1000

    private fun projector(dryRun: Boolean = false) = Projector(mirror, state, graph, dryRun, batchSize = 100, clock = { now })

    private suspend fun ingestAndProject(vararg events: Event) {
        mirror.ingest(events.toList())
        while (projector().projectOnce() > 0) Unit
    }

    private val alice = Signer()
    private val t0 = now - 1000

    @Test
    fun aFollowListBecomesEdges() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0))
            assertEquals(setOf(hex(1), hex(2)), graph.of(alice.pubkey, Edge.FOLLOWS))
            assertTrue(state.dirty.isEmpty())
        }

    @Test
    fun aNewerListReplacesTheOldOne() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0))
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(2)), listOf("p", hex(3))), createdAt = t0 + 10))
            assertEquals(setOf(hex(2), hex(3)), graph.of(alice.pubkey, Edge.FOLLOWS))
        }

    @Test
    fun anOlderListArrivingLateChangesNothing() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(2))), createdAt = t0 + 10))
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0))
            assertEquals(setOf(hex(2)), graph.of(alice.pubkey, Edge.FOLLOWS))
        }

    @Test
    fun aKind5ByAddressClearsTheFollowList() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0))
            ingestAndProject(alice.event(5, listOf(listOf("a", "3:${alice.pubkey}:"), listOf("k", "3")), createdAt = t0 + 10))
            assertTrue(graph.of(alice.pubkey, Edge.FOLLOWS).isEmpty())
        }

    @Test
    fun aKind5ByIdRetractsAReport() =
        runTest {
            val report = alice.event(1984, listOf(listOf("p", hex(7), "spam")), createdAt = t0)
            val other = alice.event(1984, listOf(listOf("p", hex(8), "spam")), createdAt = t0)
            ingestAndProject(report, other)
            assertEquals(setOf(hex(7), hex(8)), graph.of(alice.pubkey, Edge.REPORTS))

            ingestAndProject(alice.event(5, listOf(listOf("e", report.id), listOf("k", "1984")), createdAt = t0 + 10))
            assertEquals(setOf(hex(8)), graph.of(alice.pubkey, Edge.REPORTS))
        }

    @Test
    fun someoneElsesKind5DeletesNothing() =
        runTest {
            val report = alice.event(1984, listOf(listOf("p", hex(7))), createdAt = t0)
            ingestAndProject(report)
            ingestAndProject(Signer().event(5, listOf(listOf("e", report.id), listOf("k", "1984")), createdAt = t0 + 10))
            assertEquals(setOf(hex(7)), graph.of(alice.pubkey, Edge.REPORTS))
        }

    @Test
    fun aVanishNamingTheSearchRelayClearsEverything() =
        runTest {
            ingestAndProject(
                alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0),
                alice.event(10000, listOf(listOf("p", hex(2))), createdAt = t0),
                alice.event(1984, listOf(listOf("p", hex(3))), createdAt = t0),
                alice.event(10040, listOf(listOf("30382:rank", hex(4), "wss://nip85")), createdAt = t0),
            )
            assertEquals(setOf(hex(4)), graph.of(alice.pubkey, Edge.DESIGNATES))

            ingestAndProject(alice.event(62, listOf(listOf("relay", searchRelay.url)), createdAt = t0 + 10))
            for (edge in Edge.entries) assertTrue(graph.of(alice.pubkey, edge).isEmpty(), "$edge left behind")
        }

    @Test
    fun aVanishFromEverywhereClearsEverything() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0))
            ingestAndProject(alice.event(62, listOf(listOf("relay", "ALL_RELAYS")), createdAt = t0 + 10))
            assertTrue(graph.of(alice.pubkey, Edge.FOLLOWS).isEmpty())
        }

    @Test
    fun aVanishNamingAnotherRelayDoesNothing() =
        runTest {
            ingestAndProject(alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0))
            ingestAndProject(alice.event(62, listOf(listOf("relay", "wss://relay.damus.io/")), createdAt = t0 + 10))
            assertEquals(setOf(hex(1)), graph.of(alice.pubkey, Edge.FOLLOWS))
        }

    @Test
    fun anExpiredListIsDroppedWhenItsAuthorIsProjectedAgain() =
        runTest {
            val expiresAt = now + 100
            ingestAndProject(alice.event(10000, listOf(listOf("p", hex(1)), listOf("expiration", expiresAt.toString())), createdAt = t0))
            assertEquals(setOf(hex(1)), graph.of(alice.pubkey, Edge.MUTES))

            now = expiresAt
            state.markDirty(listOf(alice.pubkey))
            while (projector().projectOnce() > 0) Unit
            assertTrue(graph.of(alice.pubkey, Edge.MUTES).isEmpty())
        }

    @Test
    fun whatTheAuditRemovesLeavesTheGraph() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            ingestAndProject(list)
            mirror.remove(listOf(list.id))
            while (projector().projectOnce() > 0) Unit
            assertTrue(graph.of(alice.pubkey, Edge.FOLLOWS).isEmpty())
        }

    @Test
    fun aDuplicateMarksNobody() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            ingestAndProject(list)
            mirror.ingest(listOf(list))
            assertTrue(state.dirty.isEmpty())
            assertEquals(1, mirror.duplicates.get())
        }

    @Test
    fun aDryRunWritesNothingButCountsTheDiff() =
        runTest {
            mirror.ingest(listOf(alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0)))
            val dry = projector(dryRun = true)
            while (dry.projectOnce() > 0) Unit
            assertTrue(graph.applied.isEmpty())
            assertEquals(2, dry.edgesAdded.getValue(Edge.FOLLOWS).get())
            assertTrue(state.dirty.isEmpty())
        }

    @Test
    fun aWriteDuringProjectionKeepsTheAuthorDirty() =
        runTest {
            mirror.ingest(listOf(alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)))
            val read = state.dirty(10)
            state.markDirty(listOf(alice.pubkey)) // a write landing after the projector read
            assertEquals(0, state.clearDirty(read))
            assertTrue(alice.pubkey in state.dirty)
        }

    @Test
    fun theMirrorKeepsOnlyWhatItsFiltersAskFor() =
        runTest {
            mirror.ingest(
                listOf(
                    alice.event(1, content = "a note", createdAt = t0),
                    alice.event(5, listOf(listOf("e", hex(1)), listOf("k", "1")), createdAt = t0),
                    alice.event(5, listOf(listOf("e", hex(2)), listOf("k", "1984")), createdAt = t0),
                ),
            )
            assertEquals(1, mirror.accepted.get())
        }
}
