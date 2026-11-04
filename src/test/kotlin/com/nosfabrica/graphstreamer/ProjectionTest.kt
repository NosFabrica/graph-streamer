package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.ListKind
import com.nosfabrica.graphstreamer.projection.Projector
import com.nosfabrica.graphstreamer.store.IncompleteRead
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Lists and reports apply as they arrive. Anything that takes something away (a kind 5, a kind
 * 62, an expiry) reaches the graph when an audit finds the relay no longer holds the event and
 * marks its author, who is recomputed from the relay, which has already applied it.
 * The fake relay here holds what the real one would serve after doing so.
 */
class ProjectionTest {
    private val graph = FakeGraph()
    private val relay = FakeRelay()
    private var now = System.currentTimeMillis() / 1000

    private fun projector(dryRun: Boolean = false) = Projector(graph, relay, dryRun, batchSize = 100, clock = { now })

    private val alice = Signer()
    private val t0 = now - 1000

    /** The relay stores [events] and streams them to us. */
    private suspend fun arrive(vararg events: Event) {
        relay.events += events
        projector().apply(events.toList())
    }

    /** What the audit does for an author whose event the relay no longer holds. */
    private suspend fun auditFindsGone(author: String) {
        projector().recompute(listOf(author))
    }

    @Test
    fun aFollowListBecomesEdgesAndIsRecorded() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0)
            arrive(list)
            assertEquals(setOf(hex(1), hex(2)), graph.edges(alice.pubkey, Edge.FOLLOWS))
            assertEquals(list.id, graph.authors[alice.pubkey]?.heads?.get(ListKind.FOLLOWS)?.id)
            assertEquals(0, relay.reads)
        }

    @Test
    fun applyReportsWhatItDidNotRecord() =
        runTest {
            val newer = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0 + 10)
            val older = alice.event(3, listOf(listOf("p", hex(9))), createdAt = t0)
            val report = alice.event(1984, listOf(listOf("p", hex(5))), createdAt = t0)
            assertEquals(emptyList(), projector().apply(listOf(newer, report)))
            val ignored = projector().apply(listOf(older, report))
            assertEquals(listOf(older.id), ignored.map { it.event.id }, "a report already recorded is recorded, not ignored")
            assertTrue(ignored.single().reason.startsWith("the graph holds a newer list"), ignored.single().reason)
        }

    @Test
    fun aNewerListReplacesTheOldOneAndAnOlderOneChangesNothing() =
        runTest {
            arrive(alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0 + 10))
            arrive(alice.event(3, listOf(listOf("p", hex(9))), createdAt = t0))
            assertEquals(setOf(hex(1), hex(2)), graph.edges(alice.pubkey, Edge.FOLLOWS))
            arrive(alice.event(3, listOf(listOf("p", hex(2)), listOf("p", hex(3))), createdAt = t0 + 20))
            assertEquals(setOf(hex(2), hex(3)), graph.edges(alice.pubkey, Edge.FOLLOWS))
        }

    @Test
    fun aDeletedFollowListLeavesTheGraphOnRecompute() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            arrive(list)
            // The relay applies a kind 5 (drops the list); the next audit finds it gone.
            relay.events.remove(list)
            assertEquals(setOf(hex(1)), graph.edges(alice.pubkey, Edge.FOLLOWS))

            auditFindsGone(alice.pubkey)
            assertTrue(graph.edges(alice.pubkey, Edge.FOLLOWS).isEmpty())
            assertEquals(null, graph.authors[alice.pubkey]?.heads?.get(ListKind.FOLLOWS))
        }

    @Test
    fun aRetractedReportLeavesOnlyItsOwnTargets() =
        runTest {
            val report = alice.event(1984, listOf(listOf("p", hex(7), "spam")), createdAt = t0)
            val other = alice.event(1984, listOf(listOf("p", hex(8), "spam")), createdAt = t0)
            arrive(report, other)
            assertEquals(setOf(hex(7), hex(8)), graph.edges(alice.pubkey, Edge.REPORTS))

            relay.events.remove(report)
            auditFindsGone(alice.pubkey)
            assertEquals(setOf(hex(8)), graph.edges(alice.pubkey, Edge.REPORTS))
            assertEquals(setOf(other.id), graph.authors[alice.pubkey]?.reports?.keys)
        }

    @Test
    fun aVanishClearsEverythingOnRecompute() =
        runTest {
            val events =
                arrayOf(
                    alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0),
                    alice.event(10000, listOf(listOf("p", hex(2))), createdAt = t0),
                    alice.event(1984, listOf(listOf("p", hex(3))), createdAt = t0),
                    alice.event(10040, listOf(listOf("30382:rank", hex(4), "wss://nip85")), createdAt = t0),
                )
            arrive(*events)
            assertEquals(setOf(hex(4)), graph.edges(alice.pubkey, Edge.DESIGNATES))

            relay.events.removeAll(events.toSet()) // the relay applies a kind 62
            auditFindsGone(alice.pubkey)
            for (edge in Edge.entries) assertTrue(graph.edges(alice.pubkey, edge).isEmpty(), "$edge left behind")
            assertTrue(graph.authors[alice.pubkey]!!.heads.isEmpty())
            assertTrue(graph.authors[alice.pubkey]!!.reports.isEmpty())
        }

    @Test
    fun aRecomputeThatCannotFinishWritesNothing() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            arrive(list)
            relay.events.remove(list)
            relay.drains = false
            // Half an answer would read as deletions; the next audit finds the list gone again.
            assertFailsWith<IncompleteRead> { projector().recompute(listOf(alice.pubkey)) }
            assertEquals(setOf(hex(1)), graph.edges(alice.pubkey, Edge.FOLLOWS))
        }

    @Test
    fun anExpiredListIsDroppedWhenItsAuthorIsRecomputed() =
        runTest {
            val expiresAt = now + 100
            arrive(alice.event(10000, listOf(listOf("p", hex(1)), listOf("expiration", expiresAt.toString())), createdAt = t0))
            assertEquals(setOf(hex(1)), graph.edges(alice.pubkey, Edge.MUTES))

            now = expiresAt
            auditFindsGone(alice.pubkey)
            assertTrue(graph.edges(alice.pubkey, Edge.MUTES).isEmpty())
        }

    @Test
    fun reapplyingAnEventWritesNothing() =
        runTest {
            val list = alice.event(3, listOf(listOf("p", hex(1))), createdAt = t0)
            arrive(list)
            val writes = graph.writes.size
            projector().apply(listOf(list))
            assertEquals(writes, graph.writes.size)
        }

    @Test
    fun aDryRunWritesNothingButCountsTheDiff() =
        runTest {
            val dry = projector(dryRun = true)
            dry.apply(listOf(alice.event(3, listOf(listOf("p", hex(1)), listOf("p", hex(2))), createdAt = t0)))
            assertTrue(graph.writes.isEmpty())
            assertEquals(2, dry.edgesAdded.getValue(Edge.FOLLOWS).get())
        }

    @Test
    fun aDeletionOrVanishHandedToApplyChangesNothing() =
        runTest {
            projector().apply(
                listOf(
                    alice.event(5, listOf(listOf("e", hex(1)), listOf("k", "1984")), createdAt = t0),
                    alice.event(62, listOf(listOf("relay", "ALL_RELAYS")), createdAt = t0),
                ),
            )
            assertTrue(graph.writes.isEmpty(), "only an audit's recompute removes anything")
        }
}
