package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.AuthorState
import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.GraphRules
import com.nosfabrica.graphstreamer.projection.ListHead
import com.nosfabrica.graphstreamer.projection.ListKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GraphRulesTest {
    private val alice = Signer()

    @Test
    fun pTagsAreLowercasedDedupedAndValidated() {
        val tags =
            arrayOf(
                arrayOf("p", hex(1).uppercase()),
                arrayOf("p", hex(1)),
                arrayOf("p", "npub1notahexkey"),
                arrayOf("p"),
                arrayOf("e", hex(2)),
                arrayOf("p", hex(3), "wss://relay"),
            )
        assertEquals(listOf(hex(1), hex(3)), GraphRules.pTagPubkeys(tags))
    }

    @Test
    fun noteReportsAreNotUserReports() {
        val user = alice.event(1984, listOf(listOf("p", hex(1), "spam")))
        val note = alice.event(1984, listOf(listOf("e", hex(9), "spam"), listOf("p", hex(1))))
        assertEquals(listOf(hex(1)), GraphRules.reportTargets(user))
        assertEquals(emptyList(), GraphRules.reportTargets(note))
    }

    @Test
    fun designationsKeepPublicRowsAndNeverTheObserver() {
        val tags =
            arrayOf(
                arrayOf("30382:rank", hex(1), "wss://nip85"),
                arrayOf("30392", hex(2)),
                arrayOf("relay", hex(3)),
                arrayOf("30382:rank", alice.pubkey),
                arrayOf("30382:rank", hex(1)),
            )
        assertEquals(listOf(hex(1), hex(2)), GraphRules.designatedPubkeys(tags, alice.pubkey))
    }

    @Test
    fun aListReplacesOnlyAnOlderOne() {
        val held = AuthorState(mapOf(Edge.FOLLOWS to setOf(hex(1))), mapOf(ListKind.FOLLOWS to ListHead("ff".repeat(32), 2000)))
        val older = alice.event(3, listOf(listOf("p", hex(2))), createdAt = 1000)
        val newer = alice.event(3, listOf(listOf("p", hex(3))), createdAt = 3000)
        assertEquals(held, GraphRules.apply(alice.pubkey, held, listOf(older), now = 4000))
        val after = GraphRules.apply(alice.pubkey, held, listOf(newer), now = 4000)
        assertEquals(setOf(hex(3)), after.edges(Edge.FOLLOWS))
        assertEquals(ListHead(newer.id, 3000), after.heads[ListKind.FOLLOWS])
    }

    @Test
    fun onTheSameSecondTheLowerIdWins() {
        val a = alice.event(3, listOf(listOf("p", hex(1))), createdAt = 1000)
        val b = alice.event(3, listOf(listOf("p", hex(2))), createdAt = 1000)
        val (low, high) = if (a.id < b.id) a to b else b to a
        val viaHigh = GraphRules.apply(alice.pubkey, GraphRules.fromRelay(alice.pubkey, listOf(high), 2000), listOf(low), 2000)
        val viaLow = GraphRules.apply(alice.pubkey, GraphRules.fromRelay(alice.pubkey, listOf(low), 2000), listOf(high), 2000)
        assertEquals(low.id, viaHigh.heads[ListKind.FOLLOWS]?.id)
        assertEquals(low.id, viaLow.heads[ListKind.FOLLOWS]?.id)
    }

    @Test
    fun aListWithNoValidTargetsIsStillRecorded() {
        val empty = alice.event(3, listOf(listOf("p", "not-hex")), createdAt = 1000)
        val state = GraphRules.fromRelay(alice.pubkey, listOf(empty), now = 2000)
        assertTrue(state.edges(Edge.FOLLOWS).isEmpty())
        assertEquals(empty.id, state.heads[ListKind.FOLLOWS]?.id)
    }

    @Test
    fun reportsAccumulateAndNoteReportsAreRecordedWithoutAnEdge() {
        val r1 = alice.event(1984, listOf(listOf("p", hex(3))), createdAt = 1000)
        val r2 = alice.event(1984, listOf(listOf("p", hex(4))), createdAt = 1001)
        val note = alice.event(1984, listOf(listOf("e", hex(9)), listOf("p", hex(5))), createdAt = 1002)
        val state = GraphRules.apply(alice.pubkey, GraphRules.fromRelay(alice.pubkey, listOf(r1), 2000), listOf(r2, note), 2000)
        assertEquals(setOf(hex(3), hex(4)), state.edges(Edge.REPORTS))
        assertEquals(setOf(r1.id, r2.id, note.id), state.reports.keys)
    }

    @Test
    fun anExpiredEventCountsForNothing() {
        val list = alice.event(10000, listOf(listOf("p", hex(1)), listOf("expiration", "2500")), createdAt = 1000)
        assertEquals(setOf(hex(1)), GraphRules.fromRelay(alice.pubkey, listOf(list), now = 2499).edges(Edge.MUTES))
        assertTrue(GraphRules.fromRelay(alice.pubkey, listOf(list), now = 2500).edges(Edge.MUTES).isEmpty())
    }

    @Test
    fun someoneElsesEventsDoNotCount() {
        val list = Signer().event(3, listOf(listOf("p", hex(1))))
        assertEquals(AuthorState(), GraphRules.fromRelay(alice.pubkey, listOf(list), now = Long.MAX_VALUE))
    }
}
