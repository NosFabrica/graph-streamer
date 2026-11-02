package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.Edge
import com.nosfabrica.graphstreamer.projection.GraphRules
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
    fun theNewestListWinsAndReportsAccumulate() {
        val old = alice.event(3, listOf(listOf("p", hex(1))), createdAt = 1000)
        val new = alice.event(3, listOf(listOf("p", hex(2))), createdAt = 2000)
        val r1 = alice.event(1984, listOf(listOf("p", hex(3))))
        val r2 = alice.event(1984, listOf(listOf("p", hex(4))))
        val desired = GraphRules.desired(alice.pubkey, listOf(old, new, r1, r2), now = 3000)
        assertEquals(setOf(hex(2)), desired[Edge.FOLLOWS])
        assertEquals(setOf(hex(3), hex(4)), desired[Edge.REPORTS])
        assertEquals(emptySet(), desired[Edge.MUTES])
    }

    @Test
    fun anExpiredEventCountsForNothing() {
        val list = alice.event(10000, listOf(listOf("p", hex(1)), listOf("expiration", "2500")), createdAt = 1000)
        assertEquals(setOf(hex(1)), GraphRules.desired(alice.pubkey, listOf(list), now = 2499)[Edge.MUTES])
        assertTrue(GraphRules.desired(alice.pubkey, listOf(list), now = 2500)[Edge.MUTES]!!.isEmpty())
    }

    @Test
    fun someoneElsesEventsDoNotCount() {
        val bob = Signer()
        val list = bob.event(3, listOf(listOf("p", hex(1))))
        assertTrue(GraphRules.desired(alice.pubkey, listOf(list), now = Long.MAX_VALUE)[Edge.FOLLOWS]!!.isEmpty())
    }
}
