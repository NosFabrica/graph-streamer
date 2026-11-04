package com.nosfabrica.graphstreamer.e2e

import com.nosfabrica.graphstreamer.Signer
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.publishAndConfirm
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInfo
import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The installed service, end to end, against a relay we control: every way an edge enters or
 * leaves the graph, and every failure we could name, each checked by tools/verify (the server's
 * parsers, not ours) against what the relay then holds. See E2E.md to run it.
 */
class EndToEndTest {
    private lateinit var stores: Stores
    private lateinit var relay: LocalRelay
    private lateinit var streamer: Streamer
    private lateinit var dir: File
    private val now = System.currentTimeMillis() / 1000
    private val rnd = Random(42)

    @BeforeEach
    fun setUp(info: TestInfo) {
        assumeTrue(E2E.enabled, "GS_E2E=1 runs the end-to-end suite")
        stores = Stores().also { it.wipe() }
        dir = Files.createTempDirectory("gs-e2e").toFile()
        val port = ServerSocket(0).use { it.localPort }
        relay = LocalRelay(File(dir, "relay.db"), port).also { it.start() }
        val logs = File("build/e2e-logs").also { it.mkdirs() }
        val log = File(logs, "${info.testMethod.get().name}.log").also { it.delete() }
        streamer = Streamer(relay.url, log, mapOf("AUDIT_RETRY_MINUTES" to "1"))
    }

    @AfterEach
    fun tearDown() {
        if (!E2E.enabled) return
        streamer.stop()
        relay.close()
        stores.close()
        dir.deleteRecursively()
    }

    // ---- the corpus -----------------------------------------------------------------------

    private val targets = List(3_000) { Signer().pubkey }

    private fun pTags(n: Int) = targets.shuffled(rnd).take(n).map { listOf("p", it) }

    private fun past(maxDaysAgo: Int = 30) = now - 7_200 - rnd.nextLong(maxDaysAgo * 86_400L)

    /** Authors with every shape of list and report the parsers have a rule for. */
    private inner class Corpus(
        authors: Int = 200,
    ) {
        val signers = List(authors) { Signer() }
        val events = ArrayList<Event>()

        init {
            signers.forEachIndexed { i, s ->
                val at = past()
                when (i) {
                    0 -> events += s.event(3, pTags(5_000), at) // a large list
                    1 -> events += s.event(3, listOf(listOf("p", targets[0].uppercase()), listOf("p", "abc"), listOf("p", targets[1]), listOf("p", targets[1]), listOf("e", targets[2])), at)
                    2 -> events += s.event(3, emptyList(), at) // an empty list: recorded, no edges
                    3 -> {
                        events += s.event(3, pTags(20), at - 1_000) // replaced by the next one
                        events += s.event(3, pTags(30), at)
                    }
                    4 -> events += s.event(3, pTags(10) + listOf(listOf("expiration", (now - 60).toString())), at) // expired
                    else -> events += s.event(3, pTags(rnd.nextInt(0, 300)), at)
                }
                if (i % 3 == 0) events += s.event(10000, pTags(rnd.nextInt(1, 40)), past())
                if (i % 7 == 0) {
                    val rows = targets.shuffled(rnd).take(3).map { listOf("30382:rank", it) } + listOf(listOf("30382:rank", s.pubkey), listOf("not-a-kind", targets[5]))
                    events += s.event(10040, rows, past())
                }
                if (i % 4 == 0) {
                    events += s.event(1984, listOf(listOf("p", targets.random(rnd), "spam")), past())
                    events += s.event(1984, listOf(listOf("p", targets.random(rnd), "impersonation")), past())
                    events += s.event(1984, listOf(listOf("e", targets.random(rnd), "spam"), listOf("p", targets.random(rnd))), past()) // a note report: no edge
                }
            }
        }
    }

    /** [authors] authors with [follows] each: enough for an audit that lasts while we break things. */
    private fun bigCorpus(
        authors: Int,
        follows: Int,
    ): List<Event> = List(authors) { Signer().event(3, pTags(follows), past()) }

    private fun publish(vararg events: Event) {
        val http = OkHttpClient.Builder().build()
        val client = NostrClient(BasicOkHttpWebSocket.Builder { http })
        try {
            runBlocking {
                val url = setOf(RelayUrlNormalizer.normalize(relay.url))
                for (e in events) assertTrue(client.publishAndConfirm(e, url, 15), "the relay refused ${e.kind}")
            }
        } finally {
            client.close()
        }
    }

    private fun rebuild(
        corpus: List<Event>,
        env: Map<String, String> = emptyMap(),
    ) {
        relay.load(corpus)
        val started = now
        streamer.start(env)
        val state = stores.awaitAudit(started, streamer = streamer)
        assertEquals("true", state["audit_complete"], "first audit incomplete: $state")
    }

    private fun audit(env: Map<String, String> = emptyMap()): Map<String, Any?> {
        streamer.stop()
        stores.dueNow()
        val started = System.currentTimeMillis() / 1000
        streamer.start(env)
        return stores.awaitAudit(started, streamer = streamer)
    }

    // ---- scenarios -----------------------------------------------------------------------

    @Test
    fun aRebuildMatchesTheRelay() {
        rebuild(Corpus().events)
        assertOracle(relay)
        val state = stores.state()
        assertEquals("0", state["audit_follows_have"])
    }

    @Test
    fun liveEventsArriveWithoutAnAudit() {
        val corpus = Corpus()
        rebuild(corpus.events)
        val a = corpus.signers[10]
        val b = corpus.signers[11]
        val list = a.event(3, pTags(7))
        val mutes = b.event(10000, pTags(3))
        val report = b.event(1984, listOf(listOf("p", targets[9])))
        val designation = a.event(10040, listOf(listOf("30382:rank", targets[8])))
        publish(list, mutes, report, designation)
        waitFor(30, "the live events in the graph") {
            stores.edges(a.pubkey, "FOLLOWS") == list.tags.map { it[1] }.toSet() &&
                stores.edges(b.pubkey, "MUTES") == mutes.tags.map { it[1] }.toSet() &&
                targets[9] in stores.edges(b.pubkey, "REPORTS") &&
                stores.edges(a.pubkey, "DESIGNATES") == setOf(targets[8])
        }
        assertOracle(relay, grace = 0)
    }

    @Test
    fun aRestartResumesTheLiveStream() {
        val corpus = Corpus()
        rebuild(corpus.events)
        Thread.sleep(2_500) // past one resume-point save
        // Nothing streamed yet (the corpus went straight into the relay's store): the point to
        // resume from is still where the stream started.
        assertTrue(stores.state()["live_since"] != null, "no resume point saved: ${stores.state()}")
        streamer.stop()
        val a = corpus.signers[12]
        val list = a.event(3, pTags(4))
        publish(list)
        streamer.start() // not due: no audit, only the live stream from its saved point
        waitFor(30, "a list published while the streamer was down") { stores.edges(a.pubkey, "FOLLOWS") == list.tags.map { it[1] }.toSet() }
        assertOracle(relay, grace = 0)
    }

    @Test
    fun everyDeletionReachesTheGraphAtTheNextAudit() {
        val corpus = Corpus()
        val expiring = Signer()
        val expiringList = expiring.event(3, pTags(5) + listOf(listOf("expiration", (now + 20).toString())), past())
        rebuild(corpus.events + expiringList)
        val (withK, withoutK, byAddress, vanished, reporter) = listOf(20, 21, 22, 23, 24).map { corpus.signers[it] }
        val list = { s: Signer -> corpus.events.single { it.pubKey == s.pubkey && it.kind == 3 } }
        assertTrue(stores.edges(withK.pubkey, "FOLLOWS").isNotEmpty())
        val report = corpus.events.first { it.pubKey == reporter.pubkey && it.kind == 1984 && it.tags.none { t -> t[0] == "e" } }
        assertTrue(report.tags.first()[1] in stores.edges(reporter.pubkey, "REPORTS"))
        assertEquals(5, stores.edges(expiring.pubkey, "FOLLOWS").size)

        publish(
            withK.event(5, listOf(listOf("e", list(withK).id), listOf("k", "3"))),
            withoutK.event(5, listOf(listOf("e", list(withoutK).id))),
            byAddress.event(5, listOf(listOf("a", "3:${byAddress.pubkey}:"), listOf("k", "3"))),
            vanished.event(62, listOf(listOf("relay", "ALL_RELAYS"))),
            reporter.event(5, listOf(listOf("e", report.id), listOf("k", "1984"))),
        )
        waitFor(40, "the list to expire") { System.currentTimeMillis() / 1000 > now + 21 }
        relay.deleteExpired()

        val state = audit()
        assertEquals("true", state["audit_complete"], "$state")
        for (s in listOf(withK, withoutK, byAddress, vanished, expiring)) assertEquals(emptySet(), stores.edges(s.pubkey, "FOLLOWS"), "follows of ${s.pubkey}")
        assertTrue(report.tags.first()[1] !in stores.edges(reporter.pubkey, "REPORTS"), "the deleted report's edge")
        assertOracle(relay)
    }

    @Test
    fun aMassAbsenceIsKeptUntilAllowed() {
        val corpus = Corpus()
        rebuild(corpus.events)
        val gone = corpus.signers.subList(30, 55)
        publish(*gone.map { s -> s.event(5, listOf(listOf("e", corpus.events.single { it.pubKey == s.pubkey && it.kind == 3 }.id), listOf("k", "3"))) }.toTypedArray())
        val before = gone.associate { it.pubkey to stores.edges(it.pubkey, "FOLLOWS") }

        val guarded = audit(mapOf("AUDIT_MAX_DELETES" to "10"))
        assertTrue((guarded["audit_follows_note"] as String).contains("exceed AUDIT_MAX_DELETES=10"), "$guarded")
        assertEquals(before, gone.associate { it.pubkey to stores.edges(it.pubkey, "FOLLOWS") }, "nothing removed past the guard")
        assertNotEquals(0, oracle(relay).first, "the graph still holds the lists the relay lost")

        audit(mapOf("AUDIT_MAX_DELETES" to "0"))
        assertOracle(relay)
    }

    @Test
    fun aRelayOutageMidReconcileIsRetriedNotPaged() {
        val corpus = bigCorpus(15_000, 60)
        relay.load(corpus)
        val started = now
        streamer.start(mapOf("AUDIT_WINDOW" to "500"))
        waitFor(120, "the first audit to start writing") { stores.count("MATCH ()-[r:FOLLOWS]->() RETURN count(r)") > 50_000 }
        relay.stop()
        Thread.sleep(8_000)
        relay.start()
        val failed = stores.awaitAudit(started, streamer = streamer)
        assertEquals("false", failed["audit_follows_complete"], "$failed")
        assertTrue((failed["audit_follows_note"] as String).contains("the next audit resumes it"), "$failed")
        assertTrue((failed["audit_follows_downloaded"] as String).toInt() < corpus.size, "no paging of the whole filter: $failed")
        // AUDIT_RETRY_MINUTES=1: the retry comes by itself.
        val retried = stores.awaitAudit((failed["audit_ended_at"] as String).toLong() + 1, streamer = streamer)
        assertEquals("true", retried["audit_complete"], "$retried")
        assertOracle(relay)
    }

    @Test
    fun killedMidWriteItRecovers() {
        relay.load(bigCorpus(15_000, 60))
        var edges = 0L
        repeat(4) { round ->
            streamer.start(mapOf("AUDIT_WINDOW" to "2000"))
            val target = edges + 60_000 + rnd.nextLong(60_000)
            waitFor(180, "round $round to write more edges") { stores.count("MATCH ()-[r:FOLLOWS]->() RETURN count(r)").also { edges = it } > target }
            streamer.kill9()
        }
        val started = System.currentTimeMillis() / 1000
        streamer.start()
        val state = stores.awaitAudit(started, streamer = streamer)
        assertEquals("true", state["audit_complete"], "$state")
        assertOracle(relay)
    }

    @Test
    fun aNeo4jRestartMidAuditIsSurvived() {
        relay.load(bigCorpus(15_000, 60))
        val started = now
        streamer.start(mapOf("AUDIT_WINDOW" to "2000"))
        waitFor(120, "the first audit to start writing") { stores.count("MATCH ()-[r:FOLLOWS]->() RETURN count(r)") > 100_000 }
        docker("restart", E2E.neo4jContainer)
        waitFor(120, "Neo4j back") { stores.count("RETURN 1") == 1L }
        waitFor(300, "a complete audit after the restart") {
            if (!streamer.alive) error("the streamer died:\n" + streamer.logText().takeLast(3000))
            stores.state()["audit_complete"] == "true" && (stores.state()["audit_ended_at"] as String).toLong() >= started
        }
        assertOracle(relay)
    }

    @Test
    fun aLostRedisIsRebuilt() {
        rebuild(Corpus().events)
        streamer.stop()
        docker("restart", E2E.redisContainer) // no persistence: every set is gone, as after an eviction storm
        waitFor(60, "Redis back") { stores.redis.resource.use { it.ping() == "PONG" } }
        audit()
        assertOracle(relay)
    }

    @Test
    fun aDriftedRedisIsReconciled() {
        rebuild(Corpus().events)
        streamer.stop()
        stores.redis.resource.use { r ->
            val keys = r.keys("followed_by:*").shuffled(rnd)
            keys.take(20).forEach { r.del(it) } // evicted
            keys.drop(20).take(20).forEach { r.sadd(it, Signer().pubkey) } // a member Neo4j lacks
            keys.drop(40).take(20).forEach { k -> r.spop(k) } // a member lost
            r.sadd("followed_by:" + Signer().pubkey, Signer().pubkey) // a set with no edge at all
            r.sadd("designates:" + Signer().pubkey, Signer().pubkey)
        }
        assertNotEquals(0, oracle(relay).first, "the damage is visible")
        stores.driver.session().use { it.run("MATCH (s:GraphStreamerState) REMOVE s.redis_reconciled_at").consume() }
        val started = System.currentTimeMillis() / 1000
        streamer.start()
        waitFor(120, "a Redis reconcile") { (stores.state()["redis_reconciled_at"] as? String)?.toLong()?.let { it >= started } ?: false }
        val state = stores.state()
        assertTrue((state["redis_replaced"] as String).toLong() >= 59, "$state")
        assertTrue((state["redis_deleted"] as String).toLong() >= 2, "$state")
        assertOracle(relay)
    }

    /**
     * Production's graph today: built by the server's consumer from the firehose, in arrival
     * order. Older list versions arriving after newer ones (it applies whatever comes last),
     * lists and reports the relay no longer holds or never had, list deletions it never applied.
     * graph-streamer then takes over that Neo4j and Redis; afterwards the graph must be exactly
     * the relay's, with no edge left that no audit compares.
     */
    @Test
    fun aServerBuiltGraphMigrates() {
        assumeTrue(E2E.serverPython != null, "GS_E2E_SERVER_PYTHON runs the migration scenario")
        val arrivals = ArrayList<Event>()
        val onRelay = ArrayList<Event>()
        val authors = List(150) { Signer() }
        authors.forEachIndexed { i, s ->
            val old = s.event(3, pTags(rnd.nextInt(5, 80)), past(60) - 86_400 * 60)
            val new = s.event(3, pTags(rnd.nextInt(5, 80)), past())
            when (i % 6) {
                0 -> { arrivals += new; arrivals += old; onRelay += new } // the old one arrived last: the server holds it
                1 -> { arrivals += old; arrivals += new; onRelay += new }
                2 -> { arrivals += new } // the relay never had it: an orphan
                3 -> { arrivals += new; onRelay += new; onRelay += s.event(5, listOf(listOf("e", new.id), listOf("k", "3")), now - 3_600 * 3) } // deleted on the relay
                else -> { arrivals += new; onRelay += new }
            }
            if (i % 3 == 0) {
                val mutes = s.event(10000, pTags(rnd.nextInt(1, 20)), past())
                arrivals += mutes
                if (i % 2 == 0) onRelay += mutes // half of them gone from the relay
            }
            if (i % 5 == 0) {
                val kept = s.event(1984, listOf(listOf("p", targets.random(rnd))), past())
                val gone = s.event(1984, listOf(listOf("p", targets.random(rnd))), past())
                arrivals += kept
                arrivals += gone // the relay never had this one
                onRelay += kept
            }
            if (i % 10 == 0) {
                val d = s.event(10040, listOf(listOf("30382:rank", targets.random(rnd))), past())
                arrivals += d
                if (i % 20 == 0) onRelay += d
            }
        }
        relay.load(onRelay)
        serverBuild(arrivals, relay, dir)
        assertTrue(stores.count("MATCH ()-[r:FOLLOWS]->() RETURN count(r)") > 0, "the server built a graph")
        assertNotEquals(0, oracle(relay).first, "the server-built graph differs from the relay")

        val started = System.currentTimeMillis() / 1000
        streamer.start()
        val first = stores.awaitAudit(started, streamer = streamer)
        assertEquals("true", first["audit_complete"], "$first")
        // The second audit finds no orphan left, and marks every kind adopted.
        val second = audit()
        assertEquals("true", second["audit_complete"], "$second")
        for (kind in listOf("follows", "mutes", "designations", "reports")) assertEquals("true", second["adopted_$kind"], "$kind: $second")
        waitFor(120, "the Redis reconcile") { (stores.state()["redis_reconciled_at"] as? String)?.toLong()?.let { it >= started } ?: false }
        assertOracle(relay)
    }

    @Test
    fun aDryRunWritesNothing() {
        relay.load(Corpus().events)
        streamer.start(mapOf("DRY_RUN" to "true"))
        val state = stores.awaitAudit(now, streamer = streamer)
        assertEquals("true", state["audit_complete"], "$state")
        assertEquals(1L, stores.count("MATCH (n) RETURN count(n)"), "only the state node")
        assertEquals(0L, stores.count("MATCH ()-[r]->() RETURN count(r)"))
        assertEquals(0L, stores.redis.resource.use { it.dbSize() })
        assertTrue((state["audit_follows_need"] as String).toInt() > 150, "$state")
    }
}
