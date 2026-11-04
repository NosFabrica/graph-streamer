package com.nosfabrica.graphstreamer.e2e

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.server.NostrServer
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import redis.clients.jedis.JedisPool
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.fail

/** The suite's settings, from the environment (E2E.md). */
object E2E {
    val enabled = System.getenv("GS_E2E") == "1"
    val neo4jUrl = System.getenv("GS_E2E_NEO4J") ?: "bolt://127.0.0.1:7688"
    const val NEO4J_USER = "neo4j"
    val neo4jPassword = System.getenv("GS_E2E_NEO4J_PASSWORD") ?: "testpassword123"
    val neo4jContainer = System.getenv("GS_E2E_NEO4J_CONTAINER") ?: "neo4j-e2e"
    val redisPort = (System.getenv("GS_E2E_REDIS_PORT") ?: "6380").toInt()
    val redisContainer = System.getenv("GS_E2E_REDIS_CONTAINER") ?: "redis-e2e"
    val python = System.getenv("GS_E2E_PYTHON") ?: "python3"
    val serverDir = System.getenv("BRAINSTORM_SERVER_DIR") ?: "../brainstorm_server"

    /** A Python with brainstorm_server's dependencies, for its consumer (the migration scenario). */
    val serverPython: String? = System.getenv("GS_E2E_SERVER_PYTHON")
    val serverBuild: String = System.getProperty("graphstreamer.serverBuild") ?: "tools/rehearsal/server_build.py"
    val bin: String = System.getProperty("graphstreamer.bin") ?: "build/install/graph-streamer/bin/graph-streamer"
    val verify: String = System.getProperty("graphstreamer.verify") ?: "tools/verify/verify_graph.py"
}

/**
 * A relay that speaks NIP-01, NIP-09, NIP-40, NIP-62 and NIP-77: Quartz's NostrServer over its
 * SQLite store in [dbFile], so its events outlive [stop]/[start] (a relay restart that drops every
 * connection). It drops the `include:spam` search token from filters, as the search relay treats
 * it as "no lens" while this store would read it as a full-text query.
 */
class LocalRelay(
    dbFile: File,
    val port: Int,
) : AutoCloseable {
    val store = EventStore(dbName = dbFile.absolutePath)
    private val server = NostrServer(store)
    private var engine: EmbeddedServer<*, *>? = null
    val url = "ws://127.0.0.1:$port/"

    fun start() {
        engine =
            embeddedServer(CIO, port = port, host = "127.0.0.1") {
                install(WebSockets) { maxFrameSize = Long.MAX_VALUE }
                routing {
                    webSocket("/") {
                        // One writer, so frames leave in the order the engine made them.
                        val out = Channel<String>(Channel.UNLIMITED)
                        launch { for (m in out) send(Frame.Text(m)) }
                        server.serve(send = { json -> out.trySend(json) }) { session ->
                            for (frame in incoming) if (frame is Frame.Text) session.receive(stripLens(frame.readText()))
                        }
                        out.close()
                    }
                }
            }.start(wait = false)
    }

    /** Drops every connection; the events stay. */
    fun stop() {
        engine?.stop(0, 500, TimeUnit.MILLISECONDS)
        engine = null
    }

    /** Straight into the store: deletions, vanishes and replacements apply as on a publish; no live delivery. */
    fun load(events: List<Event>) = runBlocking { for (chunk in events.chunked(2_000)) store.batchInsert(chunk) }

    fun deleteExpired() = runBlocking { store.deleteExpiredEvents() }

    override fun close() {
        stop()
        server.close()
        store.close()
    }

    companion object {
        private val LENS = Regex(""""search"\s*:\s*"include:spam"\s*,?""")
        private val TRAILING = Regex(""",\s*}""")

        fun stripLens(frame: String): String {
            if (!(frame.startsWith("[\"REQ\"") || frame.startsWith("[\"NEG-OPEN\"") || frame.startsWith("[\"COUNT\""))) return frame
            return frame.replace(LENS, "").replace(TRAILING, "}")
        }
    }
}

/** The installed service as its own process, so it can be killed like a pod. */
class Streamer(
    private val relayUrl: String,
    private val log: File,
    private val extraEnv: Map<String, String> = emptyMap(),
) {
    private var process: Process? = null

    fun start(env: Map<String, String> = emptyMap()): Streamer {
        val pb = ProcessBuilder(E2E.bin).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(log))
        pb.environment().putAll(
            mapOf(
                "NEO4J_URL" to E2E.neo4jUrl,
                "NEO4J_USERNAME" to E2E.NEO4J_USER,
                "NEO4J_PASSWORD" to E2E.neo4jPassword,
                "REDIS_HOST" to "127.0.0.1",
                "REDIS_PORT" to E2E.redisPort.toString(),
                "SEARCH_RELAY_URL" to relayUrl,
                "DRY_RUN" to "false",
                "LIVE_RESUME_POINT_SECONDS" to "1",
                "JAVA_OPTS" to "-Xmx512m",
            ) + extraEnv + env,
        )
        process = pb.start()
        return this
    }

    val alive get() = process?.isAlive == true

    /** SIGTERM, and wait. */
    fun stop() {
        process?.let {
            it.destroy()
            if (!it.waitFor(30, TimeUnit.SECONDS)) it.destroyForcibly().waitFor()
        }
        process = null
    }

    /** SIGKILL: no shutdown hook, no finally, mid-write. */
    fun kill9() {
        process?.let {
            it.destroyForcibly()
            it.waitFor()
        }
        process = null
    }

    fun logText(): String = log.readText()
}

/** The graph's stores, and what the tests read and reset in them. */
class Stores : AutoCloseable {
    val driver: Driver = GraphDatabase.driver(E2E.neo4jUrl, AuthTokens.basic(E2E.NEO4J_USER, E2E.neo4jPassword))
    val redis = JedisPool("127.0.0.1", E2E.redisPort)

    fun wipe() {
        driver.session().use { s ->
            // Edges first, in small transactions: a node's DETACH DELETE takes all its edges in one.
            s.run("MATCH ()-[r]->() CALL (r) { DELETE r } IN TRANSACTIONS OF 20000 ROWS").consume()
            s.run("MATCH (n) CALL (n) { DELETE n } IN TRANSACTIONS OF 20000 ROWS").consume()
            s.run("SHOW CONSTRAINTS YIELD name RETURN name").list().forEach { s.run("DROP CONSTRAINT ${it["name"].asString()}").consume() }
            s.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name").list().forEach { s.run("DROP INDEX ${it["name"].asString()} IF EXISTS").consume() }
        }
        redis.resource.use { it.flushAll() }
    }

    fun state(): Map<String, Any?> =
        driver.session().use { s ->
            s.run("MATCH (s:GraphStreamerState) RETURN properties(s) AS p").list().firstOrNull()?.get("p")?.asMap() ?: emptyMap()
        }

    /** Makes the next start run an audit at once: the schedule is read from these two. */
    fun dueNow() {
        driver.session().use { it.run("MATCH (s:GraphStreamerState) REMOVE s.audit_finished_at, s.audit_ended_at").consume() }
    }

    fun edges(
        author: String,
        rel: String,
    ): Set<String> =
        driver.session().use { s ->
            s.run("MATCH (:NostrUser {pubkey: \$a})-[:$rel]->(t:NostrUser) RETURN t.pubkey AS t", mapOf("a" to author)).list { it["t"].asString() }.toSet()
        }

    fun count(cypher: String): Long = driver.session().use { it.run(cypher).single()[0].asLong() }

    override fun close() {
        driver.close()
        redis.close()
    }
}

/** Polls [condition] until true or [seconds] pass. */
fun waitFor(
    seconds: Long,
    what: String,
    condition: () -> Boolean,
) {
    val deadline = System.currentTimeMillis() + seconds * 1000
    while (System.currentTimeMillis() < deadline) {
        if (runCatching(condition).getOrDefault(false)) return
        Thread.sleep(250)
    }
    fail("timed out after ${seconds}s waiting for $what")
}

/** Waits for an audit that ended after [after] (epoch seconds) and returns the state node. */
fun Stores.awaitAudit(
    after: Long,
    seconds: Long = 300,
    streamer: Streamer? = null,
): Map<String, Any?> {
    waitFor(seconds, "an audit ending after $after") {
        if (streamer != null && !streamer.alive) fail("the streamer died:\n" + streamer.logText().takeLast(4000))
        (state()["audit_ended_at"] as? String)?.toLong()?.let { it >= after } ?: false
    }
    return state()
}

/**
 * tools/verify against [relay], Neo4j and Redis: every author either side holds, every kind, every
 * Redis set. [grace]: events newer than now - grace are not compared (not audited yet).
 */
fun oracle(
    relay: LocalRelay,
    grace: Long = 3_900,
): Pair<Int, String> {
    val p =
        ProcessBuilder(
            E2E.python,
            "-I",
            E2E.verify,
            "--relay",
            relay.url,
            "--neo4j",
            E2E.neo4jUrl,
            "--neo4j-password",
            E2E.neo4jPassword,
            "--redis-port",
            E2E.redisPort.toString(),
            "--server-dir",
            E2E.serverDir,
            "--graph-sample",
            "1000000",
            "--relay-sample",
            "0",
            "--grace",
            grace.toString(),
        ).redirectErrorStream(true).start()
    val out = p.inputStream.bufferedReader().readText()
    p.waitFor()
    return p.exitValue() to out
}

fun assertOracle(
    relay: LocalRelay,
    grace: Long = 3_900,
) {
    val (code, out) = oracle(relay, grace)
    println("oracle: " + out.lines().filter { it.startsWith("compared") || it.startsWith("global") || it.startsWith("checking") }.joinToString(" | "))
    assertEquals(0, code, "tools/verify found differences:\n" + out.substringAfter("compared per kind"))
}

/** [events], in this order, through brainstorm_server's own consumer into the graph's stores. */
fun serverBuild(
    events: List<Event>,
    relay: LocalRelay,
    dir: File,
) {
    val file = File(dir, "arrivals.jsonl")
    file.writeText(events.joinToString("\n") { it.toJson() })
    val p =
        ProcessBuilder(
            E2E.serverPython ?: error("GS_E2E_SERVER_PYTHON: a Python with brainstorm_server's dependencies"),
            E2E.serverBuild,
            "--server-dir",
            File(E2E.serverDir).absolutePath,
            "--events",
            file.absolutePath,
            "--neo4j",
            E2E.neo4jUrl,
            "--neo4j-password",
            E2E.neo4jPassword,
            "--redis-port",
            E2E.redisPort.toString(),
            "--relay",
            relay.url,
        ).redirectErrorStream(true).start()
    val out = p.inputStream.bufferedReader().readText()
    check(p.waitFor() == 0) { "server_build.py failed:\n$out" }
    println("server build: " + out.lines().lastOrNull { it.isNotBlank() })
}

fun docker(vararg args: String) {
    val p = ProcessBuilder(listOf("docker") + args).redirectErrorStream(true).start()
    val out = p.inputStream.bufferedReader().readText()
    check(p.waitFor() == 0) { "docker ${args.joinToString(" ")}: $out" }
}
