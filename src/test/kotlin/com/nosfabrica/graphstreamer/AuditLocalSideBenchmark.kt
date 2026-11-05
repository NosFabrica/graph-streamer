package com.nosfabrica.graphstreamer

import com.nosfabrica.graphstreamer.projection.ListKind
import com.nosfabrica.graphstreamer.relay.GraphListIndex
import com.nosfabrica.graphstreamer.store.Neo4jRedisGraph
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import redis.clients.jedis.JedisPool
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * What an audit's local side costs on a graph of production size: the follow lists' `(created_at,
 * id)` pairs read through [GraphListIndex] window by window, the way Quartz's reconcile asks for
 * them (`count` a window; over the target, cut it into equal spans of time, at most 32, and count
 * each again; at or under it, load it), but with no relay. Also reads every report, as the
 * reports audit does.
 *
 * Opt-in: LOCAL_SIDE_BENCH=1 and NEO4J_TEST_URL (NEO4J_TEST_USERNAME, NEO4J_TEST_PASSWORD), against
 * a database already holding the bookkeeping. LOCAL_SIDE_TARGETS lists the window targets to try
 * (default 50000, AUDIT_WINDOW's default).
 */
class AuditLocalSideBenchmark {
    private val url = System.getenv("NEO4J_TEST_URL")
    private val enabled = System.getenv("LOCAL_SIDE_BENCH") != null && url != null

    private class Walk(
        val target: Int,
    ) {
        var countCalls = 0
        var countMs = 0L
        var loadCalls = 0
        var loadMs = 0L
        var entries = 0L
        var largestWindow = 0
        var wallMs = 0L
    }

    /** Quartz's window walk (NostrClientNegentropySyncExt: splitInto, the reconcilers' pre-split). */
    private suspend fun walk(
        index: GraphListIndex,
        target: Int,
        now: Long,
    ): Walk {
        val result = Walk(target)
        val queue = ArrayDeque<Filter>()
        queue += Filter(kinds = listOf(3))
        val start = System.nanoTime()
        while (queue.isNotEmpty()) {
            val window = queue.removeFirst()
            val lo = window.since ?: 0L
            val hi = window.until ?: now
            if (hi - lo > 1) {
                val t0 = System.nanoTime()
                val mine = index.count(window) ?: 0
                result.countMs += (System.nanoTime() - t0) / 1_000_000
                result.countCalls++
                if (mine > target) {
                    val span = hi - lo + 1
                    val over = (mine.toLong() + target - 1) / target
                    val n = over.coerceIn(2L, minOf(span, 32L)).toInt()
                    val step = span / n
                    var s = lo
                    repeat(n) { i ->
                        if (i == n - 1) {
                            queue += window.copy(since = s, until = window.until)
                        } else {
                            queue += window.copy(since = s, until = s + step - 1)
                            s += step
                        }
                    }
                    continue
                }
            }
            val t0 = System.nanoTime()
            val loaded = index.entriesFor(window)
            result.loadMs += (System.nanoTime() - t0) / 1_000_000
            result.loadCalls++
            result.entries += loaded.size
            result.largestWindow = maxOf(result.largestWindow, loaded.size)
        }
        result.wallMs = (System.nanoTime() - start) / 1_000_000
        return result
    }

    @Test
    fun followListsAndReportsAtProductionSize() {
        if (!enabled) return
        val targets = (System.getenv("LOCAL_SIDE_TARGETS") ?: "50000").split(',').map { it.trim().toInt() }
        val driver = GraphDatabase.driver(url, AuthTokens.basic(System.getenv("NEO4J_TEST_USERNAME") ?: "neo4j", System.getenv("NEO4J_TEST_PASSWORD") ?: ""))
        // Never connected: the local side reads Neo4j only.
        val graph = Neo4jRedisGraph(driver, JedisPool("127.0.0.1", 1))
        graph.use {
            val index = GraphListIndex(graph, ListKind.FOLLOWS)
            val now = System.currentTimeMillis() / 1000
            val total = graph.countLists(ListKind.FOLLOWS, 0, Long.MAX_VALUE).toLong()
            println("LOCAL follow lists recorded: $total")
            runBlocking {
                for (target in targets) {
                    repeat(2) { pass ->
                        val w = walk(index, target, now)
                        assertEquals(total, w.entries, "every pair loaded exactly once")
                        val heapMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1_048_576
                        println(
                            "LOCAL target=%,d pass=%d: %.1f s wall; %d counts %.1f s; %d loads %.1f s (%,.0f pairs/s); largest window %,d; heap %,d MB".format(
                                target,
                                pass + 1,
                                w.wallMs / 1000.0,
                                w.countCalls,
                                w.countMs / 1000.0,
                                w.loadCalls,
                                w.loadMs / 1000.0,
                                w.entries * 1000.0 / maxOf(1, w.loadMs),
                                w.largestWindow,
                                heapMb,
                            ),
                        )
                    }
                }
            }
            repeat(2) { pass ->
                val t0 = System.nanoTime()
                val reports = graph.reports()
                println("LOCAL reports pass=%d: %,d in %.1f s".format(pass + 1, reports.size, (System.nanoTime() - t0) / 1e9))
            }
        }
    }
}
