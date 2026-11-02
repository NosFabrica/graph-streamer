package com.nosfabrica.graphstreamer.store

import redis.clients.jedis.JedisPool

/**
 * Keys (all this service's own; the graph's sets are written by [Neo4jRedisGraph]):
 * - `graph_streamer:dirty` — HASH author → generation: authors waiting to be recomputed from the relay.
 * - `graph_streamer:audit` — HASH: the last negentropy audit's counters.
 */
class RedisStreamerState(
    private val pool: JedisPool,
) : StreamerState {
    override fun markDirty(pubkeys: Collection<String>) {
        if (pubkeys.isEmpty()) return
        pool.resource.use { redis ->
            val pipe = redis.pipelined()
            for (pk in pubkeys) pipe.hincrBy(DIRTY_KEY, pk, 1)
            pipe.sync()
        }
    }

    override fun dirty(limit: Int): List<DirtyAuthor> =
        pool.resource.use { redis ->
            redis
                .hrandfieldWithValues(DIRTY_KEY, limit.toLong())
                .map { DirtyAuthor(it.key, it.value.toLong()) }
        }

    override fun clearDirty(authors: List<DirtyAuthor>): Int {
        if (authors.isEmpty()) return 0
        val args = authors.flatMap { listOf(it.pubkey, it.generation.toString()) }
        return pool.resource.use { redis ->
            (redis.eval(CLEAR_IF_UNCHANGED, listOf(DIRTY_KEY), args) as Long).toInt()
        }
    }

    override fun dirtyCount(): Long = pool.resource.use { it.hlen(DIRTY_KEY) }

    override fun lastAuditFinishedAt(): Long? = pool.resource.use { it.hget(AUDIT_KEY, "finished_at")?.toLongOrNull() }

    override fun putAuditReport(report: Map<String, String>) {
        if (report.isEmpty()) return
        pool.resource.use { it.hset(AUDIT_KEY, report) }
    }

    companion object {
        const val DIRTY_KEY = "graph_streamer:dirty"
        const val AUDIT_KEY = "graph_streamer:audit"

        /** HDEL each author only if its generation is still the one the projector read. */
        private val CLEAR_IF_UNCHANGED =
            """
            local n = 0
            for i = 1, #ARGV, 2 do
              if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
                redis.call('HDEL', KEYS[1], ARGV[i])
                n = n + 1
              end
            end
            return n
            """.trimIndent()
    }
}
