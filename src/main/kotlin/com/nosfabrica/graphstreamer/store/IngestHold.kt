package com.nosfabrica.graphstreamer.store

import redis.clients.jedis.JedisPool

/** Whether the graph and its Redis sets may be written now. */
fun interface WriteGate {
    fun open(): Boolean
}

/**
 * brainstorm_server's ingest gate (`ingest_gate.py`), read from the same Redis: `ingest:hold`
 * stops every writer until an operator deletes it, and a claim of the pubkey naming means the
 * sets are not named by User id here, which is the only naming this service writes.
 */
class RedisIngestHold(
    private val redis: JedisPool,
) : WriteGate {
    override fun open(): Boolean {
        val (held, naming) = redis.resource.use { it.mget(HOLD_KEY, NAMING_KEY) }
        return held == null && (naming == null || naming == NAMED_BY_ID)
    }

    companion object {
        const val HOLD_KEY = "ingest:hold"
        const val NAMING_KEY = "migration:redis_sets:naming"
        const val NAMED_BY_ID = "ids"
    }
}
