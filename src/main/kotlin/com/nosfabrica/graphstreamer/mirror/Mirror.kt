package com.nosfabrica.graphstreamer.mirror

import com.nosfabrica.graphstreamer.Mirrored
import com.nosfabrica.graphstreamer.store.StreamerState
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong

/**
 * The local copy of the search relay's graph kinds, and the only way anything is written to it.
 *
 * The store (Quartz's SQLite `EventStore`) applies NIP-01 supersession, NIP-09 kind 5, NIP-62
 * vanish and NIP-40 expiry itself, so the projection never has to: it reads what survives.
 * Every write marks the authors it touches dirty first, under a lock the projector also takes
 * to read, so a projection either sees a write whole or leaves its authors marked.
 */
class Mirror(
    val store: IEventStore,
    private val state: StreamerState,
) {
    private val lock = Mutex()

    val accepted = AtomicLong()
    val rejected = AtomicLong()
    val duplicates = AtomicLong()
    val removed = AtomicLong()

    /** Writes the events the mirror keeps and has not seen. No signature checks: the search relay verifies what it stores. */
    suspend fun ingest(events: List<Event>) {
        val wanted = events.filter(Mirrored::keeps).distinctBy { it.id }
        if (wanted.isEmpty()) return
        lock.withLock {
            val known = store.query<Event>(Filter(ids = wanted.map { it.id })).mapTo(HashSet()) { it.id }
            val fresh = wanted.filter { it.id !in known }
            duplicates.addAndGet((wanted.size - fresh.size).toLong())
            if (fresh.isEmpty()) return
            state.markDirty(fresh.mapTo(HashSet()) { it.pubKey })
            store.batchInsert(fresh).forEach { outcome ->
                if (outcome is IEventStore.InsertOutcome.Accepted) accepted.incrementAndGet() else rejected.incrementAndGet()
            }
        }
    }

    /** Drops events by id (the relay no longer holds them) and marks their authors. */
    suspend fun remove(ids: List<String>) {
        if (ids.isEmpty()) return
        lock.withLock {
            for (chunk in ids.chunked(ID_CHUNK)) {
                val events = store.query<Event>(Filter(ids = chunk))
                if (events.isEmpty()) continue
                state.markDirty(events.mapTo(HashSet()) { it.pubKey })
                store.delete(Filter(ids = events.map { it.id }))
                removed.addAndGet(events.size.toLong())
            }
        }
    }

    /** Runs [block] with no write in flight, so what it reads from the store and the dirty set agrees. */
    suspend fun <T> consistent(block: suspend () -> T): T = lock.withLock { block() }

    companion object {
        const val ID_CHUNK = 500
    }
}
