package com.nosfabrica.graphstreamer.relay

import com.nosfabrica.graphstreamer.RelayFilters
import com.nosfabrica.graphstreamer.store.AuthorSource
import com.nosfabrica.graphstreamer.store.IncompleteRead
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPagesResuming
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * Reads authors' current lists and reports from the search relay, paging past its cap and
 * resuming across dropped connections. No signature checks: the relay verifies what it stores.
 */
class SearchRelayReader(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val idleTimeoutMs: Long = 30_000,
) : AuthorSource {
    override suspend fun currentEvents(authors: List<String>): List<Event> {
        if (authors.isEmpty()) return emptyList()
        val events = ArrayList<Event>()
        val result =
            client.fetchAllPagesResuming(relay = relay, filters = RelayFilters.byAuthors(authors), idleTimeoutMs = idleTimeoutMs, pageSize = PAGE) {
                events.add(it)
            }
        if (result.end != PagedFetchResult.End.DRAINED) {
            throw IncompleteRead("reading ${authors.size} authors ended ${result.end}${result.message?.let { ": $it" } ?: ""}")
        }
        return events
    }

    companion object {
        private const val PAGE = 500
    }
}
