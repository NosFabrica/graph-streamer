package com.nosfabrica.graphstreamer.mirror

import com.nosfabrica.graphstreamer.Mirrored
import com.vitorpamplona.quartz.nip01Core.core.Tag
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.IndexingStrategy

/**
 * The store is read by author and kind, by id, and by the `k` of a deletion — never by `p`. A
 * follow list carries one `p` tag per follow, so indexing them would be most of the database for
 * no query. No full-text search either: the one search token used (`include:spam`) is an
 * extension the store strips, and a blank search constrains nothing.
 */
class GraphIndexing(
    private val base: IndexingStrategy = DefaultIndexingStrategy(indexFullTextSearch = false),
) : IndexingStrategy by base {
    override fun shouldIndex(
        kind: Int,
        tag: Tag,
    ): Boolean = !(tag.size >= 1 && tag[0] == "p" && (kind == Mirrored.FOLLOWS || kind == Mirrored.MUTES)) && base.shouldIndex(kind, tag)
}
