package com.nosfabrica.graphstreamer.store

import java.sql.Connection
import java.sql.DriverManager

/**
 * Pubkey → User id, from `nostr_user` in the Scorecard database. The table is the contract
 * (brainstorm_server owns its schema); this is the same statement the server runs.
 */
fun interface UserIds {
    /** The id of every pubkey, giving one to any that has none. Committed before it returns. */
    fun idsFor(pubkeys: Collection<String>): Map<String, Int>
}

class PostgresUserIds(
    private val url: String,
    private val user: String,
    private val password: String,
) : UserIds {
    override fun idsFor(pubkeys: Collection<String>): Map<String, Int> {
        val wanted = pubkeys.distinct()
        if (wanted.isEmpty()) return emptyMap()
        wanted.firstOrNull { !HEX64.matches(it) }?.let { error("not a 64-char lowercase hex pubkey: ${it.take(80)}") }
        // Autocommit: each statement is its own committed transaction.
        DriverManager.getConnection(url, user, password).use { connection ->
            val found = HashMap<String, Int>(wanted.size * 2)
            for (chunk in wanted.chunked(CHUNK)) {
                found += query(connection, CREATE, chunk)
                // A concurrent writer inserted these first; its rows were not in our snapshot.
                val missing = chunk.filterNot(found::containsKey)
                if (missing.isNotEmpty()) found += query(connection, READ, missing)
            }
            check(found.size == wanted.size) { "${wanted.size - found.size} pubkeys were given no user id" }
            return found
        }
    }

    private fun query(
        connection: Connection,
        sql: String,
        pubkeys: List<String>,
    ): MutableMap<String, Int> =
        connection.prepareStatement(sql).use { statement ->
            statement.setArray(1, connection.createArrayOf("text", pubkeys.toTypedArray()))
            statement.executeQuery().use { rows ->
                val out = HashMap<String, Int>(pubkeys.size * 2)
                while (rows.next()) out[rows.getString("pubkey")] = rows.getInt("id")
                out
            }
        }

    companion object {
        private val HEX64 = Regex("[0-9a-f]{64}")
        private const val CHUNK = 50_000

        // Only missing pubkeys reach the INSERT, so a known one never draws from the sequence.
        // Sorted, so overlapping batches lock index entries in one order.
        private const val CREATE = """
            WITH input AS (
                SELECT DISTINCT pubkey FROM unnest(?) AS t(pubkey)
            ), inserted AS (
                INSERT INTO nostr_user (pubkey)
                SELECT i.pubkey FROM input i
                WHERE NOT EXISTS (SELECT 1 FROM nostr_user u WHERE u.pubkey = i.pubkey)
                ORDER BY i.pubkey
                ON CONFLICT (pubkey) DO NOTHING
                RETURNING id, pubkey
            )
            SELECT id, pubkey FROM inserted
            UNION ALL
            SELECT u.id, u.pubkey FROM nostr_user u JOIN input i USING (pubkey)
        """

        private const val READ = "SELECT id, pubkey FROM nostr_user WHERE pubkey = ANY(?)"
    }
}
