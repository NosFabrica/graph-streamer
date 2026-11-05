# graph-streamer

Long-running, stateless Kotlin service: the search relay's follow lists, mute lists, reports and
kind-10040 designations, streamed (live REQ) and audited (NIP-77) into Brainstorm's Neo4j graph
and Redis sets. No local event store: the relay is the store, and the graph records which events
its edges came from. Read [README.md](README.md) first. Its "What it writes" tables are a contract
with `brainstorm_server` and `brainstorm_graperank_algorithm`.

## Map

| Path | What |
|---|---|
| `Main.kt` | Wires the relay client, Redis and Neo4j; runs the live stream, audit (which recomputes) and stats loops. |
| `Config.kt` | Environment. `DRY_RUN` defaults to true. |
| `RelayFilters.kt` | Every filter sent to the relay: live, audit, by-author. |
| `projection/GraphRules.kt` | Pure: `apply` (events onto an author's state) and `fromRelay` (state from the relay's events). Parsing matches brainstorm_server's. |
| `projection/Projector.kt` | The only graph writer: applies events, recomputes the authors an audit hands it, under one lock. |
| `relay/LiveStream.kt` | The live REQ (Quartz's `subscribeEventBatches`), written a batch at a time, the resume point saved from `caughtUpAtFlow`; a CLOSED the client can't recover from stops the process. |
| `relay/Audit.kt` | NIP-77 reconcile per kind against the graph's bookkeeping; `AbsenceGuard`; `GraphListIndex`. |
| `relay/SearchRelayReader.kt` | By-author reads for recomputes; throws unless the relay drained. |
| `store/` | `StreamerState` (live resume point, audit report: one Neo4j node), `GraphStore` (Neo4j + Redis), `AuthorSource`, and their implementations. |

## Rules

- **The search relay is the source of truth.** Never add a second upstream. One relay's
  absence would no longer mean anything, and nothing could ever leave the graph.
- **Applying an event only ever adds or replaces.** Anything that removes goes through a recompute
  from the relay. A handler that applies a deletion itself can't see vanishes, expiries or
  `k`-less deletions, and it drifts from the relay.
- **A recompute acts only on a read that drained** (`IncompleteRead` otherwise). Half an answer
  reads as deletions.
- **Never act on absence without a completed reconcile.** A paged fallback fills the graph only.
  Page only a bounded window: a failed reconcile is retried as a delta, never paged whole, since
  that downloads the corpus again.
  Keep every guard in `AbsenceGuard` unless you replace it with a stronger one.
- **Every graph write goes through `Projector`**, under its lock, with edges and bookkeeping in
  one Neo4j transaction. Bookkeeping that disagrees with the edges makes the audit wrong.
- **Redis is a copy of Neo4j, never written on its own.** The same transaction marks the author
  in the Redis outbox (`GraphStreamerRedisPending`); the mark goes after Redis has the change. A
  write path that skips the mark loses Redis on a crash (`killedMidWriteItRecovers`).
- **Bound windows by both sides.** Quartz splits a reconcile window on the local index's count; on
  a first audit that is zero. Every audited kind's index goes through Quartz's `withRelayCount`,
  which adds the relay's NIP-45 COUNT (bounded, retried, paused on a relay that stops answering).
- **The live resume point counts only what was written.** `subscribeEventBatches` hands
  `CaughtUpResume` an event once its batch's `collect` returns: never put a `buffer()` between
  them, and write the batch inside the block.
- **Record every event the audit compares, even when it yields no edge** (an empty list, a note
  report). Otherwise it is downloaded again on every audit.
- **Changing an audit filter in `RelayFilters` changes what is compared with the bookkeeping.**
  Treat it as a data migration.
- **Edge names, bookkeeping properties and Redis key names are cross-repo contracts** (the
  server's consumer and its `backfill_redis_relationships`, the GrapeRank worker). Never
  interpolate data into Cypher. Relationship types and property names come from `Edge` and
  `ListKind`.
- `./gradlew e2eTest` ([E2E.md](E2E.md)) runs the service against a real relay, Neo4j and Redis,
  judged by `tools/verify` (the server's parsers). Add a scenario for any new way an edge enters
  or leaves, and any failure you fix.
- A new test must fail with the change it guards reverted. Check the test count, not only the
  exit code: a compile error also prints no failures.

## Build

`./gradlew test`. Maven Central rate-limits bursts of parallel downloads; if a fresh build gets
HTTP 429, re-run with `--max-workers=1`. Keep the Kotlin plugin at or above the version Quartz is
built with.

Quartz 2.16.0 comes from JitPack, as `com.github.vitorpamplona.quartz:quartz-jvm`: Maven Central
refused that release (the org's monthly size limit, reset 2026-12-01). With the next release on
Central, go back to `com.vitorpamplona.quartz:quartz`, drop the JitPack repository, and change the
`-PquartzDir` substitution in `settings.gradle.kts` to match.
