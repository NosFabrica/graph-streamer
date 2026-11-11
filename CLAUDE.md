# graph-streamer

Long-running, stateless Kotlin service. Streams (live REQ) and audits (NIP-77) the search relay's
follow lists, mute lists, reports and kind-10040 designations into Brainstorm's Neo4j graph and
Redis sets. No local event store: the relay is the store, and each author's node records which
events its edges came from.

## Docs

| File | For |
|---|---|
| [README.md](README.md) | Overview, config, cutover. Its "What it writes" section is a contract with `brainstorm_server` and `brainstorm_graperank_algorithm`. Keep it short. |
| [DESIGN.md](DESIGN.md) | Mechanics and the reason for each safeguard: live stream, audit, absence guards, Redis outbox, state node, rehearsal numbers. Read before changing `relay/` or `projection/`. |
| [E2E.md](E2E.md) | Running the end-to-end suite and what each scenario proves. |

Keep them in step with the code: a new env var goes in `Config.kt` and README's table; a changed
mechanism or safeguard in DESIGN.md; a new e2e scenario in E2E.md's table.

## Map

| Path | What |
|---|---|
| `Main.kt` | Wires the relay client, Redis and Neo4j; runs the live stream, audit and Redis-reconcile loops. |
| `Config.kt` | Environment. `DRY_RUN` defaults to true. |
| `RelayFilters.kt` | Every filter sent to the relay: live, audit, by-author. |
| `projection/GraphRules.kt` | Pure: `apply` (events onto an author's state) and `fromRelay` (state from the relay's events). Parsing matches brainstorm_server's. `Edge` and `ListKind` name every edge and property. |
| `projection/Projector.kt` | The only graph writer: applies events, recomputes authors, under one lock. |
| `relay/LiveStream.kt` | The live REQ (Quartz `subscribeEventBatches`), written a batch at a time; resume point from `caughtUpAtFlow`. |
| `relay/Audit.kt` | NIP-77 reconcile per kind against the graph's bookkeeping; `AbsenceGuard`; `GraphListIndex`. |
| `relay/SearchRelayReader.kt` | By-author reads for recomputes; throws `IncompleteRead` unless the relay drained. |
| `store/` | `StreamerState` (resume point, audit report: one Neo4j node), `GraphStore` (Neo4j + Redis), `AuthorSource`, implementations. |
| `tools/verify/` | The e2e oracle: checks the graph with brainstorm_server's parsers. |

## Rules

- **The search relay is the only upstream.** With a second one, absence from one relay would mean
  nothing and no edge could ever leave the graph.
- **Applying an event only adds or replaces.** Every removal is a recompute from the relay; a
  handler that applies deletions itself misses vanishes, expiries and `k`-less deletions.
- **A recompute acts only on a read that drained** (`IncompleteRead` otherwise). Half an answer
  reads as deletions.
- **Never act on absence without a completed reconcile.** Paged fallback fills the graph only,
  and only for a bounded window; a failed reconcile is retried as a delta, never paged whole.
  Keep every guard in `AbsenceGuard` unless you replace it with a stronger one.
- **Every graph write goes through `Projector`**, under its lock, edges and bookkeeping in one
  Neo4j transaction. Bookkeeping that disagrees with the edges makes the audit wrong.
- **Redis is a copy of Neo4j.** The same transaction labels the author
  `GraphStreamerRedisPending`; the label is removed after Redis has the change. A write path that
  skips it loses Redis on a crash (`killedMidWriteItRecovers`).
- **Bound windows by both sides.** Every audited kind's index goes through Quartz's
  `withRelayCount` (the relay's NIP-45 COUNT); on a first audit the local count is zero.
- **The resume point counts only what was written.** Write the batch inside `collect`; never put a
  `buffer()` between `subscribeEventBatches` and the write.
- **Record every event the audit compares, even with no edge** (empty list, note report), or it is
  downloaded on every audit.
- **Changing an audit filter in `RelayFilters` is a data migration**: it changes what is compared
  with the bookkeeping.
- **Edge names, bookkeeping properties and Redis keys are cross-repo contracts.** Take them from
  `Edge` and `ListKind`; never interpolate data into Cypher.

## Build and test

- `./gradlew test`. On HTTP 429 from Maven Central, re-run with `--max-workers=1`.
- `./gradlew e2eTest` ([E2E.md](E2E.md)) runs the service against a real relay, Neo4j and Redis,
  judged by `tools/verify`. Add a scenario for any new way an edge enters or leaves, and any
  failure you fix.
- A new test must fail with its change reverted. Check the test count, not only the exit code: a
  compile error also prints no failures.
- Keep the Kotlin plugin at or above the version Quartz is built with.
- Quartz 2.16.0 comes from JitPack as `com.github.vitorpamplona.quartz:quartz-jvm` (Maven Central
  refused it: monthly size limit, resets 2026-12-01). With the next release on Central, go back
  to `com.vitorpamplona.quartz:quartz`, drop the JitPack repository, and update the `-PquartzDir`
  substitution in `settings.gradle.kts`.
