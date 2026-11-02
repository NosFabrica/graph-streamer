# graph-streamer

Long-running Kotlin service: a Quartz SQLite mirror of the search relay's graph kinds (live REQ +
negentropy audit), projected into Brainstorm's Neo4j graph and Redis sets. Read
[README.md](README.md) first; its "What it writes" tables are a contract with `brainstorm_server`
and `brainstorm_graperank_algorithm`.

## Map

| Path | What |
|---|---|
| `Main.kt` | Wires the store, relay client, Redis and Neo4j; runs the live stream, projector, audit and stats loops. |
| `Config.kt` | Environment. `DRY_RUN` defaults to true. |
| `Mirrored.kt` | The mirrored filters. The live REQ, the audit and the store all read them. |
| `mirror/Mirror.kt` | The only writer to the store. It marks authors dirty before writing. |
| `mirror/LiveStream.kt` | The REQ, re-sent with a moving `since`. |
| `mirror/Audit.kt` | NIP-77 reconcile per filter: download needs, delete haves under guards. |
| `mirror/GraphIndexing.kt` | Store indexing: no `p` index on kinds 3/10000, no FTS. |
| `projection/GraphRules.kt` | Pure: an author's surviving events → their edges. Rules match brainstorm_server's. |
| `projection/Projector.kt` | Dirty authors → diff against the graph → writes (or dry-run counts). |
| `store/` | `StreamerState` (dirty set, audit report) and `GraphSink` (Neo4j + Redis), with their Redis/Neo4j implementations. |

## Rules

- **The search relay is the source of truth.** Never add a second upstream. One relay's
  absence would no longer mean anything, and the audit could not delete.
- **Never delete on absence without a completed reconcile.** A paged fallback fills the mirror
  only; it must not delete. Keep every guard in `Audit` unless you replace it with a stronger one.
- **Project from the store, never from the event.** A handler that applies one event's diff
  can't see deletions, vanishes, expiries or audit removals. Recomputing the author can.
- **Every store write goes through `Mirror`**, which marks authors dirty first. A write
  anywhere else is a write the graph never hears about.
- **Changing a filter in `Mirrored` changes what the audit compares.** A new filter's whole past
  downloads on the next audit. Events a narrowed filter no longer covers are never compared
  again: they stay in the mirror, and still project, until the same change deletes them. Treat
  it as a data migration, not a refactor.
- **Edge names and Redis key names are cross-repo contracts** (the server's consumer and its
  `backfill_redis_relationships`, the GrapeRank worker). Never interpolate data into Cypher.
  Relationship types come from `Edge`.
- A new test must fail with the change it guards reverted. Check the test count, not only the
  exit code.

## Build

`./gradlew test`. Maven Central rate-limits bursts of parallel downloads; if a fresh build gets
HTTP 429, re-run with `--max-workers=1`. Keep the Kotlin plugin at or above the version Quartz is
built with.
