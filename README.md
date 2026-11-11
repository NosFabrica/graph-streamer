# graph-streamer

Keeps Brainstorm's trust graph in sync with the search relay (`wss://search.brainstorm.world`):
follows, mutes, reports and kind-10040 designations, written as Neo4j edges and Redis sets.

It replaces neofry's router and transferer, the `strfry:events` Redis list, brainstorm_server's
`process_strfry_event` consumer, and the `kind3Backfill` / `reportBackfill` workarounds.

**Status: rehearsed, not deployed.** Unit tests, an [end-to-end suite](E2E.md), and a rehearsal
against the real relay on 2026's slice. Not yet run at production size. Defaults to `DRY_RUN=true`.

## How it works

```
search relay ── live REQ ──▶ apply (newer list replaces; report adds) ──┐
                                                                         ├─▶ Neo4j + Redis
search relay ◀─ daily NIP-77 audit ─▶ recompute authors from the relay ─┘
```

- **Live stream:** one long-lived subscription; new lists and reports are applied as they arrive.
- **Audit:** every 24 h, each kind is reconciled with the relay. Missing events are downloaded;
  events the relay no longer holds (deletions, vanishes, expiries) trigger a rebuild of that author.
- **No local event store.** The relay is the store. Each author's node records which events its
  edges came from, and that is what the audit compares.

Deletions take effect at the next audit. Details, safety guards and rehearsal numbers:
[DESIGN.md](DESIGN.md).

## What it writes

These names are a contract with brainstorm_server and the GrapeRank worker.

**Neo4j edges**, from `(:NostrUser {pubkey})`:

| edge | from | rule |
|---|---|---|
| `FOLLOWS` | newest kind 3 | `p` tags: lowercased, 64-hex only, deduplicated |
| `MUTES` | newest kind 10000 | public `p` tags, same rule |
| `REPORTS` | every kind 1984 the relay holds | `p` tags of user reports only (a report with an `e` tag is about a note) |
| `DESIGNATES` | newest kind 10040 | public rows named `<kind>[:<tag>]`, never the author's own key |

**Neo4j bookkeeping**, on the author's node, in the same transaction as the edges:
`follows_event`/`follows_at`, `mutes_event`/`mutes_at`, `designations_event`/`designations_at`
(id and `created_at` of the list the edges came from), and `report_events`
(`"<created_at>:<id>"` per report, note reports included).

**Redis:** `followed_by:<target>`, `muted_by:<target>`, `reported_by:<target>` (reverse sets) and
`designates:<author>` (forward set). Always a copy of Neo4j, repaired from it when they differ.

**Own state:** one `(:GraphStreamerState {name: "graph-streamer"})` node (resume point, audit
report). See [DESIGN.md](DESIGN.md#state-node).

## Running

```sh
./gradlew test
./gradlew run   # needs the environment below
```

| variable | default | |
|---|---|---|
| `NEO4J_URL`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` | — | same names as the GrapeRank worker |
| `REDIS_HOST`, `REDIS_PORT` | — | |
| `SEARCH_RELAY_URL` | `wss://search.brainstorm.world` | |
| `DRY_RUN` | `true` | `false` writes the graph; any other value stops the boot |
| `AUDIT_EVERY_HOURS` | `24` | `0` turns the audit off |
| `AUDIT_RETRY_MINUTES` | `30` | retry delay for an incomplete audit |
| `REDIS_AUDIT_EVERY_HOURS` | `24` | full Redis-vs-Neo4j check; `0`: only when Redis comes up empty |
| `AUDIT_SINCE` | unset | epoch seconds: audit only events from then on (rehearsal, partial rebuild) |
| `AUDIT_WINDOW` | `50000` | events per reconcile window |
| `AUDIT_MAX_DELETES` | `10000` | max absent events acted on per kind per audit; `0` = no limit |
| `LIVE_RESUME_POINT_SECONDS` | `60` | how often the live resume point is saved |
| `PROJECT_BATCH` | `200` | authors per recompute read |

Run exactly **one replica**: it must be the only writer of these edges. It needs the
`NostrUser.pubkey` uniqueness constraint brainstorm_server creates. CI publishes
`ghcr.io/nosfabrica/graph-streamer` on every push (branch, `sha-<short>`, `latest` on `main`).

## Cutover

1. Deploy with `DRY_RUN=true` beside the server's consumer and read the first audit's counts.
2. Size Redis for the sets, or make them unevictable (production runs `allkeys-lru`).
3. In one deploy, stop the server's consumer for kinds 3, 5, 1984, 10000, 10040 and set
   `DRY_RUN=false`. Check with `tools/verify`.
4. Retire neofry's router, the transferer and the backfill jobs.

Kind 39999 (Trusted Lists) is not handled yet and blocks retiring neofry. Step-by-step notes:
[DESIGN.md](DESIGN.md#cutover).
