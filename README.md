# graph-streamer

Keeps Brainstorm's trust graph equal to what our search relay (`wss://search.brainstorm.world`)
holds: follows, mutes, reports and kind-10040 designations in Neo4j, and their Redis copies.
It replaces the neofry path:
- neofry's router and the transferer filling a strfry;
- the `strfry:events` Redis list;
- brainstorm_server's `process_strfry_event` consumer;
- the `kind3Backfill` and `reportBackfill` workarounds around them.

**Status: rehearsed, not deployed.** Unit tests with fakes; an end-to-end suite against a real
relay, Neo4j and Redis covering every way an edge enters or leaves, the failures we could name and
the migration from a server-built graph ([E2E.md](E2E.md)); a rehearsal against
search.brainstorm.world on 2026's slice (below). Not yet run at production size. It defaults to
`DRY_RUN=true`.

It keeps no copy of the events. The relay is the store. The graph records, on each author's
node, which events its edges came from, and that is what the audit compares with the relay.

## How it works

```
search relay ──── list / report ──────▶ apply (newer list replaces; report adds) ─┐
  (live REQ)                                                                      ├─▶ Neo4j + Redis
                                       recompute the author from the relay ──────┘
                                                  ▲
search relay ◀──── daily negentropy audit (need: download + apply; have: recompute)
```

1. **Live stream.** One long-lived REQ on the search relay. The relay fans its sync's writes out
   to live subscribers, so each new list and report arrives as the relay stores it. Deletions
   (kind 5) and vanishes (kind 62) aren't streamed: most clients leave the `k` tag off a kind 5,
   so a filter for deletions of graph kinds would miss most of them, and streaming every kind 5
   would be far more to download. The audit catches them. It is Quartz's `subscribeEvents` (2.14+). Quartz reconnects on its own, with backoff, and
   re-sends the REQ with its `since` moved to the newest `created_at` the stream was handed
   (stored events counted at EOSE, capped at the machine's clock), minus `LIVE_OVERLAP_SECONDS`
   (default 0). So after an outage of any length the REQ asks from where it began. The resume
   point is also saved every `LIVE_RESUME_POINT_SECONDS`, and a restart starts from it.
   Re-applying an event writes nothing.
   Any CLOSED stops the process except the two the client recovers from itself: a rate limit with
   a stated back-off, and an `error:` or unprefixed one followed within 3 s by the connection
   dropping (a relay restarting), unless that repeats three times running. After a CLOSED, Quartz
   doesn't send the REQ again on the same connection, only on the next reconnect or AUTH, and not
   at all once the relay has refused the same filters three times. On a healthy connection that
   can be never, and a stream that is silent until the socket happens to drop is worse than a
   restart.
2. **Apply.** A list (kind 3, 10000 or 10040) replaces the one the author's node records only
   if it is newer: later `created_at`, or the lower id on the same second. Its edges are diffed
   in, and its id and `created_at` are recorded. A report adds its targets and records its id.
   Nothing is ever *removed* by applying an event.
3. **Recompute.** When the audit finds the relay no longer holds an event the graph was built
   from, the relay has already applied whatever removed it. The audit rebuilds that author from
   everything the relay holds for them now (batched, `PROJECT_BATCH` per read), diffing against
   the graph. This is the only way edges leave the graph. A relay read that doesn't finish
   changes nothing; the audit stops recomputing that kind, and the next audit finds the same
   events still missing.
4. **Audit.** Every `AUDIT_EVERY_HOURS`, each kind is reconciled with the relay over NIP-77,
   one `created_at` window at a time: a window is split while either side holds more than
   `AUDIT_WINDOW` events (ours counted in Neo4j, the relay's by NIP-45 COUNT; on a first audit
   our side is empty, and without the relay's count the whole corpus would be one window).
   Our side is read from the graph's bookkeeping:
   - *need* (the relay has it, the graph doesn't): downloaded and applied. This covers late
     arrivals, missed live events and, on the first run, the whole past.
   - *have* (the graph has it, the relay doesn't): the author is recomputed, for
     whatever reason the relay dropped it: a deletion, a vanish, an expiry, a ban.
     The search relay is the source of truth, the same way vespa-relay's `deleteMissing` treats a
     provider's relay.
   - *orphans* (edges with no bookkeeping, so no audit compares them: another writer's, such as
     the server's consumer before cutover): after a complete reconcile, those authors are rebuilt
     from the relay, behind `AUDIT_MAX_DELETES`. A sweep that finds none marks the kind
     `adopted_<kind>` and the scan stops; from then on every edge has bookkeeping. Not with
     `AUDIT_SINCE`. An author with report edges and no report ids is rebuilt from the relay on
     their next report too, since applying a report only adds.

Every graph write is read, compute, write under one lock, so applies and recomputes never
interleave on an author.

### When absence is trusted

An audit marks the authors of absent events only when:

| guard | why |
|---|---|
| the reconcile **completed** | A window negentropy can't reconcile is paged instead (the graph still fills), and that kind recomputes nobody. A reconcile that fails outright (a disconnect, a timeout) pages nothing: the audit is reported incomplete and runs again `AUDIT_RETRY_MINUTES` later, as a delta against the bookkeeping. The relay refuses a Vespa answer with partial coverage (`coverage.full: false`, vespa-eventstore `VespaEventIndex`), so a content node that is down fails the reconcile. It does not make a third of the corpus look deleted. |
| ≥ 1 window compared | Zero windows compared nothing. |
| older than 1 h | The audit stops at `until` = now − 1 h. An event the live stream just delivered may not be visible to the relay's id reads yet. |
| ≤ `AUDIT_MAX_DELETES` absent per kind | Default 10 000; 0 turns it off. This relay is a mirror that has lost data before, so a mass absence is likelier its fault than a mass retraction. When the guard fires, the audit logs it and changes nothing. Raise the limit to let a real retraction through. |

### Deletion latency

Every removal takes effect at the next audit (`AUDIT_EVERY_HOURS`, default 24), as far as the
relay applied it: a kind 5, a kind 62 (the relay honours its own URL and `ALL_RELAYS`), a NIP-40
expiry, or anything else the relay drops. A retracted report keeps counting until then.

## What it reads

| live filter | why |
|---|---|
| `{"kinds":[3,10000,10040,1984]}` | follow lists, mute lists, designations, reports |

The audit reconciles one filter per kind (`{"kinds":[3]}`, …). A recompute reads
`{"authors":[…],"kinds":[3,10000,10040,1984]}`. Every filter carries `search: "include:spam"`,
because the relay refuses unauthenticated reads (REQ and NEG-OPEN alike) that name no
web-of-trust lens.

Kind 39999 (Trusted Lists taggings) is **not** handled yet: its handler writes brainstorm_server's
Postgres tables. That has to be settled before neofry can be retired.

## What it writes

These names are a contract with brainstorm_server and the GrapeRank worker. The edges and sets
are the ones the server's consumer writes today.

**Neo4j edges:** outgoing edges of `(:NostrUser {pubkey})`.

| edge | from | rule |
|---|---|---|
| `FOLLOWS` | newest kind 3 | its `p` tags: lowercased, 64-hex only, deduplicated |
| `MUTES` | newest kind 10000 | public `p` tags, same rule |
| `REPORTS` | every surviving kind 1984 | `p` tags of user reports only (a report with an `e` tag is about a note) |
| `DESIGNATES` | newest kind 10040 | public rows named `<kind>[:<tag>]`, never the author's own key |

**Neo4j bookkeeping:** on the author's node, written in the same transaction as the edges.

| property | meaning |
|---|---|
| `follows_event`, `follows_at` | id and `created_at` of the kind 3 the `FOLLOWS` edges came from |
| `mutes_event`, `mutes_at` | the same for kind 10000 |
| `designations_event`, `designations_at` | the same for kind 10040 |
| `report_events` | every report of theirs the relay holds, as `"<created_at>:<id>"`, note reports included |

A list is recorded even when it yields no edge (no valid `p` tags), and so is a note report. If
they weren't, the audit would see them as missing and download them every time. The `*_at` and
`*_event` properties get range indexes (`graph_streamer_<prop>`), which the audit reads lists
through. Reports are few, so they are read whole.

**Redis:** the reverse sets `followed_by:<target>`, `muted_by:<target>` and `reported_by:<target>`
(SADD/SREM of the author as their edges change), and the forward set `designates:<author>`
(replaced whole in one MULTI). Written after Neo4j, and kept a faithful copy of it two ways:

- **Outbox.** The transaction that writes an author's edges labels the node
  `GraphStreamerRedisPending` and records the targets they lost (`redis_pending_removed`); the
  label goes once Redis has the change. Startup replays whoever still has it, so a crash between
  the two stores loses nothing.
- **Reconcile.** Every set's size against its key's edge count in Neo4j; a set that differs is
  replaced from Neo4j, a set with no edge left deleted, a batch at a time under the writer's lock.
  It runs when the marker key `graph_streamer:redis_reconciled_at` is missing (Redis came up
  empty, was flushed, or was built by the server's consumer) and every `REDIS_AUDIT_EVERY_HOURS`.
  What it repaired goes to the state node (`redis_replaced`, `redis_deleted`): evictions show.

Redis holds only those sets, which the GrapeRank worker and the server read. Production runs
`allkeys-lru` at `maxmemory 24gb`; at the full graph's size (see below) these sets would be
evicted, and an evicted set reads as empty to GrapeRank. Size Redis for them, or make them
unevictable (`volatile-lru`, since they carry no TTL), before cutover.

**The service's own state:** one `(:GraphStreamerState {name: "graph-streamer"})` node in the
same Neo4j, every value a string.

| property | meaning |
|---|---|
| `live_since` | epoch seconds the live stream resumes from after a restart (its last resume point) |
| `audit_started_at`, `audit_ended_at`, `audit_complete` | the last audit's start, end and whether every kind completed; an incomplete one runs again `AUDIT_RETRY_MINUTES` after its end |
| `audit_finished_at` | when the last complete audit ended; the next is due `AUDIT_EVERY_HOURS` after it |
| `audit_<kind>_<counter>` | per audited kind (`follows`, `mutes`, `designations`, `reports`): `need`, `have`, `orphans`, `ignored`, `windows`, `downloaded`, `recomputed`, `complete`, `note` |
| `adopted_<kind>` | `true` once an orphan sweep of that kind found none |
| `redis_reconciled_at`, `redis_keys`, `redis_replaced`, `redis_deleted`, `redis_outbox` | the last Redis reconcile and outbox replay |

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
| `DRY_RUN` | `true` | `false` writes the graph; anything not a switch stops the boot |
| `AUDIT_EVERY_HOURS` | `24` | `0` turns the audit off |
| `AUDIT_RETRY_MINUTES` | `30` | when an incomplete audit (a failed reconcile) runs again |
| `REDIS_AUDIT_EVERY_HOURS` | `24` | full Redis reconcile from Neo4j; `0`: only when its marker is missing |
| `AUDIT_SINCE` | unset | epoch seconds: audit only events created from then on, both sides (a rehearsal or a partial rebuild) |
| `AUDIT_WINDOW` | `50000` | lists per reconcile window |
| `AUDIT_MAX_DELETES` | `10000` | absent events per kind per audit; `0` = no limit |
| `LIVE_RESUME_POINT_SECONDS` | `60` | how often the live stream's resume point is saved for a restart |
| `LIVE_OVERLAP_SECONDS` | `0` | how far before the resume point (the newest `created_at` the stream was handed) a reconnect or restart asks from. At 0, events still queued for the graph when the process stops wait for the next audit |
| `PROJECT_BATCH` | `200` | authors per recompute (one relay read) |

It is stateless and runs as one replica, because it must be the only writer of these edges. It
needs the `NostrUser.pubkey` uniqueness constraint that brainstorm_server creates. CI builds
`ghcr.io/nosfabrica/graph-streamer` on every push (branch, `sha-<short>`, and `latest` on `main`).

### Dry run

`DRY_RUN=true` writes nothing to the graph, schema included, and nothing to the graph's Redis
sets. It does keep its state node: without it every restart would run a full dry-run audit,
which downloads the relay's whole set.
It only counts the changes it would make (`stats:` log line, every minute). Because it records
no bookkeeping, every dry-run audit compares against an empty side and downloads everything the
relay holds. That is the measurement you want once: how far the neofry-built graph is from the
relay. Set `AUDIT_EVERY_HOURS` high, or to 0, after the first one.

## Cutover

1. Deploy with `DRY_RUN=true` beside the server's consumer. Read the first audit's counts: the
   edges it would add and remove, per type, and `audit_<kind>_orphans` (capped at
   `AUDIT_MAX_DELETES` + 1): the authors whose edges the relay can't account for.
2. Size or reconfigure Redis for the sets (see Redis above).
3. In one deploy, stop the server's `strfry:events` consumer for kinds 3, 5, 1984, 10000 and 10040
   and set `DRY_RUN=false`, with `AUDIT_MAX_DELETES` at least the orphan count you accepted. Never
   run two writers on the same edges. The first audit then records every author's bookkeeping,
   writing only the edges that differ; the sweep rebuilds the orphans; the Redis reconcile (its
   marker is missing) makes every set match Neo4j. Check with `tools/verify`.
4. Retire neofry's router streams and the transferer, along with `kind3Backfill`, `reportBackfill`
   and `neofryResync`. Kind 39999 is still open.

## Rehearsal (2026-11-04)

The real service against search.brainstorm.world, `DRY_RUN=false`, `AUDIT_SINCE` = 2026-01-01,
into an empty Neo4j 5 (heap 3G, page cache 4G) and Redis 7 on a 4-core, 16 GB machine. One
slice, one run each; production's graph is about ten times larger.

| | measured |
|---|---|
| first audit, follows | ~460k lists → 27.6M FOLLOWS edges, 720k users. 14k edges/s on small lists, 30k/s on lists of thousands. About 330 lists/s while fetching. |
| first audit, others | mutes 21,556 lists, designations 931, reports 109,334: seconds to a minute each |
| storage | Neo4j store 55 B/edge, transaction logs 126 B/edge (default retention, 2 days), Redis sets 97 B/member, nothing evicted |
| second audit, all kinds | 13 s (`AUDIT_WINDOW` 200,000 or 1,000,000: 5 or 1 windows), 17 s (50,000: 27 windows), 25 s (10,000: 101 windows). No window refused, 460k lists in one included. |
| Neo4j side of the audit | 8.46M synthetic lists: 24–30 s whatever the window size, ~350k pairs/s (`AuditLocalSideBenchmark`) |

What the rehearsal found:

- MERGE on edges was quadratic in a list's length; one batch of 500 large lists held a
  transaction for over five minutes. New edges are now created once per author.
- The relay reset the connection once mid-reconcile (about five minutes unanswered). That kind
  was reported incomplete, nothing was paged, and the retry was a delta.
- The relay holds two kind-3s for some authors (about 35 in the slice), and serves both by id and
  by author. The older is downloaded and not recorded on every audit (`<kind>.ignored`); the newer
  wins, so the graph is right. A relay bug, not ours.

## Not measured yet

- The first audit over the whole corpus: from the slice, ~220–275M FOLLOWS edges, ~14 GB of
  store, ~30 GB of transaction logs and ~22–27 GB of Redis sets, about seven to nine hours. That
  Redis figure is at production's `maxmemory 24gb` (allkeys-lru), where an evicted
  `followed_by:` set reads as empty to GrapeRank. Check production's `used_memory` first.
- Its load on Vespa. Run it when the content nodes have headroom.
