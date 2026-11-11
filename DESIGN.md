# Design

How graph-streamer keeps the graph equal to the search relay, and why each safeguard exists.
The short version is in [README.md](README.md); the test suite in [E2E.md](E2E.md).

## Live stream

One long-lived REQ, Quartz's `subscribeEventBatches`, with `{"kinds":[3,10000,10040,1984]}`.
The relay fans its sync's writes out to live subscribers, so each new list and report arrives as
the relay stores it.

- **Not streamed:** deletions (kind 5) and vanishes (kind 62). Most clients leave the `k` tag off a
  kind 5, so a filter for deletions of graph kinds would miss most of them, and streaming every
  kind 5 would be far more to download. The audit catches them.
- **Reconnects:** Quartz reconnects with backoff and re-sends the REQ with `since` moved to the
  newest `created_at` written to the graph (capped at the machine's clock).
- **Resume point:** counts only written batches; saved every `LIVE_RESUME_POINT_SECONDS`, and a
  restart starts from it.
- **Failed writes** are retried for two minutes, then left to the audit. Re-applying an event
  writes nothing.
- **CLOSED** stops the process, except the two the client recovers from: a rate limit with a
  stated back-off, and an `error:` or unprefixed CLOSED followed within 3 s by the connection
  dropping (a relay restarting), unless that repeats three times running. After a CLOSED, Quartz
  re-sends the REQ only on the next reconnect or AUTH; on a healthy connection that can be never,
  and a silent stream is worse than a restart.

## Apply

A list (kind 3, 10000 or 10040) replaces the one the author's node records only if it is newer:
later `created_at`, or the lower id on the same second. Its edges are diffed in and its id and
`created_at` recorded. A report adds its targets and records its id. **Applying never removes.**

## Recompute

When the audit finds the relay no longer holds an event the graph was built from, the relay has
already applied whatever removed it. The author is rebuilt from everything the relay holds for
them now (`PROJECT_BATCH` authors per read), diffed against the graph. This is the only way edges
leave the graph. A read that doesn't drain changes nothing; that kind stops recomputing and the
next audit finds the same events missing.

Every graph write is read, compute, write under one lock, so applies and recomputes never
interleave on an author.

## Audit

Every `AUDIT_EVERY_HOURS`, each kind is reconciled over NIP-77 (`{"kinds":[3]}`, …), one
`created_at` window at a time. A window is split while either side holds more than `AUDIT_WINDOW`
events: ours counted in Neo4j, the relay's by NIP-45 COUNT (on a first audit our side is empty,
and without the relay's count the whole corpus would be one window). Our side is the graph's
bookkeeping.

- **need** (relay has it, graph doesn't): downloaded and applied. Late arrivals, missed live
  events and, on the first run, the whole past.
- **have** (graph has it, relay doesn't): the author is recomputed, whatever the reason: a
  deletion, a vanish, an expiry, a ban. Same as vespa-relay's `deleteMissing`.
- **orphans** (edges with no bookkeeping, e.g. the server's consumer before cutover): after a
  complete reconcile, those authors are rebuilt, behind `AUDIT_MAX_DELETES`. A sweep that finds
  none marks the kind `adopted_<kind>` and the scan stops. Not run with `AUDIT_SINCE`. An author
  with report edges and no report ids is also rebuilt on their next report, since applying a
  report only adds.

Every filter carries `search: "include:spam"`: the relay refuses unauthenticated reads (REQ and
NEG-OPEN) that name no web-of-trust lens. A recompute reads
`{"authors":[…],"kinds":[3,10000,10040,1984]}`.

### When absence is trusted

The audit acts on absent events (`AbsenceGuard`) only when all hold:

| guard | why |
|---|---|
| the reconcile **completed** | A window negentropy can't reconcile is paged (the graph still fills) and that kind recomputes nobody. A reconcile that fails outright pages nothing: it is reported incomplete and retried `AUDIT_RETRY_MINUTES` later as a delta. The relay refuses a Vespa answer with partial coverage, so a down content node fails the reconcile instead of making a third of the corpus look deleted. |
| ≥ 1 window compared | Zero windows compared nothing. |
| older than 1 h | The audit stops at now − 1 h: a just-streamed event may not be visible to the relay's id reads yet. |
| ≤ `AUDIT_MAX_DELETES` absent per kind | This relay is a mirror that has lost data before; a mass absence is likelier its fault than a mass retraction. When it fires, the audit logs and changes nothing. Raise it to let a real retraction through. |

### Deletion latency

Every removal takes effect at the next audit (default 24 h), as far as the relay applied it:
kind 5, kind 62 (the relay honours its own URL and `ALL_RELAYS`), NIP-40 expiry, or anything else
the relay drops. A retracted report keeps counting until then.

## Bookkeeping

A list is recorded even when it yields no edge, and so is a note report; otherwise the audit
would download them every time. The `*_at` and `*_event` properties get range indexes
(`graph_streamer_<prop>`), which the audit reads lists through. Reports are few and read whole.

## Redis

Reverse sets get SADD/SREM as edges change; `designates:<author>` is replaced whole in one MULTI.
Written after Neo4j and kept a copy of it two ways:

- **Outbox.** The transaction that writes an author's edges labels the node
  `GraphStreamerRedisPending` and records lost targets (`redis_pending_removed`); the label goes
  once Redis has the change. Startup replays whoever still has it.
- **Reconcile.** Each set's size against its key's edge count in Neo4j; a differing set is
  replaced, a set with no edge deleted, a batch at a time under the writer's lock. Runs when the
  marker `graph_streamer:redis_reconciled_at` is missing (Redis empty, flushed, or built by the
  server's consumer) and every `REDIS_AUDIT_EVERY_HOURS`.

Production runs `allkeys-lru` at `maxmemory 24gb`. At full size these sets would be evicted, and
an evicted set reads as empty to GrapeRank. Size Redis for them or use `volatile-lru` (they carry
no TTL) before cutover.

## State node

`(:GraphStreamerState {name: "graph-streamer"})`, every value a string.

| property | meaning |
|---|---|
| `live_since` | epoch seconds the live stream resumes from after a restart |
| `audit_started_at`, `audit_ended_at`, `audit_complete` | last audit's start, end, whether every kind completed |
| `audit_finished_at` | end of the last complete audit; the next is due `AUDIT_EVERY_HOURS` after it |
| `audit_<kind>_<counter>` | per kind (`follows`, `mutes`, `designations`, `reports`): `need`, `have`, `orphans`, `ignored`, `windows`, `downloaded`, `recomputed`, `complete`, `note` |
| `adopted_<kind>` | `true` once an orphan sweep of that kind found none |
| `redis_reconciled_at`, `redis_keys`, `redis_replaced`, `redis_deleted`, `redis_outbox` | last Redis reconcile and outbox replay |

## Dry run

`DRY_RUN=true` writes nothing to the graph (schema included) or its Redis sets, but keeps the
state node so restarts don't re-run the audit. It logs the changes it would make (`stats:`, every
minute). With no bookkeeping, every dry-run audit downloads everything the relay holds: the
measurement of how far the existing graph is from the relay. Set `AUDIT_EVERY_HOURS` high or `0`
after the first one.

## Cutover

1. Deploy with `DRY_RUN=true` beside the server's consumer. Read the first audit's counts: edges
   it would add and remove per type, and `audit_<kind>_orphans` (capped at
   `AUDIT_MAX_DELETES` + 1), the authors whose edges the relay can't account for.
2. Size or reconfigure Redis (above).
3. In one deploy, stop the server's `strfry:events` consumer for kinds 3, 5, 1984, 10000 and 10040
   and set `DRY_RUN=false`, with `AUDIT_MAX_DELETES` at least the orphan count you accepted. Never
   run two writers on the same edges. The first audit records every author's bookkeeping, writing
   only edges that differ; the sweep rebuilds orphans; the Redis reconcile makes every set match.
   Check with `tools/verify`.
4. Retire neofry's router streams and transferer, `kind3Backfill`, `reportBackfill` and
   `neofryResync`.

Kind 39999 (Trusted Lists taggings) is not handled: its handler writes brainstorm_server's
Postgres tables. That has to be settled before neofry can be retired.

## Rehearsal (2026-11-04)

`DRY_RUN=false`, `AUDIT_SINCE` = 2026-01-01, against search.brainstorm.world, into an empty
Neo4j 5 (heap 3G, page cache 4G) and Redis 7 on 4 cores / 16 GB. Production is about 10× larger.

| | measured |
|---|---|
| first audit, follows | ~460k lists → 27.6M FOLLOWS edges, 720k users. 14k edges/s on small lists, 30k/s on large. ~330 lists/s fetching. |
| first audit, others | mutes 21,556 lists, designations 931, reports 109,334: seconds to a minute each |
| storage | Neo4j store 55 B/edge, tx logs 126 B/edge (2-day retention), Redis 97 B/member, nothing evicted |
| second audit, all kinds | 13 s (`AUDIT_WINDOW` 200k–1M: 5–1 windows), 17 s (50k: 27), 25 s (10k: 101). No window refused. |
| Neo4j side of the audit | 8.46M synthetic lists: 24–30 s at any window size, ~350k pairs/s (`AuditLocalSideBenchmark`) |

Findings:

- MERGE on edges was quadratic in list length; a batch of 500 large lists held a transaction for
  over five minutes. New edges are now created once per author.
- The relay reset the connection once mid-reconcile. That kind was reported incomplete, nothing
  was paged, the retry was a delta.
- The relay holds two kind-3s for some authors (~35 in the slice) and serves both. The older is
  downloaded and not recorded on every audit (`<kind>.ignored`); the newer wins. A relay bug.

Not measured yet:

- The first audit over the whole corpus. Extrapolated: ~220–275M FOLLOWS edges, ~14 GB store,
  ~30 GB transaction logs, ~22–27 GB Redis sets, seven to nine hours. That Redis figure is at
  production's `maxmemory 24gb`: check `used_memory` first.
- Its load on Vespa. Run it when the content nodes have headroom.
