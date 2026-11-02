# graph-streamer

Keeps Brainstorm's trust graph equal to what our search relay (`wss://search.brainstorm.world`)
holds: follows, mutes, reports and kind-10040 designations in Neo4j, and their Redis copies.
It replaces the neofry path:
- neofry's router and the transferer filling a strfry;
- the `strfry:events` Redis list;
- brainstorm_server's `process_strfry_event` consumer;
- the `kind3Backfill` and `reportBackfill` workarounds around them.

**Status: not yet run against the live relay or graph.** Unit-tested with fakes for Neo4j, Redis
and the relay; the Cypher has not run against a real Neo4j yet. It defaults to `DRY_RUN=true`.

It keeps no copy of the events. The relay is the store. The graph records, on each author's
node, which events its edges came from, and that is what the audit compares with the relay.

## How it works

```
                 ┌─ list / report ──────────▶ apply (newer list replaces; report adds)  ─┐
search relay ────┤                                                                       ├─▶ Neo4j + Redis
  (live REQ)     └─ kind 5 / kind 62 ─▶ mark author dirty ─▶ recompute from the relay  ─┘
                                                 ▲
search relay ◀──── daily negentropy audit ───────┘  (need: download + apply; have: mark dirty)
```

1. **Live stream.** One long-lived REQ on the search relay. The relay fans its sync's writes out
   to live subscribers, so each new list, report, deletion and vanish arrives as the relay stores
   it. The REQ is re-sent every `LIVE_REFRESH_SECONDS` with `since` = now − `LIVE_OVERLAP_SECONDS`, so a
   reconnect (Quartz re-sends the REQ) re-reads at most that much. Re-applying an event writes nothing.
2. **Apply.** A list (kind 3, 10000 or 10040) replaces the one the author's node records only
   if it is newer: later `created_at`, or the lower id on the same second. Its edges are diffed
   in, and its id and `created_at` are recorded. A report adds its targets and records its id.
   Nothing is ever *removed* by applying an event.
3. **Recompute.** A kind 5 or kind 62 is only a trigger: the relay has already applied it. Its
   author is marked dirty, and the recomputer rebuilds dirty authors from everything the relay
   holds for them now (batched, `PROJECT_BATCH` per read), diffing against the graph. This is the
   only way edges leave the graph. A relay read that doesn't finish changes nothing, and the author
   stays dirty.
4. **Audit.** Every `AUDIT_EVERY_HOURS`, each kind is reconciled with the relay over NIP-77.
   Our side is read from the graph's bookkeeping:
   - *need* (the relay has it, the graph doesn't): downloaded and applied. This covers late
     arrivals, missed live events and, on the first run, the whole past.
   - *have* (the graph has it, the relay doesn't): the author is marked dirty and recomputed, for
     whatever reason the relay dropped it: a kind 5 without a `k` tag, an expiry, a vanish, a ban.
     The search relay is the source of truth, the same way vespa-relay's `deleteMissing` treats a
     provider's relay.

Every graph write is read, compute, write under one lock, so applies and recomputes never
interleave on an author.

### When absence is trusted

An audit marks the authors of absent events only when:

| guard | why |
|---|---|
| the reconcile **completed** | A window negentropy can't reconcile is paged instead (the graph still fills), and that kind recomputes nobody. The relay refuses a Vespa answer with partial coverage (`coverage.full: false`, vespa-eventstore `VespaEventIndex`), so a content node that is down fails the reconcile. It does not make a third of the corpus look deleted. |
| ≥ 1 window compared | Zero windows compared nothing. |
| older than 1 h | The audit stops at `until` = now − 1 h. An event the live stream just delivered may not be visible to the relay's id reads yet. |
| ≤ `AUDIT_MAX_DELETES` absent per kind | Default 10 000; 0 turns it off. This relay is a mirror that has lost data before, so a mass absence is likelier its fault than a mass retraction. When the guard fires, the audit logs it and changes nothing. Raise the limit to let a real retraction through. |

### Deletion latency

| cause | takes effect |
|---|---|
| kind 5 with a `k` tag naming 3, 1984, 10000 or 10040 | as it streams in (next recompute batch) |
| kind 62 | as it streams in, as far as the relay applied it (it honours its own URL and `ALL_RELAYS`) |
| kind 5 without `k`, NIP-40 expiry, anything else the relay drops | next audit |

## What it reads

| live filter | why |
|---|---|
| `{"kinds":[3,10000,10040,1984]}` | follow lists, mute lists, designations, reports |
| `{"kinds":[5],"#k":["3","10000","10040","1984"]}` | deletions of those: triggers a recompute |
| `{"kinds":[62]}` | requests to vanish: triggers a recompute |

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
(replaced whole in one MULTI). Written after Neo4j.

The service's own keys:

| key | meaning |
|---|---|
| `graph_streamer:dirty` | HASH author → generation: authors waiting to be recomputed from the relay |
| `graph_streamer:audit` | HASH: the last audit's start and finish, and per kind `need`, `have`, `downloaded`, `recomputed`, `complete`, `note` |

A dirty mark is cleared only after the author is written, and only if its generation is still
the one the recomputer read. A crash, or a kind 5 that arrives mid-recompute, leaves it marked.

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
| `AUDIT_WINDOW` | `50000` | lists per reconcile window |
| `AUDIT_MAX_DELETES` | `10000` | absent events per kind per audit; `0` = no limit |
| `LIVE_REFRESH_SECONDS` | `600` | |
| `LIVE_OVERLAP_SECONDS` | `1800` | |
| `PROJECT_BATCH` | `200` | authors per recompute (one relay read) |

It is stateless and runs as one replica, because it must be the only writer of these edges. It
needs the `NostrUser.pubkey` uniqueness constraint that brainstorm_server creates. CI builds
`ghcr.io/nosfabrica/graph-streamer` on every push (branch, `sha-<short>`, and `latest` on `main`).

### Dry run

`DRY_RUN=true` writes nothing to Neo4j, schema included, and nothing to the graph's Redis sets.
It only counts the changes it would make (`stats:` log line, every minute). Because it records
no bookkeeping, every dry-run audit compares against an empty side and downloads everything the
relay holds. That is the measurement you want once: how far the neofry-built graph is from the
relay. Set `AUDIT_EVERY_HOURS` high, or to 0, after the first one.

## Cutover

1. Deploy with `DRY_RUN=true` beside the server's consumer. Read the first audit's counts: the
   edges it would add and remove, per type.
2. In one deploy, stop the server's `strfry:events` consumer for kinds 3, 5, 1984, 10000 and 10040
   and set `DRY_RUN=false`. Never run two writers on the same edges. The first audit then records
   every author's bookkeeping, writing only the edges that differ.
3. Retire neofry's router streams and the transferer, along with `kind3Backfill`, `reportBackfill`
   and `neofryResync`. Kind 39999 is still open.

## Not measured yet

- The first audit downloads every follow list the relay holds, by id, through the relay. Its
  load on Vespa, and how long it takes, are unknown. Run it when the content nodes have headroom.
- Whether vespa-relay reconciles a one-kind NEG-OPEN at this size without windows being refused.
  Refused windows are paged, which fills the graph but can't detect deletions.
- The cost of the bookkeeping writes on top of the edge writes, and of the six range indexes.
