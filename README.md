# graph-streamer

Keeps Brainstorm's trust graph — follows, mutes, reports and kind-10040 designations in Neo4j,
and their Redis copies — equal to what our search relay (`wss://search.brainstorm.world`)
holds. It replaces the neofry path: neofry's router and the transferer filling a strfry,
the `strfry:events` Redis list, brainstorm_server's `process_strfry_event` consumer, and the
`kind3Backfill` / `reportBackfill` workarounds around them.

**Status: not yet run against the live relay or graph.** Built and unit-tested against Quartz's
real SQLite store (in memory) and fakes for Neo4j and Redis. It defaults to `DRY_RUN=true`.

## How it works

```
search relay ──live REQ──────────▶ ┐
  (source of truth)                 ├─▶ mirror (Quartz SQLite) ─▶ dirty authors ─▶ projector ─▶ Neo4j + Redis
search relay ◀─negentropy audit──▶ ┘
```

1. **Mirror.** A local Quartz `EventStore` holds exactly what the [filters](#what-is-mirrored)
   ask for. The store applies NIP-01 supersession (newest list wins), NIP-09 kind 5 (by id and by
   address, same author only), NIP-62 vanish and NIP-40 expiry on its own.
2. **Live stream.** One long-lived REQ on the search relay. The relay fans its sync's writes out
   to live subscribers, so a new list, report, deletion or vanish lands here as the relay stores it.
   The REQ is re-sent every `LIVE_REFRESH_SECONDS` with `since` = now − `LIVE_OVERLAP_SECONDS`, so a
   reconnect (Quartz re-sends the REQ) re-reads at most that much.
3. **Audit.** Every `AUDIT_EVERY_HOURS`, each filter is reconciled with the relay over NIP-77:
   - *need* (relay has it, we don't): downloaded. This covers late arrivals, missed live events
     and, on the first run, the whole past.
   - *have* (we have it, the relay doesn't): **deleted**, whatever the reason the relay dropped
     it: a kind 5 without a `k` tag, a vanish, an expiry, a ban. The search relay is the source of
     truth, the same way vespa-relay's `deleteMissing` treats a provider's relay.
4. **Projection.** Every write marks its authors dirty first. The projector recomputes each
   dirty author from what the mirror still holds and diffs that against Neo4j. A new list, a
   deletion, a vanish, an expiry and an audit removal are therefore one code path, and replaying
   it is harmless.

### When absence is trusted

The audit deletes only when:

| guard | why |
|---|---|
| the reconcile **completed** | A window negentropy can't reconcile is paged instead (the mirror still fills), and that filter deletes nothing. The relay refuses a Vespa answer with partial coverage (`coverage.full: false`, vespa-eventstore `VespaEventIndex`), so a content node that is down fails the reconcile. It does not make a third of the corpus look deleted. |
| ≥ 1 window compared | Zero windows compared nothing. |
| older than 1 h | The audit stops at `until` = now − 1 h. An event the live stream just delivered may not be visible to the relay's id reads yet. |
| ≤ `AUDIT_MAX_DELETES` per filter | Default 10 000; 0 turns it off. This relay is a mirror that has lost data before, so a mass absence is likelier its fault than a mass retraction. When the guard fires, the audit logs it and keeps the events. Raise the limit to let a real retraction through. |

### Deletion latency

| cause | takes effect |
|---|---|
| kind 5 with a `k` tag naming 3, 1984, 10000 or 10040 | as it streams in |
| kind 62 naming the search relay or `ALL_RELAYS` | as it streams in |
| kind 5 without `k`, or anything else the relay drops (a ban, its own vanish handling) | next audit |
| NIP-40 expiry | the next time the author is projected, at the latest the next audit |

## What is mirrored

| filter | why |
|---|---|
| `{"kinds":[3,10000,10040]}` | follow lists, mute lists, designations (replaceable) |
| `{"kinds":[1984]}` | reports |
| `{"kinds":[5],"#k":["3","1984","10000","10040"]}` | deletions of the above |
| `{"kinds":[62]}` | requests to vanish |

Every filter carries `search: "include:spam"`. The relay refuses unauthenticated reads (REQ and
NEG-OPEN alike) that name no web-of-trust lens, and this token waives one. The local store strips
extension tokens and has full-text search off, so the same filter matches the same set on both
sides (`AuditFiltersTest`).

Kind 39999 (Trusted Lists taggings) is **not** mirrored yet: its handler writes brainstorm_server's
Postgres tables. That has to be settled before neofry can be retired.

## What it writes

These names are a contract with brainstorm_server and the GrapeRank worker, and they are the
same ones the server's consumer writes today.

**Neo4j** — outgoing edges of `(:NostrUser {pubkey})`:

| edge | from | rule |
|---|---|---|
| `FOLLOWS` | newest kind 3 | its `p` tags: lowercased, 64-hex only, deduplicated |
| `MUTES` | newest kind 10000 | public `p` tags, same rule |
| `REPORTS` | every surviving kind 1984 | `p` tags of user reports only (a report with an `e` tag is about a note) |
| `DESIGNATES` | newest kind 10040 | public rows named `<kind>[:<tag>]`, never the author's own key |

**Redis:** the reverse sets `followed_by:<target>`, `muted_by:<target>` and `reported_by:<target>`
(SADD/SREM of the author as their edges change), and the forward set `designates:<author>`
(replaced whole in one MULTI). Neo4j is written first, then Redis.

The service's own keys:

| key | meaning |
|---|---|
| `graph_streamer:dirty` | HASH author → generation: authors waiting to be projected |
| `graph_streamer:audit` | HASH: the last audit's start and finish, and per filter `need`, `have`, `downloaded`, `deleted`, `complete`, `note` |

An author is marked dirty before their events are written, under a lock the projector also
takes to read. The mark is cleared after their edges are written, and only if its generation
is still the one the projector read. A crash at any point leaves the author marked.

## Running

```sh
./gradlew test
./gradlew run   # needs the environment below
```

| variable | default | |
|---|---|---|
| `NEO4J_URL`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` | — | same names as the GrapeRank worker |
| `REDIS_HOST`, `REDIS_PORT` | — | |
| `SEARCH_RELAY_URL` | `wss://search.brainstorm.world` | also the store's NIP-62 scope |
| `STORE_PATH` | `/data/graph-streamer.db` | on a volume; losing it means a full re-download |
| `DRY_RUN` | `true` | `false` writes the graph; anything not a switch stops the boot |
| `AUDIT_EVERY_HOURS` | `24` | `0` turns the audit off |
| `AUDIT_WINDOW` | `50000` | events per reconcile window |
| `AUDIT_MAX_DELETES` | `10000` | per filter per audit; `0` = no limit |
| `LIVE_REFRESH_SECONDS` | `600` | |
| `LIVE_OVERLAP_SECONDS` | `1800` | |
| `PROJECT_BATCH` | `200` | authors per projection batch |

It runs as one long-lived replica: the mirror is a single SQLite file, and the dirty set assumes
one projector. It needs the `NostrUser.pubkey` uniqueness constraint that brainstorm_server
creates. CI builds `ghcr.io/nosfabrica/graph-streamer` on every push (branch, `sha-<short>`, and
`latest` on `main`).

## Cutover

1. Deploy with `DRY_RUN=true` beside the server's consumer and let the first audit fill the mirror.
   The `stats:` log line and `graph_streamer:audit` report the edge diffs it *would* write.
2. Read those diffs. In dry run they measure exactly how far the neofry-built graph is from the
   search relay's view.
3. In one deploy, stop the server's `strfry:events` consumer for kinds 3, 5, 1984, 10000 and 10040
   and set `DRY_RUN=false`. Never run two writers on the same edges.
4. Retire neofry's router streams and the transferer, along with `kind3Backfill`, `reportBackfill`
   and `neofryResync`. Kind 39999 is still open.

## Not measured yet

- The first audit downloads every follow list the relay holds, by id, through the relay. Its
  load on Vespa, and how long it takes, are unknown. Run it when the content nodes have headroom.
- Whether vespa-relay reconciles a NEG-OPEN with a tag filter (`#k`). If not, that filter is
  paged every audit and never deletes.
- The mirror's size on disk. `p` tags of kinds 3 and 10000 are not indexed, which is most of it.
