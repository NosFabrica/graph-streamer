# End-to-end suite

`./gradlew e2eTest` runs the installed service (`installDist`) as its own process against a relay
we control, a real Neo4j and a real Redis, and checks every result with `tools/verify`, which
uses brainstorm_server's parsers, not ours. It is not part of `./gradlew test`.

## What it runs against

- **The relay**: Quartz's `NostrServer` over its SQLite store, served by Ktor
  (`e2e/Harness.kt`, `LocalRelay`). NIP-01, NIP-09, NIP-40, NIP-45, NIP-62 and NIP-77. It can be
  stopped and started mid-audit; its store is a file, so events survive. It drops the
  `include:spam` token from filters, which the search relay reads as "no lens".
- **Neo4j 5 and Redis 7** in Docker, wiped before each test. Use scratch containers: the suite
  deletes everything in them. The default Neo4j port, 7688, is also where the one-click stack
  publishes its Neo4j: with that stack up, give the scratch containers other ports
  (`GS_E2E_NEO4J`, `GS_E2E_REDIS_PORT`).
- **Postgres 16** in Docker as the Scorecard database. The suite creates `nostr_user` there
  (the server's migration, verbatim) and empties it before each test.
- **The oracle**: `tools/verify/verify_graph.py` over every author either side holds, plus the
  whole-graph checks (every Redis set against its key's edges, no edge without bookkeeping).
  It expects every set named by User id, read from the nodes' `user_id`.

## Running it

```bash
docker run -d --name neo4j-e2e --network host -e NEO4J_AUTH=neo4j/testpassword123 \
  -e NEO4J_server_bolt_listen__address=:7688 -e NEO4J_server_http_listen__address=:7475 \
  -e NEO4J_server_memory_heap_max__size=1G -e NEO4J_server_memory_pagecache_size=512M neo4j:5
```

```bash
docker run -d --name redis-e2e --network host redis:7-alpine redis-server --port 6380 --save "" --appendonly no
```

```bash
docker run -d --name pg-e2e -e POSTGRES_PASSWORD=testpassword123 -e POSTGRES_DB=scorecard -p 127.0.0.1:5441:5432 postgres:16
```

```bash
python3 -m venv /tmp/verify-venv && /tmp/verify-venv/bin/pip install neo4j redis websockets
```

```bash
GS_E2E=1 GS_E2E_PYTHON=/tmp/verify-venv/bin/python BRAINSTORM_SERVER_DIR=../brainstorm_server ./gradlew e2eTest
```

The migration and concurrent-writer scenarios also need a Python with brainstorm_server's
dependencies (its `pyproject.toml`), as `GS_E2E_SERVER_PYTHON`; without it they are skipped.

| Variable | Default | |
|---|---|---|
| `GS_E2E` | unset | `1` runs the suite |
| `GS_E2E_NEO4J` | `bolt://127.0.0.1:7688` | |
| `GS_E2E_NEO4J_PASSWORD` | `testpassword123` | |
| `GS_E2E_NEO4J_CONTAINER` | `neo4j-e2e` | restarted by one scenario |
| `GS_E2E_REDIS_PORT` | `6380` | |
| `GS_E2E_REDIS_CONTAINER` | `redis-e2e` | restarted by one scenario |
| `GS_E2E_PG_HOST`, `GS_E2E_PG_PORT` | `127.0.0.1`, `5441` | |
| `GS_E2E_PG_DB`, `GS_E2E_PG_USER`, `GS_E2E_PG_PASSWORD` | `scorecard`, `postgres`, `testpassword123` | |
| `GS_E2E_PYTHON` | `python3` | with `neo4j redis websockets` |
| `GS_E2E_SERVER_PYTHON` | unset | with brainstorm_server's dependencies |
| `BRAINSTORM_SERVER_DIR` | `../brainstorm_server` | the oracle's parsers, the server's consumer |

Each test's service log is in `build/e2e-logs/<test>.log`.

## Scenarios

| Test | What it proves |
|---|---|
| `aRebuildMatchesTheRelay` | Every shape of list and report (a 5,000-follow list, malformed and duplicate `p` tags, an empty list, a replaced list, an expired one, self-designation, note reports) ends as the oracle says; the first audit's windows are bounded by the relay's count, not one window. |
| `liveEventsArriveWithoutAnAudit` | Lists, mutes, reports and designations published while running reach the graph and Redis through the live stream. |
| `aRestartResumesTheLiveStream` | A list published while the service was down arrives on restart, without an audit. |
| `everyDeletionReachesTheGraphAtTheNextAudit` | Kind 5 by `e` with and without `k`, by `a`, a kind-62 vanish, an expiry, a deleted report: each author's edges go at the next audit. |
| `aMassAbsenceIsKeptUntilAllowed` | 25 deleted lists over `AUDIT_MAX_DELETES=10` change nothing; with the guard off they go. |
| `aRelayOutageMidReconcileIsRetriedNotPaged` | The relay down until the audit gives up: the kind is reported incomplete, nothing is paged, the retry `AUDIT_RETRY_MINUTES` later completes. |
| `killedMidWriteItRecovers` | SIGKILL four times during the first audit: after a restart, Neo4j and Redis are exact (the Redis outbox). |
| `aNeo4jRestartMidAuditIsSurvived` | Neo4j restarted during the first audit: the process stays up and the next complete audit is exact. |
| `aLostRedisIsRebuilt` | Redis restarted empty: the reconcile rebuilds every set from Neo4j. |
| `whileIngestIsHeldNothingIsWrittenAndNothingIsLost` | With `ingest:hold` set, two lists published live change nothing; once the key is deleted the newer one is in the graph and Redis. |
| `whileTheSetsAreNamedByPubkeyNothingIsWritten` | Started while the pubkey naming is claimed: no node, no set; the first audit runs once the claim is `ids`. |
| `aDriftedRedisIsReconciled` | Evicted sets, extra and missing members, sets with no edge: the reconcile repairs each. |
| `everyNodeARebuildMakesHasItsId` | After a first audit every node carries the id `nostr_user` gives its pubkey, and no id is on two nodes. |
| `aNewTargetGetsAnIdAndAReplayChangesNothing` | Targets new to the graph get ids through the live stream; a full audit afterwards moves no id and draws none. |
| `aNodeAnotherWriterNumberedKeepsItsId` | A node the server numbered first keeps that id. |
| `twoIdCallsRacingAgree` | Eight concurrent id calls for the same new pubkeys return the same ids. |
| `aDryRunNeedsNoScorecardDatabase`, `writingWithoutAScorecardDatabaseRefusesToStart` | A dry run starts without the connection and asks for no ids; write mode without it stops at boot. |
| `aServerBuiltGraphMigrates` | A graph built by the server's consumer (stale versions applied last, lists and reports the relay never had, a list deletion it never applied) becomes exactly the relay's, every kind adopted. |
| `otherWritersOnTheSameNodes` | The GrapeRank write-back and the profile resolver writing the same nodes for two minutes: no failed batch on either side, an exact graph. |
| `aDryRunWritesNothing` | `DRY_RUN=true`: one state node, no edge, an empty Redis. |

Each fix these scenarios drove was checked by disabling it and watching its scenario fail.
