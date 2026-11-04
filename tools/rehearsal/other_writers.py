#!/usr/bin/env python3
"""The other writers on the graph's nodes, as production runs them, for --seconds: the server's
GrapeRank write-back (its own set_observer_scores, 5,000-row managed transactions of score
properties) and the profile resolver's lightning and NIP-05 writes (its Cypher, verbatim: properties
and ZAPS_VIA edges). Each round takes the nodes as they are now, so it races graph-streamer's
writes on the same nodes. Prints how many batches each writer committed and failed.

Run with a Python that has brainstorm_server's dependencies:

  other_writers.py --server-dir ../brainstorm_server --neo4j bolt://127.0.0.1:7688 \
      --neo4j-password ... --seconds 120
"""

import argparse
import asyncio
import os
import random
import sys
import time

LIGHTNING = """
UNWIND $rows AS r
MATCH (u:NostrUser {pubkey: r.pubkey})
SET u.lnurl = r.lnurl, u.lnurl_status = r.status
WITH u, r
OPTIONAL MATCH (u)-[old:ZAPS_VIA]->(s:NostrUser)
WHERE r.signer IS NULL OR s.pubkey <> r.signer
DELETE old
WITH DISTINCT u, r
WHERE r.signer IS NOT NULL
MERGE (signer:NostrUser {pubkey: r.signer})
MERGE (u)-[:ZAPS_VIA]->(signer)
"""

NIP05 = """
UNWIND $rows AS r
MATCH (u:NostrUser {pubkey: r.pubkey})
SET u.nip05 = r.nip05, u.nip05_status = r.status, u.nip05_checked_at = r.checked_at
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-dir", required=True)
    ap.add_argument("--neo4j", required=True)
    ap.add_argument("--neo4j-user", default="neo4j")
    ap.add_argument("--neo4j-password", required=True)
    ap.add_argument("--seconds", type=int, default=120)
    args = ap.parse_args()
    for k, v in {
        "NEO4J_DB_URL": args.neo4j, "NEO4J_DB_USERNAME": args.neo4j_user, "NEO4J_DB_PASSWORD": args.neo4j_password,
        "REDIS_HOST": "127.0.0.1", "REDIS_PORT": "1", "NOSTR_TRANSFER_TO_RELAY": "ws://127.0.0.1:1",
        "NOSTR_TRANSFER_FROM_RELAY": "ws://127.0.0.1:1", "NOSTR_UPLOAD_TA_EVENTS_RELAY": "ws://127.0.0.1:1",
        "NOSTR_UPLOAD_TA_EVENTS_RELAY_PUBLIC_URL": "ws://127.0.0.1:1", "DB_URL": "postgresql+asyncpg://u:u@127.0.0.1:1/u",
        "DEPLOY_ENVIRONMENT": "rehearsal", "AUTH_ALGORITHM": "HS256", "AUTH_SECRET_KEY": "x",
        "AUTH_ACCESS_TOKEN_EXPIRE_MINUTES": "60", "SQL_ADMIN_USERNAME": "x", "SQL_ADMIN_PASSWORD": "x",
        "SQL_ADMIN_SECRET_KEY": "x", "CUTOFF_OF_VALID_GRAPERANK_SCORES": "0.02", "PERFORM_NOSTR_FULL_SYNC": "false",
        "FRONTEND_URL": "http://localhost", "PUBLIC_BASE_URL": "http://localhost",
    }.items():
        os.environ.setdefault(k, v)
    os.chdir("/")
    sys.path.insert(0, os.path.abspath(args.server_dir))
    asyncio.run(run(args))


async def run(args):
    from neo4j import AsyncGraphDatabase

    from app.repos.user_repo import set_observer_scores

    driver = AsyncGraphDatabase.driver(args.neo4j, auth=(args.neo4j_user, args.neo4j_password))
    deadline = time.time() + args.seconds
    stats = {"scores_ok": 0, "scores_failed": 0, "resolver_ok": 0, "resolver_failed": 0}
    observer = "f" * 64

    async def pubkeys():
        async with driver.session() as s:
            result = await s.run("MATCH (u:NostrUser) RETURN u.pubkey AS p")
            return [r["p"] async for r in result]

    async def graperank():
        while time.time() < deadline:
            keys = await pubkeys()
            random.shuffle(keys)
            for i in range(0, len(keys), 5000):
                rows = [{"pubkey": p, "props": {f"influence_{observer}": random.random(), f"hops_{observer}": random.randint(1, 6)}} for p in keys[i : i + 5000]]
                try:
                    async with driver.session() as s:
                        await set_observer_scores(s, rows)
                    stats["scores_ok"] += 1
                except Exception as e:
                    stats["scores_failed"] += 1
                    print("graperank write failed:", type(e).__name__, str(e)[:200])
                if time.time() >= deadline:
                    break
            await asyncio.sleep(0.2)

    async def resolver():
        while time.time() < deadline:
            keys = await pubkeys()
            random.shuffle(keys)
            for i in range(0, min(len(keys), 20000), 500):
                chunk = keys[i : i + 500]
                ln = [{"pubkey": p, "lnurl": "lnurl1x", "status": "OK", "signer": random.choice(keys) if random.random() < 0.5 else None} for p in chunk]
                nip = [{"pubkey": p, "nip05": "x@example.com", "status": "VALID", "checked_at": int(time.time())} for p in chunk]
                try:
                    async with driver.session() as s:
                        await s.execute_write(lambda tx: _run(tx, LIGHTNING, ln))
                        await s.execute_write(lambda tx: _run(tx, NIP05, nip))
                    stats["resolver_ok"] += 1
                except Exception as e:
                    stats["resolver_failed"] += 1
                    print("resolver write failed:", type(e).__name__, str(e)[:200])
                if time.time() >= deadline:
                    break
            await asyncio.sleep(0.2)

    await asyncio.gather(graperank(), resolver())
    await driver.close()
    print("other writers:", stats)


async def _run(tx, cypher, rows):
    result = await tx.run(cypher, rows=rows)
    await result.consume()


if __name__ == "__main__":
    main()
