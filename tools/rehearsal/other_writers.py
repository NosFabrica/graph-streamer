#!/usr/bin/env python3
"""The other writer on the graph's nodes, as production runs it with SCORES_IN_POSTGRES, for
--seconds: the profile resolver's lightning and NIP-05 writes (its Cypher, verbatim: properties,
ZAPS_VIA edges, and a signer node numbered by the id rule). Scores are no longer written to
Neo4j, so there is no score writer here. Each round takes the nodes as they are now, so it races
graph-streamer's writes on the same nodes; signers are also drawn from --signers, pubkeys
graph-streamer is about to create, so both sides ask nostr_user for the same new ids.
Prints how many batches committed and failed, and how many signers held another id.

Run with a Python that has brainstorm_server's dependencies:

  other_writers.py --server-dir ../brainstorm_server --neo4j bolt://127.0.0.1:7688 \
      --neo4j-password ... --scorecard-db-url postgresql+asyncpg://USER:PASSWORD@127.0.0.1:5441/scorecard \
      --signers pubkeys.txt --seconds 120
"""

import argparse
import asyncio
import os
import random
import sys
import time

# brainstorm_profile_resolver's LIGHTNING_WITH_IDS (store/Neo4jGraphStore.kt).
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
FOREACH (_ IN CASE WHEN signer.user_id IS NULL THEN [1] ELSE [] END | SET signer.user_id = r.signer_id)
MERGE (u)-[:ZAPS_VIA]->(signer)
RETURN collect(DISTINCT CASE WHEN signer.user_id <> r.signer_id THEN signer.pubkey END) AS mismatched
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
    ap.add_argument("--scorecard-db-url", required=True, help="where nostr_user lives")
    ap.add_argument("--signers", help="file of pubkeys graph-streamer will create, one per line")
    ap.add_argument("--seconds", type=int, default=120)
    args = ap.parse_args()
    for k, v in {
        "NEO4J_DB_URL": args.neo4j, "NEO4J_DB_USERNAME": args.neo4j_user, "NEO4J_DB_PASSWORD": args.neo4j_password,
        "REDIS_HOST": "127.0.0.1", "REDIS_PORT": "1", "NOSTR_TRANSFER_TO_RELAY": "ws://127.0.0.1:1",
        "NOSTR_TRANSFER_FROM_RELAY": "ws://127.0.0.1:1", "NOSTR_UPLOAD_TA_EVENTS_RELAY": "ws://127.0.0.1:1",
        "NOSTR_UPLOAD_TA_EVENTS_RELAY_PUBLIC_URL": "ws://127.0.0.1:1", "DB_URL": "postgresql+asyncpg://u:u@127.0.0.1:1/u",
        "SCORECARD_DB_URL": args.scorecard_db_url, "USER_IDS_ENABLED": "true", "SCORES_IN_POSTGRES": "true",
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

    # The same statement the resolver runs: the table is the contract.
    from app.repos.user_id_repo import ids_for

    driver = AsyncGraphDatabase.driver(args.neo4j, auth=(args.neo4j_user, args.neo4j_password))
    deadline = time.time() + args.seconds
    stats = {"resolver_ok": 0, "resolver_failed": 0, "id_mismatches": 0}
    upcoming = [line.strip() for line in open(args.signers) if line.strip()] if args.signers else []

    async def pubkeys():
        async with driver.session() as s:
            result = await s.run("MATCH (u:NostrUser) RETURN u.pubkey AS p")
            return [r["p"] async for r in result]

    while time.time() < deadline:
        keys = await pubkeys()
        random.shuffle(keys)
        signers = keys + upcoming
        for i in range(0, min(len(keys), 20000), 500):
            chunk = keys[i : i + 500]
            ln = [{"pubkey": p, "lnurl": "lnurl1x", "status": "OK", "signer": random.choice(signers) if random.random() < 0.5 else None} for p in chunk]
            nip = [{"pubkey": p, "nip05": "x@example.com", "status": "VALID", "checked_at": int(time.time())} for p in chunk]
            try:
                ids = await ids_for([r["signer"] for r in ln if r["signer"]])
                for r in ln:
                    r["signer_id"] = ids.get(r["signer"])
                async with driver.session() as s:
                    mismatched = await s.execute_write(lambda tx: _run(tx, LIGHTNING, ln))
                    await s.execute_write(lambda tx: _run(tx, NIP05, nip))
                stats["id_mismatches"] += len(mismatched)
                stats["resolver_ok"] += 1
            except Exception as e:
                stats["resolver_failed"] += 1
                print("resolver write failed:", type(e).__name__, str(e)[:200])
            if time.time() >= deadline:
                break
        await asyncio.sleep(0.2)

    await driver.close()
    print("other writers:", stats)


async def _run(tx, cypher, rows):
    result = await tx.run(cypher, rows=rows)
    records = [r async for r in result]
    return records[0]["mismatched"] if records else []


if __name__ == "__main__":
    main()
