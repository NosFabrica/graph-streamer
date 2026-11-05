#!/usr/bin/env python3
"""Builds a graph the way production's was built: every event, in the order given, through
brainstorm_server's own consumer (process_strfry_event), into Neo4j and Redis. The input to a
migration rehearsal: graph-streamer then takes over this graph, and tools/verify checks the result.

Run with a Python that has brainstorm_server's dependencies (pyproject.toml), from anywhere:

  server_build.py --server-dir ../brainstorm_server --events events.jsonl \
      --neo4j bolt://127.0.0.1:7688 --neo4j-password ... --redis-port 6380 --relay ws://127.0.0.1:PORT

--relay is what the server's kind-5 handler reads reports back from (NOSTR_TRANSFER_TO_RELAY).
"""

import argparse
import asyncio
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-dir", required=True)
    ap.add_argument("--events", required=True, help="one event JSON per line, in arrival order")
    ap.add_argument("--neo4j", required=True)
    ap.add_argument("--neo4j-user", default="neo4j")
    ap.add_argument("--neo4j-password", required=True)
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", required=True)
    ap.add_argument("--relay", required=True)
    args = ap.parse_args()

    # The settings the consumer's modules read at import; the rest only need to parse.
    env = {
        "NEO4J_DB_URL": args.neo4j,
        "NEO4J_DB_USERNAME": args.neo4j_user,
        "NEO4J_DB_PASSWORD": args.neo4j_password,
        "REDIS_HOST": args.redis_host,
        "REDIS_PORT": str(args.redis_port),
        "NOSTR_TRANSFER_TO_RELAY": args.relay,
        "NOSTR_TRANSFER_FROM_RELAY": args.relay,
        "NOSTR_UPLOAD_TA_EVENTS_RELAY": args.relay,
        "NOSTR_UPLOAD_TA_EVENTS_RELAY_PUBLIC_URL": args.relay,
        "DB_URL": "postgresql+asyncpg://unused:unused@127.0.0.1:1/unused",
        "DEPLOY_ENVIRONMENT": "rehearsal",
        "AUTH_ALGORITHM": "HS256",
        "AUTH_SECRET_KEY": "rehearsal",
        "AUTH_ACCESS_TOKEN_EXPIRE_MINUTES": "60",
        "SQL_ADMIN_USERNAME": "x",
        "SQL_ADMIN_PASSWORD": "x",
        "SQL_ADMIN_SECRET_KEY": "x",
        "CUTOFF_OF_VALID_GRAPERANK_SCORES": "0.02",
        "PERFORM_NOSTR_FULL_SYNC": "false",
        "FRONTEND_URL": "http://localhost",
        "PUBLIC_BASE_URL": "http://localhost",
    }
    for k, v in env.items():
        os.environ.setdefault(k, v)
    # No .env from the server checkout: only the values above.
    os.chdir("/")
    sys.path.insert(0, os.path.abspath(args.server_dir))
    asyncio.run(build(args))


async def build(args):
    from neo4j import AsyncGraphDatabase

    from app.message_queue_tasks.process_strfry_event import create_pubkey_index, process_strfry_event

    driver = AsyncGraphDatabase.driver(args.neo4j, auth=(args.neo4j_user, args.neo4j_password))
    applied = 0
    async with driver.session() as session:
        await create_pubkey_index(session)
        with open(args.events) as f:
            for line in f:
                if line.strip():
                    await process_strfry_event(session, json.loads(line))
                    applied += 1
    await driver.close()
    print(f"server consumer applied {applied} events")


if __name__ == "__main__":
    main()
