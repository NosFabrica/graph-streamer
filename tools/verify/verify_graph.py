#!/usr/bin/env python3
"""Read-only check of a graph against the search relay, with an oracle independent of
graph-streamer: brainstorm_server's own parsers (p_tag_pubkeys, extract_report_targets,
parse_designated_pubkeys), loaded from its source.

For a sample of authors, taken from the graph and from the relay (so authors the graph lacks are
found too), it reads everything the relay holds for them and compares, per kind:

  - the edges (FOLLOWS, MUTES, DESIGNATES from the newest list, ties to the lower id, expired
    events skipped; REPORTS from every p-only report),
  - the bookkeeping (the list head the graph records is the relay's newest),
  - the Redis copies (followed_by:/muted_by:/reported_by:<target> hold the author; designates:<author>
    is the DESIGNATES set).

Then, over the whole graph: every Redis reverse set's size against the Neo4j edge counts.

With --since, a kind is compared only for authors whose newest event is from then on, or whose
graph records one: a graph built with AUDIT_SINCE holds nothing older.

Exit code 0 only if nothing differs. Needs: pip install neo4j redis websockets.
"""

import argparse
import ast
import asyncio
import json
import random
import re
import sys
import time
from collections import Counter, defaultdict

import redis
import websockets
from neo4j import GraphDatabase

KINDS = {3: ("FOLLOWS", "follows", "followed_by:"), 10000: ("MUTES", "mutes", "muted_by:"), 10040: ("DESIGNATES", "designations", None)}
REPORTS = 1984
UNRANKED = "include:spam"


def load_server_parsers(server_dir):
    """The three parsing functions and the constants they use, from brainstorm_server's source,
    without importing the app (and its FastAPI, nostr_sdk and settings)."""
    wanted = {
        "app/utils/nostr.py": {"p_tag_pubkeys", "_HEX64"},
        "app/services/report_graph_service.py": {"extract_report_targets"},
        "app/services/designation_service.py": {"parse_designated_pubkeys", "_DESIGNATION_TAG_NAME", "_HEX_PUBKEY"},
    }
    namespace = {"re": re}
    for path, names in wanted.items():
        tree = ast.parse(open(f"{server_dir}/{path}").read())
        found = set()
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                found.add(node.name)
            elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in node.targets):
                found.update(t.id for t in node.targets if isinstance(t, ast.Name))
            else:
                continue
            exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), namespace)
        missing = names - found
        if missing:
            sys.exit(f"{path}: {missing} not found; the server's parsers moved")
    return namespace["p_tag_pubkeys"], namespace["extract_report_targets"], namespace["parse_designated_pubkeys"]


def expiration(event):
    for tag in event["tags"]:
        if len(tag) > 1 and tag[0] == "expiration":
            try:
                return int(tag[1])
            except ValueError:
                return None
    return None


class Relay:
    def __init__(self, url):
        self.url = url
        self.ws = None
        self.sub = 0

    async def __aenter__(self):
        self.ws = await websockets.connect(self.url, max_size=None, open_timeout=30, ping_interval=30)
        return self

    async def __aexit__(self, *exc):
        await self.ws.close()

    async def req(self, flt, timeout=120):
        """Every event for one filter, paging back with until past the relay's cap."""
        out = {}
        flt = dict(flt, search=UNRANKED)
        while True:
            self.sub += 1
            sid = f"v{self.sub}"
            await self.ws.send(json.dumps(["REQ", sid, flt]))
            page = []
            deadline = time.time() + timeout
            while True:
                msg = json.loads(await asyncio.wait_for(self.ws.recv(), max(1, deadline - time.time())))
                if msg[0] == "EVENT" and msg[1] == sid:
                    page.append(msg[2])
                elif msg[0] in ("EOSE", "CLOSED") and msg[1] == sid:
                    if msg[0] == "CLOSED":
                        raise RuntimeError(f"CLOSED {msg[2]} for {flt}")
                    break
            await self.ws.send(json.dumps(["CLOSE", sid]))
            for e in page:
                out[e["id"]] = e
            cap = flt.get("limit", 5000)
            if len(page) < cap or not page:
                return list(out.values())
            flt = dict(flt, until=min(e["created_at"] for e in page) - 1)


async def sample_relay_authors(relay, kind, since, until, n, per=200):
    """Authors of [kind] events at random points in [since, until]; n = 0: every author."""
    if n == 0:
        return list({e["pubkey"] for e in await relay.req({"kinds": [kind], "until": until, "limit": 5000})})
    authors = set()
    tries = 0
    while len(authors) < n and tries < n:
        tries += 1
        at = random.randint(since, until)
        events = await relay.req({"kinds": [kind], "until": at, "since": since, "limit": per})
        authors.update(e["pubkey"] for e in events)
    return list(authors)[:n]


def graph_state(driver, authors):
    with driver.session() as s:
        heads = {
            r["a"]: r
            for r in s.run(
                """UNWIND $authors AS a MATCH (u:NostrUser {pubkey: a})
                   RETURN a, u.follows_event AS follows_event, u.follows_at AS follows_at,
                          u.mutes_event AS mutes_event, u.mutes_at AS mutes_at,
                          u.designations_event AS designations_event, u.designations_at AS designations_at,
                          u.report_events AS report_events""",
                authors=authors,
            )
        }
        edges = defaultdict(lambda: defaultdict(set))
        for r in s.run(
            """UNWIND $authors AS a MATCH (:NostrUser {pubkey: a})-[r:FOLLOWS|MUTES|REPORTS|DESIGNATES]->(t:NostrUser)
               RETURN a, type(r) AS rel, t.pubkey AS t""",
            authors=authors,
        ):
            edges[r["a"]][r["rel"]].add(r["t"])
    return heads, edges


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", default="wss://search.brainstorm.world")
    ap.add_argument("--neo4j", default="bolt://127.0.0.1:7687")
    ap.add_argument("--neo4j-user", default="neo4j")
    ap.add_argument("--neo4j-password", required=True)
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--server-dir", default="../brainstorm_server")
    ap.add_argument("--since", type=int, default=0, help="the AUDIT_SINCE the graph was built with")
    ap.add_argument("--grace", type=int, default=7200, help="ignore events newer than now - grace (not audited yet)")
    ap.add_argument("--graph-sample", type=int, default=1000)
    ap.add_argument("--relay-sample", type=int, default=500, help="authors per kind; 0 = every author the relay holds (small relays only)")
    ap.add_argument("--skip-global", action="store_true", help="skip the whole-graph Redis/Neo4j count check")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--author", action="append", default=[], help="also check this author (repeatable)")
    args = ap.parse_args()
    random.seed(args.seed)

    p_tags, report_targets, designated = load_server_parsers(args.server_dir)
    driver = GraphDatabase.driver(args.neo4j, auth=(args.neo4j_user, args.neo4j_password))
    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    now = int(time.time())
    horizon = now - args.grace
    problems = Counter()
    examples = defaultdict(list)

    def problem(kind, detail):
        problems[kind] += 1
        if len(examples[kind]) < 5:
            examples[kind].append(detail)

    asyncio.run(check(args, p_tags, report_targets, designated, driver, r, now, horizon, problem))

    if not args.skip_global:
        global_counts(driver, r, problem)
        orphans(driver, problem)

    print("\nresult:", "OK, nothing differs" if not problems else "DIFFERENCES")
    for kind, n in problems.most_common():
        print(f"  {kind}: {n}")
        for e in examples[kind]:
            print(f"      {e}")
    sys.exit(1 if problems else 0)


async def check(args, p_tags, report_targets, designated, driver, r, now, horizon, problem):
    with driver.session() as s:
        graph_authors = [
            rec["p"]
            for rec in s.run(
                "MATCH (u:NostrUser) WHERE u.follows_at IS NOT NULL OR u.mutes_at IS NOT NULL OR u.report_events IS NOT NULL OR u.designations_at IS NOT NULL "
                "WITH u, rand() AS x ORDER BY x LIMIT $n RETURN u.pubkey AS p",
                n=args.graph_sample,
            )
        ]
    async with Relay(args.relay) as relay:
        relay_authors = set()
        for kind in (3, 10000, 10040, REPORTS):
            got = await sample_relay_authors(relay, kind, max(args.since, 1609459200), now, args.relay_sample)
            print(f"relay sample kind {kind}: {len(got)} authors")
            relay_authors.update(got)
        authors = list(set(graph_authors) | relay_authors | set(args.author))
        print(f"checking {len(authors)} authors ({len(graph_authors)} from the graph, {len(relay_authors)} from the relay)")

        compared = Counter()
        for i in range(0, len(authors), 100):
            chunk = authors[i : i + 100]
            events = await relay.req({"authors": chunk, "kinds": [3, 10000, 10040, REPORTS], "limit": 5000})
            by_author = defaultdict(list)
            for e in events:
                by_author[e["pubkey"]].append(e)
            heads, edges = graph_state(driver, chunk)
            pipe = r.pipeline()
            redis_checks = []
            for a in chunk:
                mine = [e for e in by_author[a] if (expiration(e) or now + 1) > now]
                head = heads.get(a)
                for kind, (rel, prop, prefix) in KINDS.items():
                    lists = [e for e in mine if e["kind"] == kind]
                    newest = min(lists, key=lambda e: (-e["created_at"], e["id"])) if lists else None
                    recorded = head[f"{prop}_event"] if head else None
                    if newest is not None and newest["created_at"] > horizon:
                        continue  # not audited yet
                    if newest is None and recorded is None:
                        continue
                    if newest is not None and newest["created_at"] < args.since and recorded is None:
                        continue  # outside the slice
                    compared[rel] += 1
                    if newest is None:
                        problem(f"{rel}: graph records a list the relay lacks", f"{a} {recorded}")
                        continue
                    if recorded != newest["id"]:
                        problem(f"{rel}: bookkeeping is not the relay's newest list", f"{a} graph={recorded} relay={newest['id']}@{newest['created_at']}")
                    want = set(p_tags(newest["tags"])) if kind != 10040 else set(designated(newest["tags"], a))
                    have = edges[a][rel]
                    if want != have:
                        problem(f"{rel}: edges differ", f"{a} missing={len(want - have)} extra={len(have - want)}")
                    if prefix:
                        for t in have:
                            pipe.sismember(prefix + t, a)
                            redis_checks.append((f"{prefix}<target> lacks the author", f"{a} -> {t}"))
                    else:
                        pipe.smembers("designates:" + a)
                        redis_checks.append(("designates:<author> differs", (a, have)))
                reports = [e for e in mine if e["kind"] == REPORTS and e["created_at"] <= horizon]
                in_slice = set().union(*[report_targets(e) for e in reports if e["created_at"] >= args.since]) if reports else set()
                every = set().union(*[report_targets(e) for e in reports]) if reports else set()
                have = edges[a]["REPORTS"]
                if reports or have:
                    compared["REPORTS"] += 1
                    if not (in_slice <= have <= every):
                        problem("REPORTS: edges differ", f"{a} missing={len(in_slice - have)} extra={len(have - every)}")
                    for t in have:
                        pipe.sismember("reported_by:" + t, a)
                        redis_checks.append(("reported_by:<target> lacks the author", f"{a} -> {t}"))
            for (label, detail), result in zip(redis_checks, pipe.execute()):
                if label.startswith("designates"):
                    a, have = detail
                    if set(result) != have:
                        problem(label, f"{a} redis={len(result)} graph={len(have)}")
                elif not result:
                    problem(label, detail)
            print(f"  {min(i + 100, len(authors))}/{len(authors)} authors", end="\r", flush=True)
        print(f"\ncompared per kind: {dict(compared)}")


def orphans(driver, problem):
    """Authors with edges of a kind and no bookkeeping for it: edges no audit compares (another
    writer's, or a migration not finished). graph-streamer's own edges always have bookkeeping."""
    checks = {"FOLLOWS": "u.follows_event IS NULL", "MUTES": "u.mutes_event IS NULL", "DESIGNATES": "u.designations_event IS NULL", "REPORTS": "u.report_events IS NULL"}
    with driver.session() as s:
        for rel, unrecorded in checks.items():
            rows = s.run(f"MATCH (u:NostrUser) WHERE {unrecorded} AND EXISTS {{ (u)-[:{rel}]->() }} RETURN u.pubkey AS a LIMIT 1000").value()
            print(f"orphans {rel}: {len(rows):,}{'+' if len(rows) == 1000 else ''} authors with edges and no bookkeeping")
            for a in rows:
                problem(f"orphan: {rel} edges with no bookkeeping", a)


def global_counts(driver, r, problem):
    """Over the whole graph, per target: its in-degree of each edge type in Neo4j against the size
    of its Redis set (per author for designates:), and sets for nodes that have no such edge.
    Per key, so an error on one side can't hide behind an error on the other in a total."""
    pairs = {"FOLLOWS": ("followed_by:", "<-"), "MUTES": ("muted_by:", "<-"), "REPORTS": ("reported_by:", "<-"), "DESIGNATES": ("designates:", "->")}
    for rel, (prefix, direction) in pairs.items():
        pattern = f"(u)<-[:{rel}]-()" if direction == "<-" else f"(u)-[:{rel}]->()"
        degrees = {}
        with driver.session() as s:
            for rec in s.run(f"MATCH (u:NostrUser) WITH u, COUNT {{ {pattern} }} AS n WHERE n > 0 RETURN u.pubkey AS k, n"):
                degrees[rec["k"]] = rec["n"]
        keys = list(degrees)
        sizes = {}
        for i in range(0, len(keys), 10000):
            pipe = r.pipeline()
            for k in keys[i : i + 10000]:
                pipe.scard(prefix + k)
            sizes.update(zip(keys[i : i + 10000], pipe.execute()))
        # The pass reads Neo4j first and Redis after, so on a graph being written a key can look
        # wrong for one write. Re-read each suspect, both sides back to back, before calling it.
        wrong = []
        for k in (k for k in keys if degrees[k] != sizes[k]):
            for attempt in range(3):
                with driver.session() as s:
                    n = s.run(f"MATCH (u:NostrUser {{pubkey: $k}}) RETURN COUNT {{ {pattern} }} AS n", k=k).single()["n"]
                c = r.scard(prefix + k)
                if n == c:
                    break
                time.sleep(2)
            else:
                wrong.append((k, n, c))
        orphan_sets = 0
        for key in r.scan_iter(match=prefix + "*", count=10000):
            if key[len(prefix) :] not in degrees:
                orphan_sets += 1
                problem(f"global: {prefix} set with no {rel} edge in Neo4j", key)
        print(f"global {rel}: {len(keys):,} keys, {sum(degrees.values()):,} edges; {len(wrong):,} sets of the wrong size, {orphan_sets:,} sets with no edge")
        for k, n, c in wrong:
            problem(f"global: {prefix}<key> size != {rel} edges", f"{k} neo4j={n} redis={c}")


if __name__ == "__main__":
    main()
