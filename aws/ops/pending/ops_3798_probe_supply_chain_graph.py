#!/usr/bin/env python3
"""ops 3798 — probe the ACTUAL supply-chain-graph schema (no guessing).

Khalid: "supply chain dependency isn't working" — pushing back after I called
153/3411 coverage 'expected sparsity'. He is right to: I asserted the graph
covers few sectors WITHOUT ever opening it. That is the same key-guessing that
caused four bugs in this arc (backlog keys, cap_rows carry, best-setups output
key, refresh ordering). This ops opens the feed and reports what is really there.

Questions it answers, with evidence rather than assertion:
 1. Does data/supply-chain-graph.json exist, and how fresh is it?
 2. What are its ACTUAL top-level keys? Does it even have `edges`/`nodes`?
 3. If edges exist, what are the real field names on an edge? chokepoint reads
    e["supplier"] or e["source"] — if the producer writes e["from"]/e["to"] or
    e["supplier_ticker"], centrality would be built from nothing.
 4. How many DISTINCT symbols does the graph actually name, and how many of
    those overlap the 3,411-row capture ledger?
 5. Is there a better/other supply-chain feed in the fleet we should be using?

Writes no engine code. The fix follows once the schema is known.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3798_probe_supply_chain_graph") as rep:
        rep.heading("ops 3798 — what is actually inside supply-chain-graph.json")

        rep.section("1. Does the feed exist and is it fresh?")
        g = None
        try:
            h = s3.head_object(Bucket=BUCKET, Key="data/supply-chain-graph.json")
            import time as _t
            age_h = (_t.time() - h["LastModified"].timestamp()) / 3600.0
            rep.kv(bytes=h["ContentLength"], age_hours=round(age_h, 1),
                   last_modified=str(h["LastModified"])[:19])
            g = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/supply-chain-graph.json")["Body"].read())
            gate(rep, "FEED.exists", True, "%d bytes, %.1fh old" % (h["ContentLength"], age_h))
            gate(rep, "FEED.fresh", age_h < 24 * 14, "%.1f hours old" % age_h)
        except Exception as e:
            gate(rep, "FEED.exists", False, str(e)[:180])
            g = None

        if g is None:
            rep.warn("Graph feed missing — dependency_pct can never populate. "
                     "Either the producer engine is dead or the key moved.")
            rep.section("Look for the producer / alternatives")
            for k in ("data/supply-chain.json", "data/supplier-graph.json",
                      "data/supply-chain-map.json"):
                try:
                    hh = s3.head_object(Bucket=BUCKET, Key=k)
                    rep.log("  ALT FOUND %-34s %d bytes" % (k, hh["ContentLength"]))
                except Exception:
                    rep.log("  absent    %s" % k)
            rep.fail("no graph")
            sys.exit(1)

        rep.section("2. ACTUAL top-level keys")
        for k, v in g.items():
            rep.log("  %-26s %-8s %s" % (
                k, type(v).__name__,
                len(v) if isinstance(v, (list, dict, str)) else v))

        rep.section("3. Real field names on an edge / node")
        edges = g.get("edges")
        nodes = g.get("nodes")
        rep.kv(edges_present=isinstance(edges, list) and len(edges or []),
               nodes_present=isinstance(nodes, list) and len(nodes or []))
        if isinstance(edges, list) and edges:
            rep.log("  edge[0] keys: %s" % sorted(edges[0].keys()))
            rep.log("  edge[0]     : %s" % json.dumps(edges[0])[:220])
            has_sup = sum(1 for e in edges if e.get("supplier") or e.get("source"))
            gate(rep, "EDGE.reader_matches", has_sup > 0,
                 "%d of %d edges expose supplier|source (what chokepoint reads)" % (
                     has_sup, len(edges)))
        else:
            gate(rep, "EDGE.reader_matches", False, "NO edges list — chokepoint's loop is a no-op")
        if isinstance(nodes, list) and nodes:
            rep.log("  node[0] keys: %s" % sorted(nodes[0].keys()))
            rep.log("  node[0]     : %s" % json.dumps(nodes[0])[:220])
            has_c = sum(1 for n in nodes if isinstance(n.get("centrality"), (int, float)))
            rep.kv(nodes_with_centrality=has_c)

        rep.section("4. Symbol universe of the graph vs the capture ledger")
        syms = set()
        for e in (edges or []):
            for k in ("supplier", "source", "customer", "target", "from", "to",
                      "supplier_ticker", "customer_ticker", "symbol"):
                v = e.get(k)
                if isinstance(v, str) and v.strip():
                    syms.add(v.strip().upper())
        for n in (nodes or []):
            for k in ("symbol", "id", "ticker"):
                v = n.get(k)
                if isinstance(v, str) and v.strip():
                    syms.add(v.strip().upper())
        rep.kv(distinct_symbols_in_graph=len(syms))
        rep.log("  sample: %s" % sorted(list(syms))[:24])

        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        led = {r.get("ticker") for r in rows if r.get("ticker")}
        ov = syms & led
        rep.kv(capture_ledger=len(led), overlap=len(ov))
        gate(rep, "OVERLAP.nonzero", len(ov) > 0, "%d shared symbols" % len(ov))
        rep.log("  overlap sample: %s" % sorted(list(ov))[:24])

        cur = sum(1 for r in rows if r.get("dependency_pct") is not None)
        rep.kv(dependency_populated_today=cur)
        rep.section("VERDICT")
        if len(ov) > cur * 1.5 and len(ov) > 0:
            rep.warn("The graph names %d symbols in the ledger but only %d carry "
                     "dependency_pct — the join or the centrality read is losing "
                     "names. Worth fixing, not explaining away as sparsity." % (
                         len(ov), cur))
        elif len(syms) < 300:
            rep.warn("The graph itself only names %d symbols. dependency_pct cannot "
                     "exceed that. If Khalid expects broad coverage, the GRAPH is the "
                     "thing to expand — the join is fine." % len(syms))
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — schema probed")


if __name__ == "__main__":
    main()
