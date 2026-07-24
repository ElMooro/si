#!/usr/bin/env python3
"""ops 3803 — probe justhodl-supply-chain-linkage as a wider dependency source.

Khalid: "a lot of supply chain dependency say unmapped" — correct, and
relabelling was the right first step but not the fix. The ceiling is the
CURATED graph: justhodl-supply-chain-graph names ~185 symbols / 303 edges, so
~94% of a 3,000-name board can never populate.

AUDIT-FIRST (do not build what exists): the fleet already has a SECOND engine,
justhodl-supply-chain-linkage, whose docstring says it builds customer/supplier
maps from FMP's /stable/supply-chain-by-symbol endpoint rather than a hand-
curated edge list. If that feed covers materially more symbols, the fix is to
JOIN IT — not to expand a curated list by hand, and not to loosen the peer floor
that (correctly) killed the fake AMZN=100% prints.

This ops opens the linkage feed and answers, with evidence:
  1. Does data/supply-chain-linkage.json exist and how fresh is it?
  2. Its ACTUAL schema — top-level keys, per-row field names (no guessing; key
     mistakes have caused five bugs in this arc).
  3. How many DISTINCT symbols does it name, and how many overlap the capture
     ledger — i.e. what would coverage become if joined?
  4. Does it carry directional supplier/customer relationships, which is what a
     dependency share needs, or only aggregate risk scores?

Writes no engine code.
"""
import sys, json, time
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
    with report("3803_probe_linkage_coverage") as rep:
        rep.heading("ops 3803 — can supply-chain-linkage widen dependency coverage?")

        rep.section("1. Feed presence + freshness")
        L = None
        try:
            h = s3.head_object(Bucket=BUCKET, Key="data/supply-chain-linkage.json")
            age = (time.time() - h["LastModified"].timestamp()) / 3600.0
            rep.kv(bytes=h["ContentLength"], age_hours=round(age, 1),
                   last_modified=str(h["LastModified"])[:19])
            L = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/supply-chain-linkage.json")["Body"].read())
            gate(rep, "LINK.exists", True, "%d bytes" % h["ContentLength"])
            gate(rep, "LINK.fresh", age < 24 * 21, "%.1fh old" % age)
        except Exception as e:
            gate(rep, "LINK.exists", False, str(e)[:160])

        if L is None:
            rep.warn("linkage feed absent — the only path is expanding the curated "
                     "graph engine itself, which is a separate build.")
            rep.fail("no linkage feed")
            sys.exit(1)

        rep.section("2. ACTUAL schema")
        for k, v in L.items():
            rep.log("  %-28s %-8s %s" % (k, type(v).__name__,
                    len(v) if isinstance(v, (list, dict, str)) else v))

        rep.section("3. Symbol universe")
        syms = set()

        def harvest(o, depth=0):
            if depth > 4:
                return
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ("ticker", "symbol", "supplier", "customer",
                             "supplier_ticker", "customer_ticker") and isinstance(v, str):
                        if 1 <= len(v.strip()) <= 6 and v.strip().isupper():
                            syms.add(v.strip())
                    harvest(v, depth + 1)
            elif isinstance(o, list):
                for x in o[:4000]:
                    harvest(x, depth + 1)

        harvest(L)
        rep.kv(distinct_symbols_in_linkage=len(syms))
        rep.log("  sample: %s" % sorted(list(syms))[:26])

        rep.section("4. Directional relationships present?")
        found = {}
        def scan(o, depth=0):
            if depth > 4:
                return
            if isinstance(o, dict):
                ks = set(o.keys())
                for probe in ("suppliers", "customers", "supplier", "customer",
                              "n_suppliers", "n_customers", "supplier_count"):
                    if probe in ks:
                        found[probe] = found.get(probe, 0) + 1
                for v in o.values():
                    scan(v, depth + 1)
            elif isinstance(o, list):
                for x in o[:2000]:
                    scan(x, depth + 1)
        scan(L)
        for k, v in sorted(found.items(), key=lambda z: -z[1]):
            rep.log("  field '%s' appears %d times" % (k, v))
        gate(rep, "LINK.directional", bool(found),
             "directional supplier/customer fields present" if found else "NONE — only aggregates")

        rep.section("5. What would coverage become?")
        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        led = {r.get("ticker") for r in rows if r.get("ticker")}
        cur = sum(1 for r in rows if r.get("dependency_pct") is not None)
        g = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/supply-chain-graph.json")["Body"].read())
        gsyms = {n.get("ticker") for n in (g.get("nodes") or []) if n.get("ticker")}
        ov_link = syms & led
        ov_graph = gsyms & led
        union = (syms | gsyms) & led
        rep.kv(ledger=len(led), dependency_today=cur,
               graph_overlap=len(ov_graph), linkage_overlap=len(ov_link),
               union_overlap=len(union))
        gate(rep, "COVERAGE.gain", len(union) > len(ov_graph),
             "union would reach %d names vs %d from the curated graph alone"
             % (len(union), len(ov_graph)))

        rep.section("VERDICT")
        if len(ov_link) > len(ov_graph) * 1.3:
            rep.ok("WORTH JOINING: linkage reaches %d ledger names vs the graph's %d. "
                   "Next ops wires it as a second edge source, keeping the >=3 peer "
                   "floor intact." % (len(ov_link), len(ov_graph)))
        else:
            rep.warn("Linkage adds little (%d vs %d). Expanding coverage then means "
                     "widening the PRODUCER's universe, not adding a join."
                     % (len(ov_link), len(ov_graph)))
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — probe complete")


if __name__ == "__main__":
    main()
