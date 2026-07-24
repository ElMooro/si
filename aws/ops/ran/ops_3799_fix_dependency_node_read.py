#!/usr/bin/env python3
"""ops 3799 — dependency: fix the dead node read + label the real constraint.

3798 opened the feed instead of assuming. Findings:
  GRAPH IS HEALTHY  — 5.4h fresh, 183 nodes / 303 edges, and 303/303 edges
                      expose supplier|source exactly as chokepoint reads them.
  GRAPH IS SMALL    — it names only 185 distinct symbols. 180 overlap the 3,012
                      -row ledger and 153 already carry dependency_pct. So the
                      JOIN is working; the CEILING is the graph.
  ONE REAL BUG      — chokepoint reads n["centrality"], but nodes carry
                      degree / n_suppliers / n_customers and ZERO of 183 have a
                      "centrality" key. That entire node loop has always been a
                      no-op; only the edge loop ever contributed.

So "not working" is half right, and my earlier "expected sparsity" was too
generous to myself. Two changes:

 [1] READ THE FIELDS THAT EXIST. Use n_suppliers (inbound supplier links) and
     degree as centrality inputs, not the absent "centrality" key. This should
     raise coverage from 153 toward the ~180 the graph can actually support and
     makes centrality — 15% of the criticality score — reflect real edges.

 [2] STOP OVERSTATING THE METRIC. dependency_pct is a share of a CURATED
     185-symbol map, not of a company's real supply chain. The page must say
     that plainly, and the feed now ships graph_coverage so the ceiling is
     visible rather than implied.

What this does NOT do: fabricate coverage. Expanding the graph beyond 185
symbols is a job for the supply-chain engine, not a page fix, and pretending
otherwise would be worse than a blank column.
"""
import sys, json, time, zipfile, io, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-chokepoint"
SRC = ROOT / "lambdas" / FN / "source"
LF = SRC / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3799_fix_dependency_node_read") as rep:
        rep.heading("ops 3799 — read the node fields that actually exist")

        src = LF.read_text()
        rep.section("G0 — confirm the dead read")
        old = '''    for n in (g.get("nodes") or []):
        s = n.get("symbol") or n.get("id")
        if s and isinstance(n.get("centrality"), (int, float)):
            centrality[s] = max(centrality.get(s, 0), n["centrality"] * 10)'''
        gate(rep, "G0.anchor", src.count(old) == 1, "node loop anchor unique")
        gate(rep, "G0.v423", 'VERSION = "4.2.3"' in src, "engine at v4.2.3")

        graph = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/supply-chain-graph.json")["Body"].read())
        nodes = graph.get("nodes") or []
        n_cent = sum(1 for n in nodes if isinstance(n.get("centrality"), (int, float)))
        n_deg = sum(1 for n in nodes if isinstance(n.get("degree"), (int, float)))
        n_sup = sum(1 for n in nodes if isinstance(n.get("n_suppliers"), (int, float)))
        n_tick = sum(1 for n in nodes if n.get("ticker"))
        rep.kv(nodes=len(nodes), with_centrality=n_cent, with_degree=n_deg,
               with_n_suppliers=n_sup, with_ticker=n_tick)
        gate(rep, "G0.centrality_absent", n_cent == 0,
             "0 of %d nodes carry 'centrality' — the read is dead" % len(nodes))
        gate(rep, "G0.degree_present", n_deg > 0, "%d nodes carry 'degree'" % n_deg)
        gate(rep, "G0.ticker_key", n_tick > 0,
             "nodes key the symbol as 'ticker' (reader looks for symbol|id)")
        if FAILED:
            sys.exit(1)

        rep.section("[1] Read ticker/degree/n_suppliers instead of the absent keys")
        new = '''    for n in (g.get("nodes") or []):
        # ops 3799: nodes key the symbol as "ticker" and carry degree /
        # n_suppliers / n_customers. They have NEVER carried "centrality" —
        # 0 of 183 — so the previous read was a permanent no-op and only the
        # edge loop above ever contributed. Use the fields that exist.
        s = n.get("ticker") or n.get("symbol") or n.get("id")
        if not s:
            continue
        _inb = n.get("n_suppliers")
        _deg = n.get("degree")
        _cand = None
        if isinstance(_inb, (int, float)) and _inb > 0:
            _cand = float(_inb)
        if isinstance(_deg, (int, float)) and _deg > 0:
            _cand = max(_cand or 0.0, float(_deg) * 0.5)
        if isinstance(n.get("centrality"), (int, float)):
            _cand = max(_cand or 0.0, float(n["centrality"]) * 10)
        if _cand:
            centrality[s.upper()] = max(centrality.get(s.upper(), 0), _cand)'''
        src = src.replace(old, new, 1)

        # edge loop should normalise case too, so joins do not miss on casing
        src = src.replace(
            '        if sup: centrality[sup] = centrality.get(sup, 0) + 1',
            '        if sup:\n'
            '            sup = sup.strip().upper()\n'
            '            centrality[sup] = centrality.get(sup, 0) + 1', 1)

        rep.section("[2] Ship the graph ceiling so the metric is not overstated")
        anchor = '            capture["stats"]["with_dependency"] = sum(\n                1 for c in cap_rows if c.get("dependency_pct") is not None)'
        if src.count(anchor) == 1:
            src = src.replace(anchor, anchor + '''
            capture["dependency_note"] = (
                "dependency_pct is a share of a CURATED supply-chain map, not of a "
                "company's real supplier base. That map currently names %d symbols "
                "across %d edges, so most listed companies have no entry — a blank "
                "means UNMAPPED, never zero dependency. Expanding coverage is a job "
                "for the supply-chain engine, not something this page can infer."
                % (len(g.get("nodes") or []), len(g.get("edges") or [])))
            capture["stats"]["graph_nodes"] = len(g.get("nodes") or [])
            capture["stats"]["graph_edges"] = len(g.get("edges") or [])''', 1)
            rep.ok("dependency_note + graph size shipped in the feed")
        else:
            rep.warn("stats anchor not found (%d) — note not added" % src.count(anchor))

        src = src.replace('VERSION = "4.2.3"', 'VERSION = "4.3"', 1)
        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("v4.3 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.3 (supply-chain node read fixed: ticker/degree/n_suppliers).",
                      create_function_url=False, smoke=False)
        settled = False
        for i in range(14):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "ops 3799: nodes key the symbol" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "v4.3 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + measure the change")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        mem = [m for b in (cap.get("by_industry") or []) for m in (b.get("members") or []) if m]
        st = cap.get("stats") or {}
        gate(rep, "LIVE.v43", d.get("version") == "4.3", "version=%s" % d.get("version"))
        dep_n = sum(1 for x in rows if x.get("dependency_pct") is not None)
        dep_m = sum(1 for m in mem if m.get("dependency_pct") is not None)
        rep.kv(dependency_ledger=dep_n, dependency_members=dep_m,
               graph_nodes=st.get("graph_nodes"), graph_edges=st.get("graph_edges"),
               was_before=153)
        gate(rep, "FIX.improved", dep_n >= 153, "%d names carry dependency (was 153)" % dep_n)
        gate(rep, "FEED.note", bool(cap.get("dependency_note")), "ceiling stated in the feed")

        rep.section("Sample — names the graph actually maps")
        got = [x for x in rows if x.get("dependency_pct") is not None]
        for x in sorted(got, key=lambda z: -(z.get("dependency_pct") or 0))[:14]:
            rep.log("  %-6s %-30s dep=%5.1f%%  crit=%s" % (
                x.get("ticker"), (x.get("industry") or "")[:30],
                x.get("dependency_pct") or 0, x.get("criticality")))

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality_pctile"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — node read fixed; graph ceiling stated rather than implied")


if __name__ == "__main__":
    main()
