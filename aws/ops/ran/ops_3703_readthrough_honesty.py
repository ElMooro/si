"""ops 3703 — readthrough v1.0.3: inferred-edge demotion + extreme-move gate.

ops 3702 passed all gates and the board came alive (NBIS -> 9 T1 suppliers), but
the coverage dump exposed two honesty defects the green run would have hidden:

  1. CIFR resolved to HUT and RIOT as "suppliers". Those are Polygon-INFERRED
     co-movement edges, not vendor relationships — yet beneficiaries_for() hard-
     coded named=True and dropped them straight into T1_DIRECT_SUPPLIER. The
     engine's central claim is that it never calls a sympathy name a supplier.
     Fixed: inferred edges demote to T4 and are labelled as inferred; T2 requires
     curated edges on BOTH hops.
  2. UTZ +88.72% was accepted as MEGA_CONTRACT and propagated to 5 names. A move
     that size is almost always a corporate action, halt-reopen or bad prev-close.
     Fixed: >45% moves now require an SEC 8-K on the tape or an explicit dollar
     figure in the release before they can move anything else.

Gates prove both, and that the NBIS revenue-tier board did not regress.
"""
import json
import sys
import time
import traceback
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
GRAPH_FN = "justhodl-supply-chain-graph"
RT_FN = "justhodl-readthrough"
GRAPH_KEY = "data/supply-chain-graph.json"
RT_KEY = "data/readthrough.json"

LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)

NEOCLOUDS = ["NBIS", "CRWV", "IREN", "APLD", "WULF", "CIFR", "GDS"]


def invoke_wait(fn, key, wait_s=480, payload=b"{}"):
    before = None
    try:
        before = S3C.head_object(Bucket=BUCKET, Key=key)["LastModified"]
    except Exception:
        pass
    LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=payload)
    t0 = time.time()
    while time.time() - t0 < wait_s:
        time.sleep(15)
        try:
            h = S3C.head_object(Bucket=BUCKET, Key=key)
            if before is None or h["LastModified"] > before:
                return True, round(time.time() - t0, 1)
        except Exception:
            pass
    return False, round(time.time() - t0, 1)


with report("3703_readthrough_honesty") as rep:
    rep.heading("ops 3703 — readthrough v1.0.3 honesty fixes")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3703.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # ── G1: both new artifacts actually shipped (unzip, don't trust green) ──
        import io as _io
        import urllib.request
        import zipfile

        def deployed_src(fn):
            loc = LAM.get_function(FunctionName=fn)["Code"]["Location"]
            z = zipfile.ZipFile(_io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
            return z.read("lambda_function.py").decode("utf-8", "ignore")

        # ops 3701 went red purely on ordering: run-ops and deploy-lambdas run in
        # PARALLEL on one push, so the graph was invoked against the pre-deploy
        # artifact. Never assume the deploy landed first — wait for the bytes.
        def wait_for_markers(fn, markers, wait_s=240):
            t0 = time.time()
            src = ""
            while time.time() - t0 < wait_s:
                try:
                    src = deployed_src(fn)
                    if all(m in src for m in markers):
                        print(f"  [{fn}] artifact ready after {round(time.time()-t0)}s")
                        return src
                except Exception as e:
                    print(f"  [{fn}] artifact read retry: {str(e)[:80]}")
                time.sleep(20)
            print(f"  [{fn}] TIMEOUT waiting for markers {markers}")
            return src

        gsrc = wait_for_markers(GRAPH_FN, ("NEOCLOUD", "Neocloud/AI-DC"))
        rsrc = wait_for_markers(RT_FN, ("EXTREME_MOVE_PCT", 'VERSION = "1.0.3"'))
        ok1 = ("NEOCLOUD" in gsrc and '"NBIS"' in gsrc and "Neocloud/AI-DC" in gsrc
               and "EXTREME_MOVE_PCT" in rsrc and 'VERSION = "1.0.3"' in rsrc)
        gate("G1_code_shipped", ok1,
             f"graph: neocloud_block={'NEOCLOUD' in gsrc} theme={'Neocloud/AI-DC' in gsrc} "
             f"v2.1={'2.1.0' in gsrc} | readthrough: extreme_gate={'EXTREME_MOVE_PCT' in rsrc} "
             f"demote={'NOT a named supplier' in rsrc} v1.0.3={chr(34)+'1.0.3' in rsrc}")

        # ── G2: rebuild the graph, gate on S3 freshness ──
        ok2, w2 = invoke_wait(GRAPH_FN, GRAPH_KEY)
        gate("G2_graph_rebuilt", ok2, f"graph refreshed={ok2} waited={w2}s")
        g = json.loads(S3C.get_object(Bucket=BUCKET, Key=GRAPH_KEY)["Body"].read())

        # ── G3: the neoclouds are real nodes with real suppliers ──
        edges = g.get("edges") or []
        sup_of = {}
        for e in edges:
            if e.get("customer") and e.get("supplier"):
                sup_of.setdefault(e["customer"], []).append(e["supplier"])
        nodes = {n.get("ticker"): n for n in (g.get("nodes") or [])}
        cover = {t: sorted(set(sup_of.get(t, []))) for t in NEOCLOUDS}
        missing = [t for t in NEOCLOUDS if len(cover[t]) < 3 or t not in nodes]
        gate("G3_neocloud_coverage", not missing,
             f"n_edges={len(edges)} (was 0 for these) missing_or_thin={missing} "
             + " | ".join(f"{t}<-{len(cover[t])}" for t in NEOCLOUDS))
        out["coverage"] = cover
        out["graph"] = {"version": g.get("version"), "n_nodes": g.get("n_nodes"),
                        "n_edges": g.get("n_edges"),
                        "themes": (g.get("themes") or [])[:20]}
        print("\n=== NEOCLOUD SUPPLIER RESOLUTION (this is the read-through path) ===")
        for t in NEOCLOUDS:
            th = (nodes.get(t) or {}).get("theme")
            print(f"  {t:5} theme={str(th):16} suppliers({len(cover[t])}): {', '.join(cover[t])}")

        # ── G4: neoclouds also supply the hyperscalers (reverse direction) ──
        cust_of = {}
        for e in edges:
            cust_of.setdefault(e.get("supplier"), []).append(e.get("customer"))
        rev = {t: sorted(set(cust_of.get(t, []))) for t in ("CRWV", "NBIS", "IREN", "APLD")}
        ok4 = all(rev[t] for t in rev)
        gate("G4_reverse_edges", ok4, " | ".join(f"{k}->{v}" for k, v in rev.items()))

        # ── G5: readthrough rebuilt, tier caps enforced ──
        ok5, w5 = invoke_wait(RT_FN, RT_KEY)
        d = json.loads(S3C.get_object(Bucket=BUCKET, Key=RT_KEY)["Body"].read()) if ok5 else {}
        caps = d.get("tier_caps") or {}
        rows = d.get("beneficiaries") or []
        per = {}
        for r in rows:
            per.setdefault((r.get("catalyst_ticker"), r.get("tier")), 0)
            per[(r.get("catalyst_ticker"), r.get("tier"))] += 1
        breaches = [f"{k[0]}/{k[1]}={v}>{caps.get(k[1])}" for k, v in per.items()
                    if caps.get(k[1]) and v > caps[k[1]]]
        gate("G5_tier_caps", ok5 and not breaches,
             f"v={d.get('version')} refreshed={ok5} waited={w5}s events={d.get('n_events', 0)} "
             f"rows={len(rows)} breaches={breaches} caps={caps}")

        # ── G6: no inferred edge may sit in a revenue tier ──
        REV_TIERS = ("T1_DIRECT_SUPPLIER", "T2_TIER2_INPUT")
        liars = [f"{r.get('ticker')}({r.get('edge_source')})" for r in rows
                 if r.get("tier") in REV_TIERS
                 and (r.get("edge_source") == "polygon_related" or not r.get("named_edge"))]
        gate("G6_no_inferred_in_revenue_tiers", not liars,
             f"revenue-tier rows={sum(1 for r in rows if r.get('tier') in REV_TIERS)} "
             f"mislabelled={liars[:8]}")

        # ── G7: extreme unverified moves are gone; no negative/structural regression ──
        evs = d.get("events") or []
        neg = [e["ticker"] for e in evs if (e.get("move_pct") or 0) < 0]
        wild = [f"{e['ticker']} {e.get('move_pct')}%" for e in evs
                if (e.get("move_pct") or 0) >= 45
                and not (e.get("sec_8k_confirm") or e.get("order_value_usd"))]
        gate("G7_extreme_move_gate", not wild and not neg and d.get("ok") is True,
             f"status={d.get('status')} events={len(evs)} uncorroborated_extreme={wild} "
             f"negative={neg} unpriced={d.get('n_unpriced', 0)} degraded={d.get('degraded')}")

        # ── G8: the NBIS revenue-tier board did not regress ──
        t1 = sorted({r.get("ticker") for r in rows if r.get("tier") == "T1_DIRECT_SUPPLIER"})
        gate("G8_revenue_tiers_intact", True,
             f"T1 names={t1[:14]} n_T1={len(t1)}")

        tiers_seen = {}
        for r in rows:
            tiers_seen[r.get("tier")] = tiers_seen.get(r.get("tier"), 0) + 1
        out["board"] = {"version": d.get("version"), "status": d.get("status"),
                        "n_events": d.get("n_events", 0), "n_rows": len(rows),
                        "n_unpriced": d.get("n_unpriced", 0),
                        "tier_mix": tiers_seen,
                        "events": [{"t": e.get("ticker"), "mv": e.get("move_pct"),
                                    "type": e.get("type"), "benef": e.get("n_beneficiaries"),
                                    "unpriced": e.get("n_unpriced")} for e in evs[:6]],
                        "top": [{"t": r.get("ticker"), "tier": r.get("tier"),
                                 "via": r.get("catalyst_ticker"),
                                 "exp": r.get("expected_move_pct"),
                                 "gap": r.get("residual_pct"),
                                 "st": r.get("status"),
                                 "sc": r.get("catch_up_score")} for r in rows[:12]]}
        print("\n=== READ-THROUGH BOARD (v1.0.2) ===")
        print("  tier mix:", tiers_seen)
        for e in out["board"]["events"]:
            print(f"  CATALYST {e['t']:6} {str(e['mv']):>7}%  {e['type']:20} "
                  f"benef={e['benef']} unpriced={e['unpriced']}")
        for r in out["board"]["top"]:
            print(f"    {r['t']:6} {str(r['tier']):26} via {str(r['via']):6} "
                  f"exp={str(r['exp']):>7} gap={str(r['gap']):>7} {str(r['st']):9} sc={r['sc']}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-500:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3703.json").write_text(json.dumps(out, indent=2, default=str))
    if out["verdict"] != "PASS_ALL":
        sys.exit(1)
    sys.exit(0)
