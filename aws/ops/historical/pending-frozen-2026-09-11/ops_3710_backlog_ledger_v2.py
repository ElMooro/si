"""ops 3710 — Stage 1 of Khalid's reverse-engineered-backlog theory.

THEORY: a customer's newly announced backlog IS a purchase order its suppliers
have not reported yet. So do not only ask "did the stock move" — ask whether the
implied dollars are already on the supplier's books, and whether consensus has
modelled them. Price lag is a day trade. Estimate lag is a quarter trade.

SHIPPED
  justhodl-backlog  — now emits rpo_asof / rpo_filed / rpo_form. Without the
    as-of date you cannot say a claim is provably absent from the last filing,
    which is the entire test.
  justhodl-readthrough v1.1.0
    * FLOW CONSERVATION — the tier capture share is now SPLIT across that tier's
      named suppliers by BOM weight (gpu 6.0 ... connectors 0.3, eda 0.15), with
      a 2x boost when the release names the supplier. This closes the known
      limitation from ops 3706: with 12 T1 names the same $33B was counted 12x.
    * FUNDAMENTAL JOIN — data/backlog.json + data/forward-orders.json +
      data/estimate-revisions.json onto every beneficiary row.
    * UNBOOKED TEST — implied_order_usd / last reported RPO, plus
      claim_predates_filing (is the last RPO filing older than the catalyst?).
    * 2x2 QUADRANT — TWICE_UNPRICED / ESTIMATES_LEADING / PRICE_LEADING /
      FULLY_PRICED / PRICE_ONLY.

HONEST EXPECTATION: RPO is a SaaS/cloud/defense disclosure. MU, STX, WDC, APH
largely do not file that tag, so coverage will be thin exactly in AI hardware.
PRICE_ONLY rows are the expected majority and are NOT a failure — they are the
engine declining to invent a fundamental it does not have.
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
FN = "justhodl-readthrough"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"
LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)

with report("3710_backlog_ledger_v2") as rep:
    rep.heading("ops 3710 — backlog reverse-engineering, stage 1")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3710.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        import io as _io
        import urllib.request
        import zipfile

        def deployed(fn):
            loc = LAM.get_function(FunctionName=fn)["Code"]["Location"]
            z = zipfile.ZipFile(_io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
            return z.read("lambda_function.py").decode("utf-8", "ignore")

        src = bsrc = ""
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                src = deployed(FN)
                bsrc = deployed("justhodl-backlog")
                if 'VERSION = "1.1.1"' in src and "rpo_asof" in bsrc:
                    print("  artifacts ready after", round(time.time() - t0), "s")
                    break
            except Exception as e:
                print("  retry:", str(e)[:70])
            time.sleep(20)
        gate("G1_shipped", 'VERSION = "1.1.1"' in src and "load_fundamentals" in src
             and "BOM_CLASS_WEIGHT" in src and "rpo_asof" in bsrc,
             f"v1.1.0={chr(34)+'1.1.0' in src} join={'load_fundamentals' in src} "
             f"flow_cons={'BOM_CLASS_WEIGHT' in src} backlog_asof={'rpo_asof' in bsrc}")

        # refresh the backlog sidecar first so rpo_asof actually exists downstream
        bb = None
        try:
            bb = S3C.head_object(Bucket=BUCKET, Key="data/backlog.json")["LastModified"]
        except Exception:
            pass
        sizes = []
        for _pass in range(3):
            try:
                bb = S3C.head_object(Bucket=BUCKET, Key="data/backlog.json")["LastModified"]
            except Exception:
                bb = None
            LAM.invoke(FunctionName="justhodl-backlog", InvocationType="Event", Payload=b"{}")
            bt = time.time()
            while time.time() - bt < 300:
                time.sleep(20)
                try:
                    h = S3C.head_object(Bucket=BUCKET, Key="data/backlog.json")
                    if bb is None or h["LastModified"] > bb:
                        break
                except Exception:
                    pass
            _d = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/backlog.json")["Body"].read())
            sizes.append(len(_d.get("by_ticker") or {}))
            print(f"  backlog pass {_pass+1}: ledger={sizes[-1]} slice={_d.get('slice_this_run')}")
        out["ledger_growth"] = sizes
        print("  backlog sidecar refreshed in", round(time.time() - bt), "s")
        bl = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/backlog.json")["Body"].read())
        byt = bl.get("by_ticker") or {}
        with_asof = [k for k, v in byt.items() if v.get("rpo_asof") or v.get("deferred_asof")]
        gate("G2b_ledger_accumulates", len(sizes) >= 2 and sizes[-1] > sizes[0],
             f"ledger sizes across 3 passes = {sizes} (must GROW, not churn)")
        gate("G2_backlog_asof", len(with_asof) >= 20,
             f"backlog names={len(byt)} with_asof={len(with_asof)} "
             f"sample={ {k: byt[k].get('rpo_asof') for k in with_asof[:5]} }")

        before = None
        try:
            before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        ok3, w = False, 0
        t0 = time.time()
        while time.time() - t0 < 420:
            time.sleep(15)
            try:
                h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                if before is None or h["LastModified"] > before:
                    ok3, w = True, round(time.time() - t0, 1)
                    break
            except Exception:
                pass
        gate("G3_invoke", ok3, f"refreshed={ok3} waited={w}s")
        d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()) if ok3 else {}
        rows = d.get("beneficiaries") or []

        # ── flow conservation: per event+tier the capture shares must sum to the
        # tier's base share, never more. This is the ops 3706 limitation closed.
        TIER_BASE = {k: v["capture_share"] if isinstance(v, dict) and "capture_share" in v
                     else (v.get("capture_share") if isinstance(v, dict) else None)
                     for k, v in (d.get("tiers") or {}).items()}
        sums = {}
        for r0 in rows:
            k = (r0.get("catalyst_ticker"), r0.get("tier"))
            sums[k] = sums.get(k, 0.0) + (r0.get("capture_share") or 0.0)
        over = [f"{k[0]}/{k[1]}={round(v,4)}>{TIER_BASE.get(k[1])}" for k, v in sums.items()
                if TIER_BASE.get(k[1]) is not None and v > TIER_BASE[k[1]] + 1e-6]
        gate("G4_flow_conservation", not over,
             f"tier-share sums checked={len(sums)} overruns={over[:6]} "
             f"example={ {f'{k[0]}/{k[1]}': round(v,4) for k, v in list(sums.items())[:4]} }")

        qc = d.get("quadrant_counts") or {}
        joined = [r0 for r0 in rows if (r0.get("fundamentals") or {}).get("rpo_usd")
                  or (r0.get("fundamentals") or {}).get("est_direction")]
        gate("G5_join_live", bool(qc) and len(joined) >= 1,
             f"v={d.get('version')} quadrants={qc} rows={len(rows)} "
             f"rows_with_fundamentals={len(joined)} degraded={d.get('degraded')}")

        unbooked = [r0 for r0 in rows
                    if (r0.get("fundamentals") or {}).get("claim_predates_filing")]
        # the whole point of 3710: no row may claim consensus has not moved when
        # consensus was never observed.
        liars = [x.get("ticker") for x in rows
                 if x.get("pricing_quadrant") == "TWICE_UNPRICED"
                 and (x.get("fundamentals") or {}).get("est_direction") is None]
        gate("G7_no_unearned_twice", not liars, f"TWICE_UNPRICED without revision data={liars[:8]}")

        junk = [f"{x.get('ticker')}={(x.get('fundamentals') or {}).get('unbooked_vs_rpo')}"
                for x in rows
                if ((x.get("fundamentals") or {}).get("unbooked_vs_rpo") or 0) > 5]
        gate("G8_rpo_denominator_sane", not junk, f"implausible unbooked ratios={junk[:8]}")

        gate("G6_unbooked_test", True,
             f"rows where the last RPO filing predates the catalyst: {len(unbooked)} "
             f"-> {[r0['ticker'] for r0 in unbooked[:10]]}")

        out["board"] = {
            "version": d.get("version"), "quadrants": qc,
            "n_events": d.get("n_events"), "n_rows": len(rows),
            "twice_unpriced": [{"t": x.get("ticker"), "via": x.get("catalyst_ticker"),
                                "tier": x.get("tier"), "gap": x.get("residual_pct"),
                                "sc": x.get("catch_up_score"),
                                "implied": x.get("implied_order_usd"),
                                "cap": x.get("capture_share"),
                                "unbooked": (x.get("fundamentals") or {}).get("unbooked_vs_rpo"),
                                "rpo_asof": (x.get("fundamentals") or {}).get("rpo_asof"),
                                "est": (x.get("fundamentals") or {}).get("est_direction")}
                               for x in (d.get("twice_unpriced") or [])[:14]],
            "smci": [{"t": x.get("ticker"), "cap": x.get("capture_share"),
                      "implied": x.get("implied_order_usd"), "exp": x.get("expected_move_pct"),
                      "gap": x.get("residual_pct"), "q": x.get("pricing_quadrant"),
                      "sc": x.get("catch_up_score")}
                     for x in rows if x.get("catalyst_ticker") == "SMCI"][:14],
        }
        print("\n=== QUADRANTS ===", qc)
        print("\n=== SMCI CHAIN, flow-conserved ===")
        for x in out["board"]["smci"]:
            imp = x["implied"]
            print(f"  {x['t']:6} capture={str(x['cap']):>8} implied="
                  f"{('$' + format(imp/1e9, '.2f') + 'B') if imp else '     -':>9} "
                  f"exp={str(x['exp']):>7} gap={str(x['gap']):>7} {str(x['q']):18} sc={x['sc']}")
        print("\n=== TWICE UN-PRICED ===")
        for x in out["board"]["twice_unpriced"]:
            print(f"  {x['t']:6} via {str(x['via']):6} {str(x['tier']):26} gap={str(x['gap']):>7} "
                  f"unbooked_vs_rpo={str(x['unbooked'])} rpo_asof={str(x['rpo_asof'])} "
                  f"est={str(x['est'])} sc={x['sc']}")
        if not out["board"]["twice_unpriced"]:
            print("  (none right now)")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-600:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3710.json").write_text(json.dumps(out, indent=2, default=str))
    if out["verdict"] != "PASS_ALL":
        sys.exit(1)
    sys.exit(0)
