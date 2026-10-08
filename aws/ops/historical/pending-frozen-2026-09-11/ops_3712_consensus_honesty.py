"""ops 3712 — Stage 1.5: make consensus observable so TWICE_UNPRICED can be earned.

ops 3707/3710 showed the binding constraint was never the propagation math — it
was CONSENSUS OBSERVABILITY. estimate-revisions only carries names within ~2
months of earnings, so "has consensus moved" was blank on nearly every row and
the quadrant collapsed to PRICE_ONLY (78 of 79 rows at ops 3710).

readthrough v1.2.0 adds two tracks:
  1. ANALYST ACTIONS — dated upgrades / PT raises / guidance raises from
     data/analyst-actions.json, joined per beneficiary and filtered to actions
     dated ON OR AFTER the catalyst. Observable on day one.
  2. CONSENSUS SNAPSHOT LEDGER — FY+1 revenue consensus snapshotted per
     shortlist name into readthrough/consensus-snapshots.json each run, so real
     revision deltas accrue for names nowhere near an earnings date.

The honesty rule that makes this work: absence of an action only counts as
evidence of no reaction when the name is VISIBLE in the feed at all. A name the
sell side never covers is UNOBSERVED, not unmoved.

Also recorded: the ops 3710 ledger gate was MIS-SPECIFIED by me. It demanded the
backlog ledger grow across three same-day passes, but the slice rotates daily,
so growth within one day is impossible by design. The merge itself is proven —
the ledger went 28 -> 83 names and readthrough rows_with_fundamentals went
0 -> 14. Nothing to fix in the engine; the gate was wrong.
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
SNAP = "readthrough/consensus-snapshots.json"
LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)

with report("3712_consensus_honesty") as rep:
    rep.heading("ops 3712 — consensus observability, stage 1.5")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3712.json").write_text(json.dumps({"verdict": "STARTED"}))
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

        src = ""
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                loc = LAM.get_function(FunctionName=FN)["Code"]["Location"]
                z = zipfile.ZipFile(_io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
                src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if 'VERSION = "1.2.1"' in src and "load_analyst_actions" in src:
                    print("  artifact ready after", round(time.time() - t0), "s")
                    break
            except Exception as e:
                print("  retry:", str(e)[:70])
            time.sleep(20)
        gate("G1_shipped", 'VERSION = "1.2.1"' in src and "CONSENSUS_DISSENTING" in src
             and "CONSENSUS_LAG_DAYS" in src and "consensus_dissenting" in src,
             f"v1.2.0={chr(34)+'1.2.0' in src} actions={'load_analyst_actions' in src} "
             f"snapshot={'consensus_snapshot' in src} observed={'consensus_observed' in src}")

        # two passes: the snapshot ledger needs a prior run to diff against
        for p in (1, 2):
            before = None
            try:
                before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
            except Exception:
                pass
            LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            t0 = time.time()
            ok = False
            while time.time() - t0 < 420:
                time.sleep(15)
                try:
                    h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                    if before is None or h["LastModified"] > before:
                        ok = True
                        break
                except Exception:
                    pass
            print(f"  pass {p}: refreshed={ok} in {round(time.time()-t0)}s")
        gate("G2_invoke", ok, f"second pass refreshed={ok}")

        d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        rows = d.get("beneficiaries") or []
        cov = d.get("consensus_honesty") or {}
        qc = d.get("quadrant_counts") or {}

        try:
            snap = json.loads(S3C.get_object(Bucket=BUCKET, Key=SNAP)["Body"].read())
        except Exception:
            snap = {}
        gate("G3_snapshot_ledger", len(snap) >= 20,
             f"consensus snapshot ledger holds {len(snap)} names "
             f"(sample={list(snap)[:6]})")

        observed = cov.get("rows_consensus_observed", 0)
        gate("G4_consensus_observable", observed > 0,
             f"v={d.get('version')} rows={len(rows)} consensus_observed={observed} "
             f"sellside_covered={cov.get('names_with_sellside_coverage')} "
             f"deltas={cov.get('names_with_consensus_delta')} quadrants={qc} "
             f"degraded={d.get('degraded')}")

        # a downgrade since the catalyst may never sit in a bullish quadrant
        bull = ("TWICE_UNPRICED", "UNBOOKED_NO_CONSENSUS")
        mis = [x.get("ticker") for x in rows
               if x.get("pricing_quadrant") in bull
               and (x.get("fundamentals") or {}).get("consensus_dissenting")]
        gate("G7_dissent_not_bullish", not mis, f"dissenting names in a bullish quadrant={mis[:8]}")

        # a state that fires on most of the board is not a state
        share = qc.get("TWICE_UNPRICED", 0) / max(1, len(rows))
        gate("G8_twice_is_discriminating", share <= 0.45,
             f"TWICE_UNPRICED share={share:.0%} of {len(rows)} rows "
             f"(was 68% at ops 3711, before the consensus-lag rule)")

        early = qc.get("CONSENSUS_NOT_DUE_YET", 0)
        gate("G9_lag_rule_active", early > 0 or share <= 0.10,
             f"CONSENSUS_NOT_DUE_YET={early} - fresh catalysts no longer masquerade as anomalies")

        liars = [x.get("ticker") for x in rows
                 if x.get("pricing_quadrant") == "TWICE_UNPRICED"
                 and not (x.get("fundamentals") or {}).get("consensus_observed")]
        gate("G5_no_unearned_twice", not liars, f"TWICE_UNPRICED without observability={liars[:8]}")

        TIER_BASE = {k: (v or {}).get("capture_share") for k, v in (d.get("tiers") or {}).items()}
        sums = {}
        for x in rows:
            k = (x.get("catalyst_ticker"), x.get("tier"))
            sums[k] = sums.get(k, 0.0) + (x.get("capture_share") or 0.0)
        over = [f"{k[0]}/{k[1]}" for k, v in sums.items()
                if TIER_BASE.get(k[1]) is not None and v > TIER_BASE[k[1]] + 1e-6]
        gate("G6_flow_conservation_holds", not over, f"overruns={over[:6]} groups={len(sums)}")

        out["board"] = {
            "version": d.get("version"), "quadrants": qc, "coverage": cov,
            "reacted": [{"t": x.get("ticker"), "via": x.get("catalyst_ticker"),
                         "n_act": (x.get("fundamentals") or {}).get("analyst_actions_since_catalyst"),
                         "net": (x.get("fundamentals") or {}).get("analyst_net_since_catalyst"),
                         "q": x.get("pricing_quadrant"), "sc": x.get("catch_up_score")}
                        for x in rows
                        if ((x.get("fundamentals") or {}).get("analyst_actions_since_catalyst") or 0) > 0][:12],
            "top": [{"t": x.get("ticker"), "via": x.get("catalyst_ticker"),
                     "tier": x.get("tier"), "gap": x.get("residual_pct"),
                     "q": x.get("pricing_quadrant"),
                     "obs": (x.get("fundamentals") or {}).get("consensus_observed"),
                     "moved": (x.get("fundamentals") or {}).get("consensus_moved"),
                     "sc": x.get("catch_up_score")}
                    for x in rows[:14]],
        }
        print("\n=== QUADRANTS ===", qc)
        print("=== COVERAGE ===", cov)
        print("\n=== SELL SIDE ALREADY REACTED ===")
        for x in out["board"]["reacted"]:
            print(f"  {x['t']:6} via {str(x['via']):6} actions={x['n_act']} net={x['net']} "
                  f"{str(x['q']):22} sc={x['sc']}")
        if not out["board"]["reacted"]:
            print("  (no dated sell-side action on any beneficiary since its catalyst)")
        print("\n=== TOP ROWS ===")
        for x in out["board"]["top"]:
            print(f"  {x['t']:6} {str(x['tier']):26} gap={str(x['gap']):>7} "
                  f"{str(x['q']):22} observed={x['obs']} moved={x['moved']} sc={x['sc']}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-600:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3712.json").write_text(json.dumps(out, indent=2, default=str))
    if out["verdict"] != "PASS_ALL":
        sys.exit(1)
    sys.exit(0)
