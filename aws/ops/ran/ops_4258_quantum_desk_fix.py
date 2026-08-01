"""
ops_4258 -- quantum-desk v1.0.1: read the fleet's real vocabularies.

4257's first run proved the pipe (function created, scheduled, 12/12
sources read, artifact written) and exposed three defects, all mine:
  1. regime paths were guesses -- real docs bury the label
     (nowcast: quadrant.regime="SOFT LANDING"; cycle-clock:
     investment-clock phases; router: sleeve nesting). Fix: bounded
     deep-search + alias table extended with the REAL vocabularies.
  2. best-setups rows are not at a top-level "setups" -- fix: bounded
     deep-search for the first list of {ticker, conviction} rows.
  3. ops_report has no .row -- this report uses kv/log only.

This op: wait for the v1.0.1 redeploy (same push), re-run, and require
what 4257 could not show: regime votes >= 1 and money_map > 0, then
print the REAL ladder and blotter into the report.
"""
import json, sys, time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-quantum-desk"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=200, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)

fails = []
with report("4258_quantum_desk_fix") as r:
    r.heading("ops 4258 -- quantum-desk v1.0.1 (real vocabularies) proven")

    r.section("1. wait for v1.0.1 redeploy (deploy-lambdas on this push)")
    ver = None
    for i in range(50):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                p = lam.invoke(FunctionName=FN,
                               InvocationType="RequestResponse",
                               Payload=b"{}")
                body = json.loads(p["Payload"].read() or b"{}")
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/quantum-desk.json")["Body"].read())
                ver = doc.get("version")
                if ver == "1.0.1":
                    break
        except Exception:
            pass
        time.sleep(10)
    if ver != "1.0.1":
        fails.append("v1.0.1 never landed (saw %s)" % ver)
        r.fail("v1.0.1 never landed (saw %s)" % ver)
    else:
        r.ok("v1.0.1 live and re-run: %s" % json.dumps(body)[:160])

    if ver == "1.0.1":
        r.section("2. the blotter, from real fleet data")
        reg = doc.get("regime") or {}
        votes = reg.get("votes") or []
        (r.ok if votes else r.warn)(
            "REGIME: %s -- votes: %s%s"
            % (reg.get("regime"),
               ", ".join("%(source)s=%(regime)s" % v for v in votes)
               or "NONE (all three regime docs unmappable -- neutral "
                  "prior, disclosed)",
               " -- SPLIT %s" % reg.get("disagreement")
               if reg.get("disagreement") else ""))
        if not votes:
            fails.append("still zero regime votes after deep-search")
        rk = doc.get("risk_gate") or {}
        r.log("RISK-GATE: posture=%s composite=%s sizing=x%s"
              % (rk.get("posture"), rk.get("composite"),
                 rk.get("sizing_multiplier")))
        lad = doc.get("asset_ladder") or []
        r.log("ASSET LADDER (top 8 of %d):" % len(lad))
        for row in lad[:8]:
            a = row.get("audit") or {}
            d = a.get("discount") or {}
            r.kv(cls=row["class"], score=row["score"],
                 verdict=row["verdict"],
                 vs_trend=d.get("pct_vs_trend"),
                 asym=a.get("asymmetry_ratio"),
                 legs="|".join(row.get("legs_used", [])))
        mm = doc.get("money_map") or []
        if not mm:
            fails.append("money map still empty: %s"
                         % doc.get("money_map_note"))
            r.fail("money map empty: %s" % doc.get("money_map_note"))
        else:
            r.log("MONEY MAP (top %d):" % min(8, len(mm)))
            for m in mm[:8]:
                r.kv(ticker=m["ticker"], fit=m["khalid_fit"],
                     cls=m["class"], quadrant=m.get("flow_quadrant"),
                     squeeze=m.get("squeeze_fuel"),
                     size_x=m.get("size_hint_x"))
        top = lad[0] if lad else None
        if top:
            r.ok("BEST CLASS NOW: %s (score %s, %s) at sizing x%s"
                 % (top["class"], top["score"], top["verdict"],
                    rk.get("sizing_multiplier")))
        dh = doc.get("data_health") or {}
        r.log("data health: %s/%s sources ok"
              % (dh.get("sources_ok"), dh.get("sources_total")))

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4258 PASS -- quantum-desk speaking the fleet's real "
             "language; blotter above is live data")

if fails:
    sys.exit(1)
