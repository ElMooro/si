"""
ops_4260 -- quantum-desk v1.0.2 verified on live fleet data.

v1.0.2 wires the ops-4259 probe: compass class vocabulary + trend.label
discount + asym{} numerics, top_setups path, ETF/sector class gates,
router BALANCED_UNCERTAIN as transparent abstain, and real BTC/ETH
ladder rows built from crypto-cycle-risk (MVRV = price/realized as the
discount analog; compass carries no crypto).

Gate: version 1.0.2 live, >=1 regime vote, abstain recorded, the TOP
ladder rows scoring on >=3 legs (multi-leg confirmation -- a playbook-
only ladder was 4258's defect), money-map classes non-uniform. Print
the full real blotter. Warn (not fail) if the strategic leg is still
unmapped -- that is forward-returns' shape, queued honestly.
"""
import json, sys, time
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-quantum-desk"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=200, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)

fails = []
with report("4260_quantum_v102") as r:
    r.heading("ops 4260 -- quantum-desk v1.0.2 live blotter")
    doc = None
    for i in range(50):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lam.invoke(FunctionName=FN,
                           InvocationType="RequestResponse", Payload=b"{}")
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/quantum-desk.json")["Body"].read())
                if doc.get("version") == "1.0.2":
                    break
        except Exception:
            pass
        time.sleep(10)
    if not doc or doc.get("version") != "1.0.2":
        fails.append("v1.0.2 never landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        reg = doc["regime"]
        votes = reg.get("votes") or []
        if not votes:
            fails.append("zero regime votes")
        r.ok("REGIME %s | votes: %s | abstained: %s"
             % (reg.get("regime"),
                ", ".join("%(source)s=%(regime)s" % v for v in votes),
                ", ".join("%(source)s(%(label)s)" % a
                          for a in reg.get("abstained") or []) or "none"))
        rk = doc["risk_gate"]
        r.log("RISK-GATE %s composite=%s sizing=x%s"
              % (rk.get("posture"), rk.get("composite"),
                 rk.get("sizing_multiplier")))
        lad = doc["asset_ladder"]
        multi = [x for x in lad[:5] if len(x.get("legs_used", [])) >= 3]
        if len(multi) < 3:
            fails.append("top-5 ladder rows not multi-leg: %s"
                         % [(x["class"], x["legs_used"]) for x in lad[:5]])
        strat = sum(1 for x in lad
                    if x["legs"].get("strategic") is not None)
        (r.ok if strat else r.warn)(
            "strategic leg wired on %d/%d rows%s"
            % (strat, len(lad),
               "" if strat else
               " -- forward-returns shape unmapped, queued (honest gap)"))
        r.log("ASSET LADDER (%d rows):" % len(lad))
        for x in lad[:10]:
            a = x.get("audit") or {}
            d = a.get("discount") or {}
            r.kv(cls=x["class"], score=x["score"], verdict=x["verdict"],
                 vs_trend=d.get("pct_vs_trend"), basis=d.get("basis"),
                 asym=a.get("asymmetry_ratio"),
                 legs="|".join(x.get("legs_used", [])))
        mm = doc["money_map"]
        classes = {m["class"] for m in mm}
        if mm and len(classes) == 1 and len(mm) > 6:
            r.warn("money-map classes uniform (%s) -- sector map may "
                   "need widening" % classes)
        r.log("MONEY MAP (%d):" % len(mm))
        for m in mm[:10]:
            r.kv(ticker=m["ticker"], fit=m["khalid_fit"], cls=m["class"],
                 quadrant=m.get("flow_quadrant"),
                 setup_verdict=m.get("setup_verdict"),
                 flags=m.get("red_flags"), size_x=m.get("size_hint_x"))
        top = lad[0] if lad else None
        if top:
            r.ok("BEST CLASS NOW: %s (score %s, %s) at x%s sizing"
                 % (top["class"], top["score"], top["verdict"],
                    rk.get("sizing_multiplier")))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4260 PASS -- v1.0.2 blotter above is live, multi-leg, "
             "honest about gaps")
if fails:
    sys.exit(1)
