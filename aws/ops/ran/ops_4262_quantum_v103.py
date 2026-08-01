"""
ops_4262 -- quantum-desk v1.0.3 FINAL GATE: every leg wired, live.

v1.0.3 wires the ops-4261 payloads exactly: trend.px_vs_200dma_pct as
the discount leg (Khalid's first lens, the compass already computes it),
asym.ratio (upstream-capped 25) as the asymmetry leg, forward-returns
assets{TICKER}.current_vs_history_percentile as the strategic leg
(also on the on-chain BTC/ETH rows), "bonds"/"credit" class vocabulary,
abs() on sign-inconsistent downside.

Gate: version 1.0.3 · discount leg on >=10/14 ladder rows · strategic
on >=8/14 · >=1 regime vote · money map non-empty. Then print the real
blotter -- this is the deliverable.
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
with report("4262_quantum_v103") as r:
    r.heading("ops 4262 -- quantum-desk v1.0.3: the live blotter")
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
                if doc.get("version") == "1.0.3":
                    break
        except Exception:
            pass
        time.sleep(10)
    if not doc or doc.get("version") != "1.0.3":
        fails.append("v1.0.3 never landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        lad = doc["asset_ladder"]
        n_d = sum(1 for x in lad
                  if x["legs"].get("discount") is not None)
        n_s = sum(1 for x in lad
                  if x["legs"].get("strategic") is not None)
        if n_d < 10:
            fails.append("discount leg only %d/%d" % (n_d, len(lad)))
        if n_s < 8:
            fails.append("strategic leg only %d/%d" % (n_s, len(lad)))
        r.ok("legs wired: discount %d/%d, strategic %d/%d"
             % (n_d, len(lad), n_s, len(lad)))
        reg = doc["regime"]
        if not reg.get("votes"):
            fails.append("zero regime votes")
        r.ok("REGIME %s | votes: %s | abstained: %s"
             % (reg.get("regime"),
                ", ".join("%(source)s=%(regime)s" % v
                          for v in reg.get("votes") or []),
                ", ".join("%(source)s(%(label)s)" % a
                          for a in reg.get("abstained") or []) or "none"))
        rk = doc["risk_gate"]
        r.log("RISK-GATE %s composite=%s sizing=x%s"
              % (rk.get("posture"), rk.get("composite"),
                 rk.get("sizing_multiplier")))
        r.log("ASSET LADDER:")
        for x in lad:
            a = x.get("audit") or {}
            d = a.get("discount") or {}
            r.kv(cls=x["class"], score=x["score"], verdict=x["verdict"],
                 vs_200dma=d.get("pct_vs_trend"),
                 asym=a.get("asymmetry_ratio"),
                 strat_pctile=a.get("strategic"),
                 dd_now=a.get("dd_now_pct"),
                 legs=len(x.get("legs_used", [])))
        mm = doc["money_map"]
        if not mm:
            fails.append("money map empty")
        r.log("MONEY MAP:")
        for m in mm:
            r.kv(ticker=m["ticker"], fit=m["khalid_fit"], cls=m["class"],
                 quadrant=m.get("flow_quadrant"),
                 setup_verdict=m.get("setup_verdict"),
                 flags=m.get("red_flags"), size_x=m.get("size_hint_x"))
        top = lad[0] if lad else None
        if top and not fails:
            r.ok("BEST CLASS NOW: %s (score %s, %s, %s%% vs 200dma) "
                 "at x%s sizing"
                 % (top["class"], top["score"], top["verdict"],
                    ((top.get("audit") or {}).get("discount") or {})
                    .get("pct_vs_trend"),
                    rk.get("sizing_multiplier")))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4262 PASS -- quantum-desk v1.0.3: every leg live, "
             "blotter above is tonight's real read")
if fails:
    sys.exit(1)
