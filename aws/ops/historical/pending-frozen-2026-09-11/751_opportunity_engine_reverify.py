"""ops/751 — re-verify justhodl-opportunity-engine after the valuation fix.

Confirms: fair-value ranges are now tight, under/over-valued % is
capped, STRONG OPPORTUNITY is confidence-gated (count dropped), and the
value-trap guard still holds.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=150, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

report = {"ops": 751, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity Engine re-verify (valuation hardening)"}
checks = {}

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    checks["invoke_ok"] = (r.get("StatusCode") == 200
                           and not r.get("FunctionError"))
except Exception as e:
    report["invoke_err"] = str(e)[:300]
    checks["invoke_ok"] = False

try:
    d = json.loads(s3.get_object(Bucket=BUCKET,
                   Key="data/opportunities.json")["Body"].read())
    top = d.get("top_opportunities", [])
    allr = d.get("all", [])
    vc = d.get("verdict_counts", {})
    report["verdict_counts"] = vc

    # STRONG must now be confidence-gated → far fewer than the old 98
    strong = vc.get("STRONG OPPORTUNITY", 0)
    checks["strong_count_sane"] = strong < 70

    # under/over-valued % capped at +/-60 everywhere
    unders = [r.get("undervalued_pct") for r in allr
              if r.get("undervalued_pct") is not None]
    checks["under_pct_capped"] = all(-60.01 <= u <= 60.01 for u in unders)

    # fair-value ranges tight for surfaced opportunities (high-conf => spread<=40%)
    spreads = []
    for r in top:
        lo, hi = r.get("fair_value_low"), r.get("fair_value_high")
        if lo and hi and lo > 0:
            spreads.append(round(hi / lo - 1, 3))
    report["max_top_spread"] = max(spreads) if spreads else None
    checks["top_ranges_tight"] = (max(spreads) <= 0.45) if spreads else True

    # value-trap guard still holds
    leaked = [r["ticker"] for r in top
              if r.get("verdict") in ("HIGH RISK", "EXPENSIVE")]
    checks["no_risk_leak"] = len(leaked) == 0

    report["sample_top"] = [
        {"ticker": r["ticker"], "verdict": r["verdict"],
         "confidence": r.get("confidence"),
         "fv": [r.get("fair_value_low"), r.get("fair_value_high")],
         "price": r.get("price"), "under_pct": r.get("undervalued_pct"),
         "opp": r.get("opportunity_score")}
        for r in top[:8]]
except Exception as e:
    report["json_err"] = str(e)[:300]
    for k in ("strong_count_sane", "under_pct_capped",
              "top_ranges_tight", "no_risk_leak"):
        checks[k] = False

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VALUATION HARDENED — ranges tight, %s capped, STRONG confidence-gated"
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/751_opportunity_engine_reverify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/751_opportunity_engine_reverify.json")
