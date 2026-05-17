"""ops/754 — verify the confidence-discount fix: top opportunities should
now favour HIGH/MODERATE confidence over capped/single-source picks.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=170, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 754, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "opportunity-engine confidence-discount verify"}

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:200]}

data = None
try:
    data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/opportunities.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

checks = {}
if data:
    top = data.get("top_opportunities", [])
    top10 = top[:10]
    conf_top10 = {}
    for r in top10:
        conf_top10[r["confidence"]] = conf_top10.get(r["confidence"], 0) + 1
    n_trustworthy = sum(1 for r in top10
                        if r["confidence"] in ("high", "moderate"))
    report["schema"] = data.get("schema_version")
    report["n_covered"] = data.get("n_covered")
    report["top10_confidence_mix"] = conf_top10
    report["top10"] = [
        {"t": r["ticker"], "verdict": r["verdict"], "under": r["undervalued_pct"],
         "conf": r["confidence"], "opp": r["opportunity_score"]}
        for r in top10]
    checks = {
        "invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
        "schema_v2": data.get("schema_version") == "2.0",
        "universe_intact": (data.get("n_covered") or 0) >= 100,
        "top_favours_trustworthy": n_trustworthy >= 6,
    }
else:
    checks = {"output_readable": False}

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CONFIDENCE-WEIGHTED RANKING LIVE — {checks.get('top_favours_trustworthy') and sum(1 for r in (data or {}).get('top_opportunities',[])[:10] if r['confidence'] in ('high','moderate'))}/10 "
    "top picks are high/moderate confidence"
    if report["all_pass"]
    else "REVIEW — see checks[] / top10")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/754_confidence_rank_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/754_confidence_rank_verify.json")
