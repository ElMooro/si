"""ops/750 — verify justhodl-opportunity-engine end-to-end.

Invokes the engine, validates data/opportunities.json schema + content,
confirms the value-trap guard (no HIGH RISK names leak into the
opportunity list), and fetches the live retail page.
"""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=150, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

report = {"ops": 750, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity Engine verification"}
checks = {}

# ── 1. invoke ──
try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    raw = r["Payload"].read().decode("utf-8", "replace")
    report["invoke_status"] = r.get("StatusCode")
    report["function_error"] = r.get("FunctionError")
    body = json.loads(json.loads(raw).get("body", "{}"))
    report["invoke_body"] = body
    checks["invoke_ok"] = (r.get("StatusCode") == 200
                           and not r.get("FunctionError")
                           and body.get("ok") is True)
    checks["covered_universe"] = (body.get("n_covered") or 0) > 50
except Exception as e:
    report["invoke_err"] = str(e)[:300]
    checks["invoke_ok"] = False
    checks["covered_universe"] = False

# ── 2. validate the output JSON ──
try:
    d = json.loads(s3.get_object(Bucket=BUCKET,
                   Key="data/opportunities.json")["Body"].read())
    top = d.get("top_opportunities", [])
    allr = d.get("all", [])
    checks["schema_ok"] = d.get("schema_version") == "1.0"
    checks["has_all_rows"] = len(allr) > 50
    checks["has_disclaimer"] = bool(d.get("disclaimer"))
    # every top item is well-formed
    fields = ("ticker", "verdict", "opportunity_score", "scores",
              "opportunities", "risks")
    checks["top_rows_wellformed"] = all(
        all(k in r for k in fields) for r in top) if top else True
    # VALUE-TRAP GUARD: no HIGH RISK / EXPENSIVE name may appear as an opportunity
    leaked = [r["ticker"] for r in top
              if r.get("verdict") in ("HIGH RISK", "EXPENSIVE")]
    checks["no_risk_leak_into_opportunities"] = len(leaked) == 0
    report["value_trap_guard"] = {"leaked": leaked}
    report["verdict_counts"] = d.get("verdict_counts")
    report["sample_top"] = [
        {"ticker": r["ticker"], "verdict": r["verdict"],
         "undervalued_pct": r.get("undervalued_pct"),
         "fair_value": [r.get("fair_value_low"), r.get("fair_value_high")],
         "price": r.get("price"), "opp_score": r.get("opportunity_score"),
         "why": (r.get("opportunities") or [None])[0]}
        for r in top[:5]]
    report["sample_avoid"] = [
        {"ticker": r["ticker"], "verdict": r["verdict"],
         "risk": (r.get("risks") or [None])[0]}
        for r in d.get("avoid_list", [])[:5]]
except Exception as e:
    report["json_err"] = str(e)[:300]
    for k in ("schema_ok", "has_all_rows", "has_disclaimer",
              "top_rows_wellformed", "no_risk_leak_into_opportunities"):
        checks[k] = False

# ── 3. live retail page ──
try:
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "ops750"})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
    checks["page_live"] = ("Stock Opportunities" in html
                           and "opportunities.json" in html)
except Exception as e:
    report["page_err"] = str(e)[:200]
    checks["page_live"] = False

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "OPPORTUNITY ENGINE LIVE — verdicts generated, value-trap guard "
    "holding, retail page serving"
    if report["all_pass"]
    else "REVIEW — see checks[] (page may still be propagating ~30-60s)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/750_opportunity_engine_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/750_opportunity_engine_verify.json")
