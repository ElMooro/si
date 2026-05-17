"""ops/751 — verify justhodl-opportunity-engine end-to-end (post-acea202 fix).

ops 750 verified the engine BEFORE the acea202 refinement (outlier
rejection / agreement-gated confidence). This re-invokes it, reads the
fresh output, sanity-checks the numbers, and confirms the page is live.
"""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=160, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 751, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "opportunity-engine verify (post-acea202)"}

# ── invoke ──
try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": (r["Payload"].read().decode()[:300]
                                 if r.get("Payload") else "")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

# ── read output ──
data = None
try:
    data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/opportunities.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:240]

if data:
    rows = data.get("all", [])
    top = data.get("top_opportunities", [])
    avoid = data.get("avoid_list", [])
    unders = [r.get("undervalued_pct") for r in rows
              if r.get("undervalued_pct") is not None]
    conf_counts = {}
    for r in rows:
        conf_counts[r.get("confidence")] = conf_counts.get(r.get("confidence"), 0) + 1
    out_of_band = [r["ticker"] for r in rows
                   if r.get("undervalued_pct") is not None
                   and not (-60.01 <= r["undervalued_pct"] <= 60.01)]
    gen = data.get("generated_at", "")
    fresh = gen[:10] == datetime.now(timezone.utc).date().isoformat()

    report["output"] = {
        "schema_version": data.get("schema_version"),
        "generated_at": gen, "is_fresh_today": fresh,
        "n_covered": data.get("n_covered"),
        "verdict_counts": data.get("verdict_counts"),
        "n_top": len(top), "n_avoid": len(avoid),
        "confidence_distribution": conf_counts,
        "undervalued_pct_range": [min(unders), max(unders)] if unders else None,
        "out_of_band_pct_tickers": out_of_band,
    }
    report["sample_top"] = [
        {"t": r["ticker"], "verdict": r["verdict"], "price": r["price"],
         "fair_mid": r["fair_value_mid"], "under_pct": r["undervalued_pct"],
         "conf": r["confidence"], "opp": r["opportunity_score"],
         "ops": r.get("opportunities"), "risks": r.get("risks")}
        for r in top[:4]]
    report["sample_avoid"] = [
        {"t": r["ticker"], "verdict": r["verdict"], "under_pct": r["undervalued_pct"],
         "risks": r.get("risks")} for r in avoid[:4]]

    checks = {
        "invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
        "fresh_today": fresh,
        "universe_covered": (data.get("n_covered") or 0) >= 100,
        "has_verdict_spread": len(data.get("verdict_counts") or {}) >= 3,
        "no_out_of_band_pct": len(out_of_band) == 0,
        "top_populated": len(top) > 0,
    }
else:
    checks = {"output_readable": False}

# ── live page ──
try:
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "justhodl-ops/751"})
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "replace")
    checks["page_live"] = "Stock Opportunities" in html
    checks["page_wired_to_json"] = "opportunities.json" in html
except Exception as e:
    report["page_err"] = str(e)[:200]
    checks["page_live"] = False

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("OPPORTUNITY ENGINE HEALTHY — live, fresh, sane output, page wired"
                     if report["all_pass"]
                     else "REVIEW — see checks[] / sample output")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/751_opportunity_engine_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/751_opportunity_engine_verify.json")
