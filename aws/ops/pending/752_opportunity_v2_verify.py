"""ops/752 — verify Opportunity Engine v2 (Retail Edge) end-to-end.

Confirms the v2 deploy landed, the new schema/fields are present, the
cap + cycle guards work, and the page is live.
"""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=170, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 752, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity Engine v2 verify"}

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode()[:300] if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "body": body}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

data = None
try:
    data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/opportunities.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:240]

checks = {}
if data:
    rows = data.get("all", [])
    unders = [r.get("undervalued_pct") for r in rows
              if r.get("undervalued_pct") is not None]
    out_of_band = [r["ticker"] for r in rows
                   if r.get("undervalued_pct") is not None
                   and abs(r["undervalued_pct"]) > 50.01]
    cyc = {}
    for r in rows:
        t = (r.get("cycle") or {}).get("tag")
        cyc[t] = cyc.get(t, 0) + 1
    n_3method = sum(1 for r in rows if r.get("n_methods") == 3)
    n_bottom = sum(1 for r in rows if r.get("bottom_line"))
    n_scard = sum(1 for r in rows if r.get("vs_industry"))
    gen = data.get("generated_at", "")

    report["output"] = {
        "schema_version": data.get("schema_version"),
        "generated_at": gen,
        "n_covered": data.get("n_covered"),
        "verdict_counts": data.get("verdict_counts"),
        "cycle_distribution": cyc,
        "n_sector_benchmarks": len(data.get("sector_benchmarks") or {}),
        "n_with_3_methods": n_3method,
        "n_with_bottom_line": n_bottom,
        "n_with_industry_scorecard": n_scard,
        "undervalued_pct_range": [min(unders), max(unders)] if unders else None,
        "out_of_band_pct": out_of_band,
        "changes_keys": list((data.get("changes") or {}).keys()),
    }
    # spot-check the EQT cyclical case
    for sym in ("EQT", "HUM"):
        rec = next((r for r in rows if r["ticker"] == sym), None)
        if rec:
            report[f"sample_{sym}"] = {
                "verdict": rec["verdict"], "under_pct": rec["undervalued_pct"],
                "confidence": rec["confidence"], "cycle": rec.get("cycle"),
                "bottom_line": rec.get("bottom_line"),
                "valuation_methods": rec.get("valuation_methods")}
    report["sample_top3"] = [
        {"t": r["ticker"], "verdict": r["verdict"], "under": r["undervalued_pct"],
         "conf": r["confidence"], "cycle": (r.get("cycle") or {}).get("tag"),
         "bottom": r.get("bottom_line")}
        for r in data.get("top_opportunities", [])[:3]]

    checks = {
        "invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
        "schema_is_v2": data.get("schema_version") == "2.0",
        "fresh_today": gen[:10] == datetime.now(timezone.utc).date().isoformat(),
        "universe_covered": (data.get("n_covered") or 0) >= 100,
        "sector_benchmarks_built": len(data.get("sector_benchmarks") or {}) >= 5,
        "no_out_of_band_pct": len(out_of_band) == 0,
        "three_method_working": n_3method > 50,
        "bottom_lines_present": n_bottom > 100,
        "industry_scorecard_present": n_scard > 100,
        "cycle_tags_present": len(cyc) >= 2,
    }
else:
    checks = {"output_readable": False}

try:
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "justhodl-ops/752"})
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "replace")
    checks["page_live"] = "Stock Opportunities" in html
    checks["page_v2_features"] = ("My Watchlist" in html
                                  and "normie glossary" in html)
except Exception as e:
    report["page_err"] = str(e)[:200]
    checks["page_live"] = False

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("OPPORTUNITY ENGINE v2 LIVE — 3-method valuation, cycle "
                     "detection, industry scorecard, watchlist page all verified"
                     if report["all_pass"]
                     else "REVIEW — see checks[] (a stale deploy shows schema 1.0)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/752_opportunity_v2_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/752_opportunity_v2_verify.json")
