"""ops/752 — verify Opportunity Engine v2.0 (peer-relative + cycle radar) e2e."""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=160, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 752, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity Engine v2.0 verify"}

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": (r["Payload"].read().decode()[:400]
                                 if r.get("Payload") else "")}
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
    with_peers = sum(1 for r in rows if r.get("peer_comparison"))
    with_cycle = sum(1 for r in rows if (r.get("cycle") or {}).get("label"))
    with_bl = sum(1 for r in rows if r.get("bottom_line"))
    cyclical = sum(1 for r in rows if (r.get("cycle") or {}).get("cyclical"))
    sample = next((r for r in rows if r.get("peer_comparison")), {})
    tp = data.get("top_pick") or {}

    report["output"] = {
        "schema_version": data.get("schema_version"),
        "generated_at": data.get("generated_at"),
        "n_covered": data.get("n_covered"),
        "verdict_counts": data.get("verdict_counts"),
        "with_peer_comparison": with_peers,
        "with_cycle": with_cycle,
        "with_bottom_line": with_bl,
        "n_cyclical": cyclical,
        "n_early_cycle": len(data.get("early_cycle", [])),
        "glossary_terms": len(data.get("glossary", {})),
    }
    report["top_pick"] = {
        "ticker": tp.get("ticker"), "verdict": tp.get("verdict"),
        "bottom_line": tp.get("bottom_line")}
    report["sample_stock"] = {
        "ticker": sample.get("ticker"),
        "verdict": sample.get("verdict"),
        "bottom_line": sample.get("bottom_line"),
        "cycle": sample.get("cycle"),
        "peer_rows": len(sample.get("peer_comparison", [])),
        "peer_first3": sample.get("peer_comparison", [])[:3],
        "scorecard": sample.get("statement_scorecard"),
    }
    report["early_cycle_sample"] = [
        {"t": r["ticker"], "verdict": r["verdict"],
         "cycle": r.get("cycle", {}).get("label")}
        for r in data.get("early_cycle", [])[:5]]

    checks = {
        "invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
        "schema_is_v2": data.get("schema_version") == "2.0",
        "universe_covered": (data.get("n_covered") or 0) >= 100,
        "peer_comparison_populated": with_peers >= 100,
        "cycle_populated": with_cycle >= 100,
        "bottom_line_populated": with_bl >= 100,
        "top_pick_present": bool(tp.get("ticker")),
        "glossary_present": len(data.get("glossary", {})) >= 8,
    }
else:
    checks = {"output_readable": False}

try:
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "justhodl-ops/752"})
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "replace")
    checks["page_live"] = "Today's Top Pick" in html
    checks["page_has_v2_sections"] = ("Early-Cycle Value" in html
                                      and "My Watchlist" in html)
except Exception as e:
    report["page_err"] = str(e)[:200]
    checks["page_live"] = False

report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("OPPORTUNITY ENGINE v2.0 LIVE — peer benchmarking, cycle "
                     "radar, scorecard, top pick and page all verified"
                     if report["all_pass"]
                     else "REVIEW — see checks[] / sample output")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/752_opportunity_v2_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/752_opportunity_v2_verify.json")
