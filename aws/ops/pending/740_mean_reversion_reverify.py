"""ops/740 — verify justhodl-mean-reversion + the screener-page integration.

Invokes the engine, reads screener/mean-reversion.json, confirms the
mean-reversion prices populated, and checks the screener page now
carries the Mean Rev columns + the loader.
"""
import json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=620, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 740, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "mean-reversion engine + screener integration"}

try:
    r = lam.invoke(FunctionName="justhodl-mean-reversion",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "response": body[:300]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:220]}

time.sleep(5)
d = None
try:
    d = json.loads(s3.get_object(Bucket=BUCKET,
                                 Key="screener/mean-reversion.json")["Body"].read())
except Exception as e:
    report["sidecar_error"] = str(e)[:180]

if d:
    cs = d.get("stocks", [])
    priced = [s for s in cs if s.get("mr_price") is not None]
    report["summary"] = {
        "schema": d.get("schema_version"), "count": d.get("count"),
        "n_priced": d.get("n_priced"),
        "n_cheap": d.get("n_cheap_vs_history"),
        "n_rich": d.get("n_rich_vs_history")}
    # cheapest vs own history (largest positive mr_upside)
    ranked = sorted(priced, key=lambda s: s.get("mr_upside_pct") or -999,
                    reverse=True)
    report["cheapest_vs_history"] = [
        {k: s.get(k) for k in ("symbol", "mr_price", "mr_upside_pct",
                               "current_pe", "median_pe", "label")}
        for s in ranked[:5]]
    report["richest_vs_history"] = [
        {k: s.get(k) for k in ("symbol", "mr_price", "mr_upside_pct",
                               "current_pe", "median_pe", "label")}
        for s in ranked[-5:]]

# screener page
try:
    req = urllib.request.Request("https://justhodl.ai/screener/",
                                 headers={"User-Agent": "justhodl-ops/740"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        html = resp.read().decode("utf-8", "replace")
        report["screener_page"] = {
            "status": resp.status,
            "has_mr_url": "mean-reversion.json" in html,
            "has_loader": "loadMeanReversion" in html,
            "has_columns": "mrUpsidePct" in html and "Mean Rev" in html}
except Exception as e:
    report["screener_page"] = {"status": "error", "err": str(e)[:160]}

checks = {
    "invoke_ok": report["invoke"].get("status") == 200
                 and report["invoke"].get("fn_error") is None,
    "sidecar_valid": bool(d) and d.get("schema_version") == "1.0",
    "prices_populated": bool(d) and (d.get("n_priced") or 0) >= 100,
    "page_has_integration": isinstance(report.get("screener_page"), dict)
        and report["screener_page"].get("has_mr_url")
        and report["screener_page"].get("has_loader")
        and report["screener_page"].get("has_columns"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — mean-reversion engine live; screener page carries the "
    "Mean Rev $ / Mean Rev % columns and loader"
    if report["all_pass"] else "REVIEW — see checks")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/740_mean_reversion_reverify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/740_mean_reversion_reverify.json")
