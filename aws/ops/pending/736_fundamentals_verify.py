"""ops/736 — verify justhodl-fundamentals-engine end-to-end.

Invokes the engine, reads data/fundamentals.json and confirms the FMP
reads actually populated (DCF gaps, segmentation, health scores), then
checks the frontend is live.
"""
import json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=620, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 736, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "fundamentals-engine end-to-end verify"}

try:
    r = lam.invoke(FunctionName="justhodl-fundamentals-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "response": body[:400]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:250]}

time.sleep(5)

d = None
try:
    o = s3.get_object(Bucket=BUCKET, Key="data/fundamentals.json")
    d = json.loads(o["Body"].read())
    report["last_modified"] = o["LastModified"].isoformat()
except Exception as e:
    report["sidecar_error"] = str(e)[:200]

if d:
    cs = d.get("companies", [])
    with_gap = [c for c in cs if c.get("dcf_gap_pct") is not None]
    with_geo = [c for c in cs if c.get("geo_mix")]
    with_prod = [c for c in cs if c.get("product_mix")]
    with_az = [c for c in cs if c.get("altman_z") is not None]
    report["summary"] = {
        "schema_version": d.get("schema_version"),
        "universe_source": d.get("universe_source"),
        "n_covered": d.get("n_covered"),
        "n_failed": d.get("n_failed"),
        "n_with_dcf_gap": len(with_gap),
        "n_with_geo_mix": len(with_geo),
        "n_with_product_mix": len(with_prod),
        "n_with_altman_z": len(with_az),
        "engine_summary": d.get("summary"),
    }
    report["sample"] = [
        {k: c.get(k) for k in ("ticker", "price", "dcf", "dcf_gap_pct",
                               "valuation_label", "altman_z", "piotroski",
                               "top_region", "top_product")}
        for c in cs[:5]]

# frontend
try:
    req = urllib.request.Request("https://justhodl.ai/fundamentals.html",
                                 headers={"User-Agent": "justhodl-ops/736"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        html = resp.read().decode("utf-8", "replace")
        report["frontend"] = {"status": resp.status,
                              "marker": "Fundamentals X-Ray" in html}
except Exception as e:
    report["frontend"] = {"status": "error", "err": str(e)[:160]}

checks = {
    "invoke_ok": report["invoke"].get("status") == 200
                 and report["invoke"].get("fn_error") is None,
    "sidecar_valid": bool(d) and d.get("schema_version") == "1.0",
    "covers_companies": bool(d) and (d.get("n_covered") or 0) >= 5,
    "dcf_populated": bool(d) and len(
        [c for c in d.get("companies", []) if c.get("dcf_gap_pct") is not None]) >= 3,
    "segmentation_populated": bool(d) and len(
        [c for c in d.get("companies", []) if c.get("geo_mix")]) >= 1,
    "health_populated": bool(d) and len(
        [c for c in d.get("companies", []) if c.get("altman_z") is not None]) >= 3,
    "frontend_live": isinstance(report.get("frontend"), dict)
                     and report["frontend"].get("marker") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — Fundamentals X-Ray live: DCF, segmentation and health "
    "scores populated; frontend rendering"
    if report["all_pass"]
    else "REVIEW — see checks (FMP field shapes or GH Pages lag)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/736_fundamentals_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/736_fundamentals_verify.json")
