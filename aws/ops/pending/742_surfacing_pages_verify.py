"""ops/742 — verify gap #2 + #3: the 5 new surfacing pages and their data."""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
report = {"ops": 742, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "health dashboard + 4 invisible-engine pages verify"}

# (page_url, marker, s3_key, headline_field)
TARGETS = [
    ("https://justhodl.ai/health.html", "Platform Health",
     "_health/dashboard.json", "system_status"),
    ("https://justhodl.ai/yield-curve.html", "Yield Curve",
     "data/yield-curve.json", "regime"),
    ("https://justhodl.ai/market-internals.html", "Market Internals",
     "data/market-internals.json", "state"),
    ("https://justhodl.ai/vix-curve.html", "VIX Curve",
     "data/vix-curve.json", "composite_regime"),
    ("https://justhodl.ai/position-sizer.html", "Position Sizer",
     "portfolio/sizer-v2.json", "decisive_call"),
]

results = []
for url, marker, key, field in TARGETS:
    row = {"page": url.split("/")[-1], "s3_key": key}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/742"})
        with urllib.request.urlopen(req, timeout=25) as resp:
            html = resp.read().decode("utf-8", "replace")
            row["page_status"] = resp.status
            row["page_marker"] = marker in html
    except Exception as e:
        row["page_status"] = "error"
        row["page_err"] = str(e)[:140]
        row["page_marker"] = False
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        row["sidecar_ok"] = True
        row["headline"] = d.get(field)
        row["generated_at"] = d.get("generated_at") or d.get("updated_at")
    except Exception as e:
        row["sidecar_ok"] = False
        row["sidecar_err"] = str(e)[:140]
    row["pass"] = bool(row.get("page_marker") and row.get("sidecar_ok"))
    results.append(row)

report["results"] = results
report["n_pass"] = sum(1 for r in results if r["pass"])
report["n_total"] = len(results)
report["all_pass"] = report["n_pass"] == report["n_total"]
report["verdict"] = (
    f"VERIFIED — all {report['n_total']} surfacing pages live with data"
    if report["all_pass"]
    else f"REVIEW — {report['n_pass']}/{report['n_total']} passed "
         "(GH Pages lag or a sidecar not yet written)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/742_surfacing_pages_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/742_surfacing_pages_verify.json")
