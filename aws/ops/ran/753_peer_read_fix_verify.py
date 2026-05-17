"""ops/753 — verify the peer-comparison read inversion is fixed.

Checks: for every stock, P/E rows where the value is ABOVE the industry
median must read 'richer than peers' (more expensive), and BELOW must
read 'cheaper'. Same logic check for a higher-is-better metric.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=160, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 753, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "peer-comparison read fix verify"}

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-engine",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["invoke_status"] = r.get("StatusCode")
    report["fn_error"] = r.get("FunctionError")
except Exception as e:
    report["invoke_err"] = str(e)[:200]

data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                Key="data/opportunities.json")["Body"].read())
rows = data.get("all", [])

pe_wrong, gm_wrong, checked_pe, checked_gm = [], [], 0, 0
for r in rows:
    for p in r.get("peer_comparison", []):
        v, pm, read = p["value"], p["industry_median"], p["read"]
        if abs(p["delta_pct"]) < 8:
            continue
        if p["key"] == "pe":   # lower is better
            checked_pe += 1
            if v > pm and "richer" not in read:
                pe_wrong.append(f"{r['ticker']}: pe {v}>{pm} read='{read}'")
            if v < pm and "cheaper" not in read:
                pe_wrong.append(f"{r['ticker']}: pe {v}<{pm} read='{read}'")
        if p["key"] == "operating_margin":  # higher is better
            checked_gm += 1
            if v > pm and "better" not in read:
                gm_wrong.append(f"{r['ticker']}: opm {v}>{pm} read='{read}'")
            if v < pm and "weaker" not in read:
                gm_wrong.append(f"{r['ticker']}: opm {v}<{pm} read='{read}'")

report["pe_rows_checked"] = checked_pe
report["pe_wrong"] = pe_wrong[:8]
report["operating_margin_rows_checked"] = checked_gm
report["operating_margin_wrong"] = gm_wrong[:8]

# show a clean sample
sample = next((r for r in rows if len(r.get("peer_comparison", [])) >= 4), {})
report["sample"] = {
    "ticker": sample.get("ticker"),
    "bottom_line": sample.get("bottom_line"),
    "peer_comparison": sample.get("peer_comparison", [])[:5]}

checks = {
    "invoke_ok": report.get("invoke_status") == 200 and not report.get("fn_error"),
    "pe_reads_all_correct": len(pe_wrong) == 0 and checked_pe > 20,
    "margin_reads_all_correct": len(gm_wrong) == 0 and checked_gm > 20,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("PEER-COMPARISON READS CORRECT — cheaper/richer/better/"
                     "weaker wording now matches the numbers"
                     if report["all_pass"] else "REVIEW — see *_wrong lists")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/753_peer_read_fix_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/753_peer_read_fix_verify.json")
