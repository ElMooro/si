"""1170 — Re-invoke snapshot Lambda with fixed flattener + verify fields populated."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1170_snapshot_reverify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-analytics-snapshot"

cfg = Config(read_timeout=30, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Pre-invoke timestamps
def head_lm(key):
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
    except Exception:
        return None

pre = {
    "research": head_lm("analytics/equity_research_flat.json"),
    "edgar":    head_lm("analytics/edgar_insiders_flat.json"),
}
print(f"pre-invoke: {pre}")

# Async invoke
invoke_t0 = time.time()
resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="Event", Payload=b"{}")
print(f"async invoke status: {resp['StatusCode']}")
invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)

# Poll for updates
print("polling S3 for updates...")
poll_start = time.time()
for i in range(60):
    time.sleep(3)
    cur_r = head_lm("analytics/equity_research_flat.json")
    cur_e = head_lm("analytics/edgar_insiders_flat.json")
    if cur_r and cur_r > invoke_dt and cur_e and cur_e > invoke_dt:
        print(f"✓ both updated after {round(time.time()-poll_start,1)}s")
        break

# Fetch + analyze content
research_obj = s3.get_object(Bucket=BUCKET, Key="analytics/equity_research_flat.json")
research_doc = json.loads(research_obj["Body"].read())
rows = research_doc.get("rows", [])

# Count non-null fields per row
field_completeness = {}
all_cols = list(rows[0].keys()) if rows else []
for col in all_cols:
    n_non_null = sum(1 for r in rows if r.get(col) is not None)
    field_completeness[col] = {"non_null": n_non_null, "pct": round(100 * n_non_null / max(len(rows), 1), 1)}

# Sample 3 rows with KEY fields
out["sample_rows"] = []
for r in rows[:3]:
    out["sample_rows"].append({
        "ticker": r.get("ticker"),
        "rating": r.get("rating"),
        "upside_pct": r.get("upside_pct"),
        "pe_ttm": r.get("pe_ttm"),
        "roic_ttm_pct": r.get("roic_ttm_pct"),
        "revenue_5yr_cagr": r.get("revenue_5yr_cagr"),
        "eps_5yr_cagr": r.get("eps_5yr_cagr"),
        "return_1yr_pct": r.get("return_1yr_pct"),
        "cagr_5yr_pct": r.get("cagr_5yr_pct"),
        "dcf_estimate": r.get("dcf_estimate"),
        "fcf_yield_pct": r.get("fcf_yield_pct"),
    })

# Headlines
out["n_rows"] = len(rows)
out["schema_n_cols"] = len(all_cols)
out["completeness_summary"] = {
    "fields_100pct": sum(1 for v in field_completeness.values() if v["pct"] == 100),
    "fields_75plus": sum(1 for v in field_completeness.values() if v["pct"] >= 75),
    "fields_under50": sum(1 for v in field_completeness.values() if v["pct"] < 50),
    "fields_zero":   sum(1 for v in field_completeness.values() if v["pct"] == 0),
}
out["field_completeness"] = field_completeness

# Key fields that should be populated
key_fields = ["rating", "upside_pct", "pe_ttm", "roic_ttm_pct",
              "revenue_5yr_cagr", "eps_5yr_cagr", "return_1yr_pct", "cagr_5yr_pct",
              "fcf_yield_pct", "ev_upside_pct"]
out["key_fields_check"] = {k: field_completeness.get(k, {}).get("pct", 0) for k in key_fields}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"[1170] DONE — {len(rows)} rows, {len(all_cols)} cols")
