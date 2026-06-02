"""1207 — Verify fixed theme cascade. Sync invoke + show hot themes + candidates."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1207_theme_cascade_v2_verify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-theme-cascade"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

print(f"[1207] Sync invoke {LAMBDA} (v2 schema fix)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:2000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade.json")["Body"].read())
    out["doc"] = {
        "schema_version": doc.get("schema_version"),
        "macro_regime": doc.get("macro_regime"),
        "n_themes_tracked": doc.get("n_themes_tracked"),
        "n_tickers_mapped": doc.get("n_tickers_mapped"),
        "n_total_ranked": doc.get("n_total_ranked"),
        "n_alert_tier": doc.get("n_alert_tier"),
        "n_medium_tier": doc.get("n_medium_tier"),
        "n_watch_tier": doc.get("n_watch_tier"),
        "top_hot_themes": doc.get("top_hot_themes", [])[:15],
        "alert_tier": doc.get("alert_tier", []),
        "medium_tier": doc.get("medium_tier", []),
        "watch_tier_top10": (doc.get("watch_tier") or [])[:10],
    }
    print(f"\n  schema: {doc.get('schema_version')}")
    print(f"  themes_tracked: {doc.get('n_themes_tracked')}  tickers_mapped: {doc.get('n_tickers_mapped')}")
    print(f"  alert(>=80): {doc.get('n_alert_tier')}  medium(50-79): {doc.get('n_medium_tier')}  watch(<50): {doc.get('n_watch_tier')}")
except Exception as e:
    out["doc"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1207] DONE")
