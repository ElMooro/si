"""1210 — Invoke cascade v3 with position sizing + laggards, verify all surfaces."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1210_cascade_sizing_laggards_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}
time.sleep(75)  # let deploy settle

print(f"[1210] Invoke justhodl-theme-cascade")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-theme-cascade",
                       InvocationType="RequestResponse", Payload=b"{}")
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

# Read output
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade.json")["Body"].read())
    out["doc"] = {
        "schema_version": doc.get("schema_version"),
        "generated_at": doc.get("generated_at"),
        "n_alert_tier": doc.get("n_alert_tier"),
        "n_medium_tier": doc.get("n_medium_tier"),
        "n_laggards_hot_themes": doc.get("n_laggards_hot_themes"),
        "earnings_within_3d": doc.get("earnings_within_3d"),
        "alert_tier": doc.get("alert_tier", []),
        "medium_tier": doc.get("medium_tier", [])[:5],
        "laggards_hot_themes": doc.get("laggards_hot_themes", []),
    }
    print(f"\n  schema: {doc.get('schema_version')}")
    print(f"  alert_tier: {doc.get('n_alert_tier')} · medium: {doc.get('n_medium_tier')}")
    print(f"  laggards: {doc.get('n_laggards_hot_themes')}")
    print(f"  earnings within 3d: {doc.get('earnings_within_3d')}")
except Exception as e:
    out["doc"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1210] DONE")
