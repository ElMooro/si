"""1209 — Re-invoke fixed backtest after 'ANY ETF in top-N' methodology fix."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1209_backtest_v2_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Wait for deploy
time.sleep(90)

print(f"[1209] Invoke backtest v2")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-theme-cascade-backtest",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:1500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read output
try:
    bt = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade-backtest.json")["Body"].read())
    out["doc"] = {
        "schema_version": bt.get("schema_version"),
        "big_pumpers_5d_stats": bt.get("big_pumpers_5d_stats"),
        "pumpers_5d_stats": bt.get("pumpers_5d_stats"),
        "control_stats": bt.get("control_stats"),
        "laggards_hot_stats": bt.get("laggards_hot_stats"),
        "lift_metrics": bt.get("lift_metrics"),
        "big_pumpers_detail": (bt.get("big_pumpers_detail") or [])[:11],
        "laggards_hot_detail": (bt.get("laggards_hot_detail") or [])[:25],
    }
    print(f"\n  ✓ backtest doc loaded — interpretation: {bt.get('lift_metrics', {}).get('interpretation')}")
except Exception as e:
    out["doc"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1209] DONE")
