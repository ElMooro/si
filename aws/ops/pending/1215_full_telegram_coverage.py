"""1215 — Re-invoke prepump-alerts-router with 20 sources; verify full Telegram coverage."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1215_full_telegram_coverage.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Wait for deploy
time.sleep(90)

# Step 1: Check which source data files actually exist in S3
print("[1215] 1. Verify data files exist for each new checker")
data_files = [
    ("activist_filings", "data/activist-13d.json"),
    ("insider_clusters", "data/insider-clusters.json"),
    ("squeeze_pretrigger", "data/squeeze-pretrigger.json"),
    ("dealer_gex", "data/dealer-gex.json"),
    ("redflag_alerter", "data/redflag-alerter.json"),
    ("divcut_warning", "data/divcut-warning.json"),
    ("breadth_thrust", "data/breadth-thrust.json"),
    ("capitulation", "data/capitulation.json"),
    ("52wk_breakout", "data/52wk-quality-breakout.json"),
    ("crisis_composite", "data/crisis-composite.json"),
]
file_status = {}
for name, key in data_files:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        file_status[name] = {
            "exists": True, "key": key,
            "last_modified": head["LastModified"].isoformat()[:19],
            "size_kb": round(head["ContentLength"] / 1024, 1),
        }
        print(f"  ✓ {name:22s}: {key} ({file_status[name]['size_kb']} KB, modified {file_status[name]['last_modified']})")
    except Exception as e:
        file_status[name] = {"exists": False, "key": key, "error": str(e)[:80]}
        print(f"  ✗ {name:22s}: {key} — {str(e)[:60]}")
out["data_file_status"] = file_status

# Step 2: Re-invoke router
print("\n[1215] 2. Invoke prepump-alerts-router (now 20 sources)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prepump-alerts-router",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["router_invoke"] = {
        "elapsed_s": elapsed, "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:2500],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
    else:
        try:
            outer = json.loads(payload)
            inner = json.loads(outer.get("body", "{}"))
            print(f"  msgs_sent={inner.get('n_messages_sent')}  counts: {inner.get('counts')}")
        except Exception:
            pass
except Exception as e:
    out["router_invoke"] = {"error": str(e)[:300]}

# Step 3: Read state file to see which signals are now tracked
print("\n[1215] 3. Read alert state file")
try:
    state = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json")["Body"].read())
    out["state"] = state
    alerted = state.get("alerted_by_signal", {})
    print(f"  Total signal types tracked: {len(alerted)}")
    for sig_type, keys in sorted(alerted.items()):
        print(f"    {sig_type:25s}: {len(keys)} alerts → {keys[:3]}")
except Exception as e:
    out["state"] = {"error": str(e)[:200]}

# Step 4: System-wide audit — all Lambdas, all schedules, telegram coverage
print("\n[1215] 4. System-wide audit")
events = boto3.client("events", region_name="us-east-1", config=cfg)
try:
    # Count scheduled alerting Lambdas
    routers_with_tg = []
    pag = lam.get_paginator("list_functions")
    all_lambdas = []
    for page in pag.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if "justhodl" in n:
                all_lambdas.append({"name": n, "timeout": f.get("Timeout")})
                if "router" in n or "alert" in n or "digest" in n or "escalat" in n or "brief-tg" in n:
                    routers_with_tg.append(n)
    out["audit"] = {
        "n_justhodl_lambdas": len(all_lambdas),
        "telegram_routers": sorted(routers_with_tg),
    }
    print(f"  Total justhodl Lambdas: {len(all_lambdas)}")
    print(f"  Telegram-routing Lambdas:")
    for r in sorted(routers_with_tg):
        print(f"    • {r}")
except Exception as e:
    out["audit"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1215] DONE")
