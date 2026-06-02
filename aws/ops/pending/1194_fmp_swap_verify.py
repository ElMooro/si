"""1194 — Constituents Lambda env swap (POLYGON_KEY → FMP_KEY) + verify.

The deploy pipeline will update the code zip. We need to manually update
the env vars since the inherit_env config doesn't auto-trigger after
config.json changes.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1194_fmp_swap_verify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-etf-constituents"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


# Step 1: Pull FMP_KEY from equity-research env
print("[1194] 1. Pull FMP_KEY from equity-research Lambda")
try:
    c = lam.get_function_configuration(FunctionName="justhodl-equity-research")
    src_env = (c.get("Environment") or {}).get("Variables", {})
    fmp_key = src_env.get("FMP_KEY")
    if not fmp_key:
        out["steps"]["pull_fmp"] = {"error": "FMP_KEY not in equity-research env"}
        raise SystemExit(1)
    out["steps"]["pull_fmp"] = {"ok": True, "len": len(fmp_key)}
    print(f"  ✓ found FMP_KEY len={len(fmp_key)}")
except Exception as e:
    out["steps"]["pull_fmp"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")
    raise SystemExit(1)


# Step 2: Set FMP_KEY on constituents Lambda (replace POLYGON_KEY)
print(f"\n[1194] 2. Update {LAMBDA} env: FMP_KEY (remove POLYGON_KEY)")
try:
    c = lam.get_function_configuration(FunctionName=LAMBDA)
    cur_env = (c.get("Environment") or {}).get("Variables", {})
    new_env = {k: v for k, v in cur_env.items() if k != "POLYGON_KEY"}
    new_env["FMP_KEY"] = fmp_key
    lam.update_function_configuration(
        FunctionName=LAMBDA,
        Environment={"Variables": new_env},
    )
    # Wait for update
    for _ in range(20):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["steps"]["env_swap"] = {
        "ok": True,
        "removed": ["POLYGON_KEY"] if "POLYGON_KEY" in cur_env else [],
        "added": ["FMP_KEY"],
        "current_env_keys": list(new_env.keys()),
    }
    print(f"  ✓ env updated · keys now: {list(new_env.keys())}")
except Exception as e:
    out["steps"]["env_swap"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")


# Step 3: Sync invoke (~10s for 13 ETFs × 1 FMP call each)
print(f"\n[1194] 3. Sync invoke (FMP holdings for ~13 high-z ETFs)")
try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - invoke_t0, 1)
    payload = resp.get("Payload").read().decode()
    out["steps"]["invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:1000],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["steps"]["invoke"] = {"error": str(e)[:300]}


# Step 4: Read constituent-pressure.json + show top stocks
print(f"\n[1194] 4. Read etf-flows/constituent-pressure.json")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/constituent-pressure.json")["Body"].read())
    out["steps"]["constituent_output"] = {
        "generated_at": doc.get("generated_at"),
        "n_high_z_etfs": doc.get("n_high_z_etfs"),
        "n_etfs_with_constituents": doc.get("n_etfs_with_constituents"),
        "threshold_z": doc.get("threshold_z"),
        "high_z_etfs": [
            {k: v for k, v in e.items() if k in
              ["ticker","zscore_90d","flow_5d_usd","flow_21d_usd","signal_label","n_constituents_fetched"]}
            for e in doc.get("high_z_etfs", [])
        ],
        "top_30_pressure": [
            {
                "stock": p.get("stock"),
                "name": p.get("name"),
                "dominant_direction": p.get("dominant_direction"),
                "total_pressure_5d_usd": p.get("total_pressure_5d_usd"),
                "total_pressure_21d_usd": p.get("total_pressure_21d_usd"),
                "n_etfs_pressuring": p.get("n_etfs_pressuring"),
                "cumulative_etf_weight_pct": p.get("cumulative_etf_weight_pct"),
                "contributing_etfs_top3": [
                    {"etf": e.get("etf"),
                     "weight_pct": e.get("weight_pct"),
                     "etf_zscore": e.get("etf_zscore"),
                     "implied_pressure_5d": e.get("implied_pressure_5d_usd")}
                    for e in (p.get("contributing_etfs") or [])[:3]
                ],
            }
            for p in (doc.get("top_constituents_by_pressure") or [])[:30]
        ],
    }
    print(f"  ✓ generated_at: {doc.get('generated_at')}")
    print(f"  ✓ high_z_etfs: {doc.get('n_high_z_etfs')}")
    print(f"  ✓ etfs_with_constituents: {doc.get('n_etfs_with_constituents')}")
    print(f"  ✓ stocks under pressure: {len(doc.get('top_constituents_by_pressure') or [])}")
except Exception as e:
    out["steps"]["constituent_output"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1194] DONE")
