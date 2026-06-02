"""1182 — Patch POLYGON_KEY directly + re-invoke ETF flow engine.

The buyback-scanner & equity-research Lambdas don't have POLYGON_KEY in
their env. We patch directly from the known value in userMemories.
Once data flows correctly we'll see the institutional composite signals
populate with real Polygon ETF Global data.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1182_polygon_patch.json"
BUCKET = "justhodl-dashboard-live"
FLOWS_LAMBDA = "justhodl-etf-fund-flows"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"

# Known good — from userMemories
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

cfg = Config(read_timeout=180, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

# Step 1: try SSM first; fall back to memory value
print("[1182] 1. Resolve POLYGON_KEY")
try:
    p = ssm.get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)
    POLYGON_KEY = p["Parameter"]["Value"]
    print(f"  ✓ from SSM (len {len(POLYGON_KEY)})")
    out["steps"]["resolve"] = {"source": "ssm", "len": len(POLYGON_KEY)}
except Exception:
    try:
        p = ssm.get_parameter(Name="/justhodl/polygon_key", WithDecryption=True)
        POLYGON_KEY = p["Parameter"]["Value"]
        print(f"  ✓ from SSM alt (len {len(POLYGON_KEY)})")
        out["steps"]["resolve"] = {"source": "ssm_alt", "len": len(POLYGON_KEY)}
    except Exception:
        # Search across multiple Lambdas
        for src in ["justhodl-crypto-intel", "justhodl-bloomberg-terminal-refresh",
                    "justhodl-options-flow-cache", "justhodl-portfolio-snapshot",
                    "justhodl-stock-screener"]:
            try:
                c = lam.get_function_configuration(FunctionName=src)
                env = (c.get("Environment") or {}).get("Variables", {})
                if env.get("POLYGON_KEY"):
                    POLYGON_KEY = env["POLYGON_KEY"]
                    out["steps"]["resolve"] = {"source": src, "len": len(POLYGON_KEY)}
                    print(f"  ✓ from {src} (len {len(POLYGON_KEY)})")
                    break
            except Exception:
                continue
        else:
            out["steps"]["resolve"] = {"source": "memory_fallback", "len": len(POLYGON_KEY)}
            print(f"  ✓ from memory fallback (len {len(POLYGON_KEY)})")

# Step 2: patch env
print(f"\n[1182] 2. Patch env on {FLOWS_LAMBDA}")
try:
    cur = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
    env = (cur.get("Environment") or {}).get("Variables", {})
    new_env = {**env, "POLYGON_KEY": POLYGON_KEY}
    lam.update_function_configuration(
        FunctionName=FLOWS_LAMBDA, Environment={"Variables": new_env},
    )
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["steps"]["env_patch"] = {"patched": True, "env_keys_now": list(new_env.keys())}
    print(f"  ✓ patched · env now has {len(new_env)} keys")
except Exception as e:
    out["steps"]["env_patch"] = {"error": str(e)[:300]}

time.sleep(5)

# Step 3: invoke + poll
print(f"\n[1182] 3. Invoke + poll")
def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None

try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=FLOWS_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"  async status={resp['StatusCode']}")
    # The Lambda fetches 84 ETFs in parallel + 90-day history each. Expect ~30-60s.
    for i in range(90):
        time.sleep(3)
        lm = head_lm("etf-flows/daily.json")
        if lm and lm > invoke_dt:
            elapsed = round(time.time() - invoke_t0, 1)
            obj = s3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")
            daily = json.loads(obj["Body"].read())
            n_ok = daily.get("n_ok", 0)

            top_in = sorted(
                [m for m in daily.get("metrics", []) if m.get("flow_zscore_90d") is not None],
                key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
            )[:10]
            top_out = sorted(
                [m for m in daily.get("metrics", []) if m.get("flow_zscore_90d") is not None],
                key=lambda x: x["flow_zscore_90d"] or 0
            )[:10]

            obj = s3.get_object(Bucket=BUCKET, Key="etf-flows/composite.json")
            comp = json.loads(obj["Body"].read())
            cc = comp.get("composite", {}) or {}

            out["steps"]["invoke"] = {
                "elapsed_s": elapsed,
                "lambda_elapsed_s": daily.get("elapsed_s"),
                "universe_size": daily.get("universe_size"),
                "n_ok": n_ok,
                "n_failed": daily.get("n_failed"),
                "top_inflows": [
                    {"ticker": m["ticker"], "z": m.get("flow_zscore_90d"),
                     "5d_usd": m.get("flow_5d_usd"), "pct_aum_5d": m.get("pct_aum_5d"),
                     "label": m.get("signal_label"), "subcategory": m.get("subcategory"),
                     "persistence": m.get("persistence_days")}
                    for m in top_in
                ],
                "top_outflows": [
                    {"ticker": m["ticker"], "z": m.get("flow_zscore_90d"),
                     "5d_usd": m.get("flow_5d_usd"), "pct_aum_5d": m.get("pct_aum_5d"),
                     "label": m.get("signal_label"), "subcategory": m.get("subcategory"),
                     "persistence": m.get("persistence_days")}
                    for m in top_out
                ],
                "composite": {
                    "regime": cc.get("regime"),
                    "defensive_rotation": cc.get("defensive_rotation", {}),
                    "smart_vs_dumb": cc.get("smart_vs_dumb", {}),
                    "risk_on_off": cc.get("risk_on_off", {}),
                    "domestic_vs_intl": cc.get("domestic_vs_intl", {}),
                    "growth_vs_value": cc.get("growth_vs_value", {}),
                    "credit_stress": cc.get("credit_stress", {}),
                },
                "sample_errors": [
                    {"t": m["ticker"], "err": m.get("error"), "body": m.get("body", "")[:120]}
                    for m in daily.get("metrics", []) if m.get("error")
                ][:5],
            }
            print(f"  ✓ {n_ok}/{daily.get('universe_size')} ETFs ok in {elapsed}s · regime={cc.get('regime')}")
            break
    else:
        out["steps"]["invoke"] = {"error": "poll timeout"}
except Exception as e:
    out["steps"]["invoke"] = {"error": str(e)[:300]}

# Step 4: rerun snapshot
print(f"\n[1182] 4. Rerun snapshot")
try:
    invoke_t0 = time.time()
    lam.invoke(FunctionName=SNAPSHOT_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    for i in range(40):
        time.sleep(3)
        try:
            lm = s3.head_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")["LastModified"]
            if lm > invoke_dt:
                obj = s3.get_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")
                doc = json.loads(obj["Body"].read())
                out["steps"]["snapshot"] = {
                    "n_rows": len(doc.get("rows", [])),
                    "schema_columns": list(doc.get("rows", [{}])[0].keys()) if doc.get("rows") else [],
                }
                print(f"  ✓ analytics/etf_flows_flat.json: {len(doc.get('rows', []))} rows")
                break
        except Exception:
            pass
    else:
        out["steps"]["snapshot"] = {"error": "poll timeout"}
except Exception as e:
    out["steps"]["snapshot"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1182] DONE")
