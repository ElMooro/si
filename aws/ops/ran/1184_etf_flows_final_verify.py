"""1184 — Final ETF flow verification after API fixes.

The fixed Lambda now:
  - Uses order=desc to get LATEST records (not 2017 archived data)
  - Single call per ETF for snapshot + history (84 calls, not 168)
  - Computes AUM = shares_outstanding * nav (no aum field in API)
  - Aggregates 5d/21d cumulatives from returned daily flows

Run + show real institutional flow results.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1184_etf_flows_final_verify.json"
BUCKET = "justhodl-dashboard-live"
FLOWS_LAMBDA = "justhodl-etf-fund-flows"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None


print(f"[1184] 1. Confirm Lambda has POLYGON_KEY")
try:
    fn_cfg = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
    env = (fn_cfg.get("Environment") or {}).get("Variables", {})
    has_key = bool(env.get("POLYGON_KEY"))
    out["steps"]["env_check"] = {
        "has_polygon_key": has_key,
        "key_len": len(env.get("POLYGON_KEY", "")),
        "all_keys": list(env.keys()),
    }
    print(f"  POLYGON_KEY present: {has_key} (len {len(env.get('POLYGON_KEY', ''))})")
except Exception as e:
    out["steps"]["env_check"] = {"error": str(e)[:200]}

print(f"\n[1184] 2. Sync invoke (so we see Lambda exit reason)")
try:
    invoke_t0 = time.time()
    # Use RequestResponse so we get the Lambda's actual return value
    resp = lam.invoke(
        FunctionName=FLOWS_LAMBDA,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    payload = resp.get("Payload").read().decode()
    elapsed = round(time.time() - invoke_t0, 1)
    try:
        body = json.loads(payload)
    except Exception:
        body = {"raw": payload[:500]}
    out["steps"]["sync_invoke"] = {
        "elapsed_s": elapsed,
        "status_code": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": body if len(json.dumps(body, default=str)) < 4000 else "too_large",
    }
    print(f"  StatusCode={resp.get('StatusCode')} FunctionError={resp.get('FunctionError')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ Function error: {payload[:500]}")
except Exception as e:
    out["steps"]["sync_invoke"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# Read latest outputs from S3 (the Lambda wrote them as part of sync invoke)
print(f"\n[1184] 3. Read outputs from S3")
try:
    daily = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")["Body"].read())
    comp = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/composite.json")["Body"].read())
    cc = (comp.get("composite") or {})

    metrics = daily.get("metrics", [])
    n_ok = daily.get("n_ok", 0)

    # Show some sample raw data for schema verification
    samples_with_data = [m for m in metrics if not m.get("error")]
    sample_raw = samples_with_data[0] if samples_with_data else None

    top_in = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
    )[:10]
    top_out = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0
    )[:10]
    errors = [m for m in metrics if m.get("error")]

    out["steps"]["results"] = {
        "generated_at": daily.get("generated_at"),
        "lambda_elapsed_s": daily.get("elapsed_s"),
        "universe_size": daily.get("universe_size"),
        "n_ok": n_ok,
        "n_failed": daily.get("n_failed"),
        "sample_with_data": {k: v for k, v in (sample_raw or {}).items() if k != "raw_sample"},
        "n_errors": len(errors),
        "sample_errors": [
            {"t": e["ticker"], "err": e.get("error"),
             "body": (e.get("body") or "")[:200]}
            for e in errors[:5]
        ],
        "top_inflows": [
            {"t": m["ticker"], "z": m.get("flow_zscore_90d"),
             "5d_usd": m.get("flow_5d_usd"), "21d_usd": m.get("flow_21d_usd"),
             "5d_pct_aum": m.get("pct_aum_5d"), "21d_pct_aum": m.get("pct_aum_21d"),
             "label": m.get("signal_label"), "sub": m.get("subcategory"),
             "persist": m.get("persistence_days"), "n_history": m.get("n_history_points")}
            for m in top_in
        ],
        "top_outflows": [
            {"t": m["ticker"], "z": m.get("flow_zscore_90d"),
             "5d_usd": m.get("flow_5d_usd"), "21d_usd": m.get("flow_21d_usd"),
             "5d_pct_aum": m.get("pct_aum_5d"), "21d_pct_aum": m.get("pct_aum_21d"),
             "label": m.get("signal_label"), "sub": m.get("subcategory"),
             "persist": m.get("persistence_days"), "n_history": m.get("n_history_points")}
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
    }
    print(f"  n_ok: {n_ok}/{daily.get('universe_size')} · regime: {cc.get('regime')}")
except Exception as e:
    out["steps"]["results"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# Rerun snapshot
print(f"\n[1184] 4. Rerun snapshot to flatten new etf_flows data")
try:
    invoke_t0 = time.time()
    lam.invoke(FunctionName=SNAPSHOT_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    for i in range(40):
        time.sleep(3)
        try:
            lm = s3.head_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")["LastModified"]
            if lm > invoke_dt:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")["Body"].read())
                out["steps"]["snapshot"] = {
                    "n_rows": len(doc.get("rows", [])),
                    "schema_cols": list(doc.get("rows", [{}])[0].keys()) if doc.get("rows") else [],
                }
                print(f"  ✓ etf_flows_flat: {len(doc.get('rows', []))} rows")
                break
        except Exception:
            pass
except Exception as e:
    out["steps"]["snapshot"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1184] DONE")
