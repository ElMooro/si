"""1183 — Test both Polygon keys for ETF Global Fund Flows entitlement.

User has two Polygon keys:
  - default:          zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d
  - desperate_lamarr: Out4PAHPLWSG6uoeQVSgGUsyN2AnVFPI

ETF Global Fund Flows ($99/mo) is per-key. Test both with the actual
endpoint to find which is entitled, then patch the Lambda with the
winner. Also check current S3 state to understand why 1182 timed out.
"""
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1183_polygon_keys_test.json"
BUCKET = "justhodl-dashboard-live"
FLOWS_LAMBDA = "justhodl-etf-fund-flows"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"

KEY_DEFAULT  = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
KEY_DESPERATE = "Out4PAHPLWSG6uoeQVSgGUsyN2AnVFPI"

cfg = Config(read_timeout=180, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def test_etf_flows_key(key: str, name: str) -> dict:
    """Test if this key has ETF Global Fund Flows entitlement."""
    url = f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker=SPY&apiKey={key}"
    info = {"name": name, "key_prefix": key[:10] + "...", "key_len": len(key)}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-KeyTest/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            status = r.status
            body = json.loads(r.read())
            info["status"] = status
            info["polygon_status"] = body.get("status")
            info["count"] = body.get("count")
            info["has_results"] = bool(body.get("results"))
            if body.get("results"):
                first = body["results"][0]
                # Show all keys in the first result so we know the schema
                info["sample_keys"] = list(first.keys())
                info["sample_row"] = first
            info["entitled"] = True
            return info
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="ignore")[:400]
        except Exception:
            pass
        info["status"] = e.code
        info["error"] = f"HTTP {e.code}"
        info["body"] = body_text
        info["entitled"] = False
        return info
    except Exception as e:
        info["error"] = str(e)[:200]
        info["entitled"] = False
        return info


# Step 1: test both keys
print("[1183] 1. Test both keys for ETF Global entitlement")
for key, name in [(KEY_DEFAULT, "default"), (KEY_DESPERATE, "desperate_lamarr")]:
    print(f"  Testing {name}...")
    result = test_etf_flows_key(key, name)
    out["steps"][f"key_{name}"] = result
    if result.get("entitled"):
        sk = result.get("sample_keys") or []
        print(f"    ✓ ENTITLED · sample_keys={sk[:8]}...")
    else:
        print(f"    ✗ NOT entitled · {result.get('error')} · {(result.get('body') or '')[:100]}")

# Determine which key to use
winner_key = None
winner_name = None
for name, key in [("desperate_lamarr", KEY_DESPERATE), ("default", KEY_DEFAULT)]:
    r = out["steps"].get(f"key_{name}", {})
    if r.get("entitled"):
        winner_key = key
        winner_name = name
        break

if not winner_key:
    print("\n  ❌ NEITHER key has ETF Global Fund Flows entitlement")
    out["steps"]["resolution"] = {"winner": None}
else:
    print(f"\n  ✓ Using {winner_name}")
    out["steps"]["resolution"] = {"winner": winner_name, "key_len": len(winner_key)}

    # Step 2: patch Lambda
    print(f"\n[1183] 2. Patch {FLOWS_LAMBDA} with winning key")
    try:
        cur = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
        env = (cur.get("Environment") or {}).get("Variables", {})
        env["POLYGON_KEY"] = winner_key
        lam.update_function_configuration(
            FunctionName=FLOWS_LAMBDA, Environment={"Variables": env},
        )
        for _ in range(15):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["steps"]["patch"] = {"ok": True, "env_keys": list(env.keys())}
        print(f"  ✓ patched")
    except Exception as e:
        out["steps"]["patch"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")

    time.sleep(5)

    # Step 3: invoke + poll with longer timeout (5 min)
    print(f"\n[1183] 3. Invoke + poll (up to 5 min)")
    def head_lm(k):
        try:
            return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
        except Exception:
            return None

    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=FLOWS_LAMBDA, InvocationType="Event", Payload=b"{}")
        invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
        print(f"  async invoke {resp['StatusCode']}, polling...")
        for i in range(100):  # ~5 min
            time.sleep(3)
            lm = head_lm("etf-flows/daily.json")
            if lm and lm > invoke_dt:
                elapsed = round(time.time() - invoke_t0, 1)
                daily = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")["Body"].read())
                comp = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/composite.json")["Body"].read())

                n_ok = daily.get("n_ok", 0)
                metrics = daily.get("metrics", [])

                top_in = sorted(
                    [m for m in metrics if m.get("flow_zscore_90d") is not None],
                    key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
                )[:10]
                top_out = sorted(
                    [m for m in metrics if m.get("flow_zscore_90d") is not None],
                    key=lambda x: x["flow_zscore_90d"] or 0
                )[:10]
                cc = (comp.get("composite") or {})

                out["steps"]["invoke"] = {
                    "elapsed_s": elapsed,
                    "lambda_elapsed_s": daily.get("elapsed_s"),
                    "universe_size": daily.get("universe_size"),
                    "n_ok": n_ok,
                    "n_failed": daily.get("n_failed"),
                    "top_inflows": [
                        {"t": m["ticker"], "z": m.get("flow_zscore_90d"),
                         "5d": m.get("flow_5d_usd"), "5d_pct_aum": m.get("pct_aum_5d"),
                         "label": m.get("signal_label"), "sub": m.get("subcategory"),
                         "persist": m.get("persistence_days")}
                        for m in top_in
                    ],
                    "top_outflows": [
                        {"t": m["ticker"], "z": m.get("flow_zscore_90d"),
                         "5d": m.get("flow_5d_usd"), "5d_pct_aum": m.get("pct_aum_5d"),
                         "label": m.get("signal_label"), "sub": m.get("subcategory"),
                         "persist": m.get("persistence_days")}
                        for m in top_out
                    ],
                    "composite": {
                        "regime": cc.get("regime"),
                        "scores": {
                            "defensive_rotation": cc.get("defensive_rotation", {}).get("score"),
                            "smart_vs_dumb": cc.get("smart_vs_dumb", {}).get("score"),
                            "risk_on_off": cc.get("risk_on_off", {}).get("score"),
                            "domestic_vs_intl": cc.get("domestic_vs_intl", {}).get("score"),
                            "growth_vs_value": cc.get("growth_vs_value", {}).get("score"),
                            "credit_stress": cc.get("credit_stress", {}).get("score"),
                        },
                        "labels": {
                            "defensive_rotation": cc.get("defensive_rotation", {}).get("label"),
                            "smart_vs_dumb": cc.get("smart_vs_dumb", {}).get("label"),
                            "risk_on_off": cc.get("risk_on_off", {}).get("label"),
                            "domestic_vs_intl": cc.get("domestic_vs_intl", {}).get("label"),
                            "growth_vs_value": cc.get("growth_vs_value", {}).get("label"),
                            "credit_stress": cc.get("credit_stress", {}).get("label"),
                        },
                    },
                    "sample_errors": [
                        {"t": m["ticker"], "err": m.get("error"), "body": (m.get("body") or "")[:200]}
                        for m in metrics if m.get("error")
                    ][:5],
                }
                print(f"  ✓ {n_ok}/{daily.get('universe_size')} ETFs in {elapsed}s · regime={cc.get('regime')}")
                break
        else:
            out["steps"]["invoke"] = {"error": "poll timeout"}
    except Exception as e:
        out["steps"]["invoke"] = {"error": str(e)[:300]}

    # Step 4: rerun snapshot
    print(f"\n[1183] 4. Rerun snapshot")
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
                        "sample_cols": list(doc.get("rows", [{}])[0].keys()) if doc.get("rows") else [],
                    }
                    print(f"  ✓ etf_flows_flat.json: {len(doc.get('rows', []))} rows")
                    break
            except Exception:
                pass
    except Exception as e:
        out["steps"]["snapshot"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1183] DONE")
