"""1214 — Roll out 3 Polygon engines (options-flow, fx-regime, futures-curves) + invoke each."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1214_polygon_3engines_rollout.json"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

LAMBDAS = [
    {
        "name": "justhodl-polygon-options-flow",
        "source_dir": "aws/lambdas/justhodl-polygon-options-flow/source",
        "memory": 1024, "timeout": 300,
        "description": "Polygon Options Starter ($29/m) — unusual options activity",
        "rule_name": "justhodl-polygon-options-flow-hourly",
        "schedule": "cron(15 14,15,16,17,18,19 * * MON-FRI *)",
        "output_key": "data/polygon-options-flow.json",
    },
    {
        "name": "justhodl-polygon-fx-regime",
        "source_dir": "aws/lambdas/justhodl-polygon-fx-regime/source",
        "memory": 512, "timeout": 60,
        "description": "Polygon Currencies Starter ($49/m) — FX regime detector",
        "rule_name": "justhodl-polygon-fx-regime-daily",
        "schedule": "cron(30 12 * * MON-FRI *)",
        "output_key": "data/polygon-fx-regime.json",
    },
    {
        "name": "justhodl-polygon-futures-curves",
        "source_dir": "aws/lambdas/justhodl-polygon-futures-curves/source",
        "memory": 512, "timeout": 90,
        "description": "Polygon Futures Starter ($29/m) — VIX/oil/gold curves",
        "rule_name": "justhodl-polygon-futures-curves-daily",
        "schedule": "cron(0 13 * * MON-FRI *)",
        "output_key": "data/polygon-futures-curves.json",
    },
]

out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}


def build_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, source_dir)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


for cfg_l in LAMBDAS:
    name = cfg_l["name"]
    print(f"\n[1214] === {name} ===")
    lam_out = {}

    # Create
    try:
        lam.get_function_configuration(FunctionName=name)
        lam_out["create"] = "exists"
        print(f"  ✓ exists")
    except lam.exceptions.ResourceNotFoundException:
        try:
            zip_bytes = build_zip(cfg_l["source_dir"])
            resp = lam.create_function(
                FunctionName=name, Runtime="python3.12", Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Description=cfg_l["description"],
                Timeout=cfg_l["timeout"], MemorySize=cfg_l["memory"],
                Architectures=["x86_64"], Publish=False,
            )
            lam_out["create"] = "created"
            # Wait active
            for _ in range(30):
                time.sleep(2)
                c = lam.get_function_configuration(FunctionName=name)
                if c.get("State") == "Active":
                    break
            print(f"  ✓ created")
        except Exception as e:
            lam_out["create_error"] = str(e)[:300]
            print(f"  ❌ create: {str(e)[:200]}")
            out["lambdas"][name] = lam_out
            continue

    # Schedule
    try:
        events.put_rule(Name=cfg_l["rule_name"],
                         ScheduleExpression=cfg_l["schedule"],
                         State="ENABLED",
                         Description=cfg_l["description"])
        fn = lam.get_function(FunctionName=name)
        events.put_targets(
            Rule=cfg_l["rule_name"],
            Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}],
        )
        try:
            lam.add_permission(
                FunctionName=name,
                StatementId=f"EBInvoke-{cfg_l['rule_name']}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{cfg_l['rule_name']}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        lam_out["schedule"] = cfg_l["schedule"]
        print(f"  ✓ scheduled: {cfg_l['schedule']}")
    except Exception as e:
        lam_out["schedule_error"] = str(e)[:300]

    # Invoke
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        payload = resp.get("Payload").read().decode()
        lam_out["invoke"] = {
            "elapsed_s": elapsed,
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1200],
        }
        print(f"  invoke: status={resp.get('StatusCode')} elapsed={elapsed}s")
        if resp.get("FunctionError"):
            print(f"  ⚠ {payload[:400]}")
    except Exception as e:
        lam_out["invoke_error"] = str(e)[:300]

    # Verify output JSON
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=cfg_l["output_key"])["Body"].read())
        lam_out["output"] = {
            "key": cfg_l["output_key"],
            "size_kb": round(len(json.dumps(doc)) / 1024, 1),
            "generated_at": doc.get("generated_at"),
            "elapsed_s": doc.get("elapsed_s"),
            # Lambda-specific fields
            "n_scanned": doc.get("n_scanned"),
            "n_extreme": doc.get("n_extreme"),
            "n_bullish": doc.get("n_bullish"),
            "n_pairs": doc.get("n_pairs"),
            "n_products_with_data": doc.get("n_products_with_data"),
            "regime_signals": doc.get("regime_signals", [])[:5],
            "signals": doc.get("signals", [])[:5],
            "extreme_call_flow_top5": [
                {"ticker": e.get("ticker"), "cv_pv": e.get("cv_pv_ratio"),
                 "vol": e.get("total_vol"), "signals": e.get("signals", [])[:2]}
                for e in (doc.get("extreme_call_flow") or [])[:5]
            ],
            "bullish_call_flow_top5": [
                {"ticker": b.get("ticker"), "cv_pv": b.get("cv_pv_ratio"),
                 "vol": b.get("total_vol"), "signals": b.get("signals", [])[:2]}
                for b in (doc.get("bullish_call_flow") or [])[:5]
            ],
            "regime_metrics": doc.get("regime_metrics"),
            "pair_data_keys": list((doc.get("pair_data") or {}).keys()),
            "product_data_summary": {
                product: [c.get("ticker") for c in contracts]
                for product, contracts in (doc.get("product_data") or {}).items()
            } if doc.get("product_data") else None,
        }
        print(f"  ✓ output: {cfg_l['output_key']} ({lam_out['output']['size_kb']} KB)")
    except Exception as e:
        lam_out["output_error"] = str(e)[:200]
        print(f"  ⚠ output: {e}")

    out["lambdas"][name] = lam_out


# Also invoke the prepump-alerts-router to see if it picks up the new signals
print(f"\n[1214] === Re-invoke prepump-alerts-router (should pick up 3 new sources) ===")
try:
    # Reset state to force re-evaluation
    state_obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json")
    state = json.loads(state_obj["Body"].read())
    # Add empty buckets for new signal types if not present
    for nt in ["options_flow", "fx_regime", "futures_curves"]:
        state.setdefault("alerted_by_signal", {}).setdefault(nt, [])
    # Don't actually reset — that would re-send all prior alerts. We just want
    # the 3 NEW signal sources to fire on their first encounter.

    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prepump-alerts-router",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["router_reinvoke"] = {
        "elapsed_s": elapsed, "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:1500],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  counts: {inner.get('counts')}")
        except:
            pass
except Exception as e:
    out["router_reinvoke"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1214] DONE")
