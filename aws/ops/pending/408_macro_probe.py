#!/usr/bin/env python3
"""Step 408 — Fast probe (no waiting). Async-fires the screener with
force=true, then inspects: (a) macro file schemas, (b) current data state.
The screener run completes in ~7 min on AWS's side — separate verify ops
will check populated fields later."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/408_macro_probe.json"
NAME = "justhodl-tmp-macro-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def deep_keys(d, prefix="", depth=0, max_depth=3):
    """Recursively find string-valued keys that might contain phase/posture/regime."""
    if depth > max_depth: return []
    found = []
    if isinstance(d, dict):
        for k, v in d.items():
            key_lower = k.lower()
            if isinstance(v, str) and any(t in key_lower for t in ("phase","posture","regime","stance","state","signal")):
                found.append((prefix + k, v[:60]))
            if isinstance(v, (dict, list)):
                found.extend(deep_keys(v, prefix + k + ".", depth + 1, max_depth))
    elif isinstance(d, list) and d:
        # Inspect first element only
        found.extend(deep_keys(d[0], prefix + "[0].", depth + 1, max_depth))
    return found

def lambda_handler(event, context):
    out = {}

    # 1. Probe LCE schema deeply
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/liquidity-conditions-engine.json")
        d = json.loads(obj["Body"].read())
        out["lce"] = {
            "top_keys": list(d.keys())[:30],
            "phase_like_fields": deep_keys(d)[:30],
            "size_kb": round(len(json.dumps(d)) / 1024, 1),
        }
    except Exception as e:
        out["lce_err"] = str(e)[:200]

    # 2. Probe GBC schema deeply
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        out["gbc"] = {
            "top_keys": list(d.keys())[:30],
            "phase_like_fields": deep_keys(d)[:30],
            "size_kb": round(len(json.dumps(d)) / 1024, 1),
        }
    except Exception as e:
        out["gbc_err"] = str(e)[:200]

    # 3. Current screener data state (read-only)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks") or []
        out["screener_data"] = {
            "generated_at": d.get("generated_at"),
            "n_stocks": len(stocks),
        }
        # Coverage stats for the new fields
        with_rev = sum(1 for s in stocks if s.get("revenue") is not None)
        with_inst = sum(1 for s in stocks if s.get("instOwnershipPct") is not None)
        with_steal = sum(1 for s in stocks if s.get("stealScore") is not None)
        out["screener_data"]["coverage"] = {
            "revenue_populated": with_rev,
            "inst_populated": with_inst,
            "stealscore_populated": with_steal,
        }
    except Exception as e:
        out["screener_data_err"] = str(e)[:200]

    # 4. Async-fire screener with force=true so refresh starts on AWS side
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="Event",     # async — returns immediately
            Payload=json.dumps({"force": True}).encode(),
        )
        out["force_refresh"] = {"status": resp.get("StatusCode"), "async": True}
    except Exception as e:
        out["force_refresh"] = {"error": str(e)[:200]}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
