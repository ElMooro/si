#!/usr/bin/env python3
"""Step 485 — Inspect options-flow output schema, verify schedule, and capture
real options-flow data for several known tickers so we know the exact shape
to ingest in the alpha-score Lambda."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/485_options_flow_inspect.json"
NAME = "justhodl-tmp-485"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    
    # Lambda config
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-options-flow-scanner")
        out["lambda"] = {
            "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
            "last_modified": cfg["LastModified"][:19],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:200]
    
    # EventBridge schedule
    for rule_name in ["justhodl-options-flow-scanner-daily", "justhodl-options-flow-scanner-hourly",
                       "justhodl-options-flow-scanner"]:
        try:
            r = events.describe_rule(Name=rule_name)
            out["schedule"] = {"name": rule_name, "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")}
            break
        except Exception: continue
    
    # Check S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/options-flow.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "exists": True, "size_kb": round(len(body)/1024, 1),
            "last_modified": obj["LastModified"].isoformat()[:19],
            "top_keys": list(p.keys()) if isinstance(p, dict) else None,
            "generated_at": p.get("generated_at"),
        }
        # Get the structure of the actual scored results
        results = p.get("results") or p.get("scored") or p.get("tier_a") or []
        if isinstance(results, list) and results:
            out["sidecar"]["n_results"] = len(results)
            out["sidecar"]["sample_first_3"] = results[:3]
        if isinstance(p.get("tier_a"), list):
            out["sidecar"]["n_tier_a"] = len(p["tier_a"])
            out["sidecar"]["tier_a_first_3"] = p["tier_a"][:3]
        if isinstance(p.get("tier_b"), list):
            out["sidecar"]["n_tier_b"] = len(p["tier_b"])
            out["sidecar"]["tier_b_first_3"] = p["tier_b"][:3]
        # Look for top-level summary fields
        out["sidecar"]["summary"] = {k: v for k, v in p.items()
                                       if not isinstance(v, (list, dict))}
    except Exception as e:
        out["sidecar"] = {"exists": False, "err": str(e)[:300]}
    
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
                            MemorySize=512, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
