#!/usr/bin/env python3
"""Step 439 — After deploy-lambdas publishes justhodl-cot-tracker, invoke it
synchronously to populate S3 immediately (otherwise we wait until Friday)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/439_cot_init.json"
NAME = "justhodl-tmp-439"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-cot-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Synchronous invoke to populate S3 now
    try:
        resp = lam.invoke(
            FunctionName="justhodl-cot-tracker",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["invoke"] = {
            "status_code": parsed.get("statusCode"),
            "n_contracts": inner.get("n_contracts"),
            "elapsed_seconds": inner.get("elapsed_seconds"),
            "extreme_long": inner.get("extreme_long"),
            "extreme_short": inner.get("extreme_short"),
        }
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # Confirm S3 file exists + read it
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/cot-latest.json")
        body = obj["Body"].read()
        out["s3_state"] = {
            "size_kb": round(len(body)/1024, 1),
            "last_modified": obj["LastModified"].isoformat() if obj["LastModified"] else None,
        }
        p = json.loads(body)
        out["s3_state"]["n_contracts"] = len(p.get("contracts") or [])
        out["s3_state"]["summary"] = p.get("summary", {})
        # Sample 5 contracts
        contracts = p.get("contracts") or []
        out["sample_contracts"] = [{
            "sym": c.get("symbol"), "name": c.get("name"), "sec": c.get("sector"),
            "long_pct": c.get("current_long_pct"), "short_pct": c.get("current_short_pct"),
            "net": c.get("net_position_pct"), "z": c.get("z_score_3y"),
            "signal": c.get("extreme_signal"), "date": c.get("date"),
        } for c in contracts[:8]]
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    # Test public HTTPS access (CORS-free read from page)
    try:
        url = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/cot-latest.json"
        r = urllib.request.urlopen(url, timeout=15)
        out["public_https"] = {"status": r.status, "size": len(r.read())}
    except Exception as e:
        out["public_https_err"] = str(e)[:200]

    # Confirm EventBridge schedule
    try:
        rules = events.list_rules(NamePrefix="justhodl-cot")
        out["eventbridge"] = [{"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                  "state": r.get("State")}
                                 for r in rules.get("Rules") or []]
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for deploy-lambdas to publish justhodl-cot-tracker...")
    time.sleep(90)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=180, Code={"ZipFile": zb})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
