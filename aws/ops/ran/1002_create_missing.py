#!/usr/bin/env python3
"""Step 1002 — Create the two missing Lambdas DIRECTLY via boto3.

Two pushes to the deploy workflow failed to create:
  justhodl-magnitude-distributions
  justhodl-miss-detector

Their source files exist in the repo, but the GH Actions workflow either
silently dropped them or hit an unobserved error. Rather than diagnose the
workflow further, this script:

  1. Reads the embedded payload (source + config) from _create_missing_payload.json
  2. From within a temp Lambda (which has the lambda-execution-role),
     calls lambda:CreateFunction with the zipped source bytes
  3. Configures the EventBridge rule + target for each schedule
  4. Invokes each once to confirm it works
  5. Deletes the temp Lambda
  6. Writes detailed report to ops/reports/1002_*.json

This is more deterministic than waiting for another workflow round trip.
"""
import base64, io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1002_create_missing.json"
NAME = "justhodl-create-missing"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"

# Load the embedded payload (built locally before this script runs)
PAYLOAD_PATH = "aws/ops/pending/_create_missing_payload.json"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


# This inner Python is what runs inside the temp Lambda we create.
# It receives the full PAYLOAD via the invoke event, then creates the
# two real Lambdas + schedules.
WORKER = r'''
import base64
import io
import json
import time
import zipfile
import boto3

ACCOUNT_ID = "857687956942"
ROLE_ARN   = "arn:aws:iam::857687956942:role/lambda-execution-role"
REGION     = "us-east-1"

lam    = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def build_zip(files_b64: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, b64 in files_b64.items():
            zf.writestr(name, base64.b64decode(b64))
    return buf.getvalue()


def ensure_function(cfg: dict, zip_bytes: bytes) -> dict:
    fn = cfg["function_name"]
    common = dict(
        Runtime    = cfg.get("runtime", "python3.12"),
        Handler    = cfg.get("handler", "lambda_function.lambda_handler"),
        Role       = cfg.get("role", ROLE_ARN),
        Description = cfg.get("description", ""),
        Timeout    = cfg.get("timeout", 60),
        MemorySize = cfg.get("memory", 256),
    )
    try:
        lam.get_function(FunctionName=fn)
        # Exists — update code + config
        lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        lam.update_function_configuration(FunctionName=fn, **common)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        return {"action": "updated", "function_name": fn}
    except lam.exceptions.ResourceNotFoundException:
        # Create fresh
        lam.create_function(
            FunctionName = fn,
            **common,
            Code         = {"ZipFile": zip_bytes},
            Architectures = cfg.get("architectures", ["x86_64"]),
            Publish      = False,
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=fn)
        return {"action": "created", "function_name": fn}


def ensure_schedule(cfg: dict) -> dict:
    fn = cfg["function_name"]
    sched = cfg.get("schedule")
    if not sched:
        return {"scheduled": False, "reason": "no schedule in config"}
    rule_name = sched["rule_name"]
    cron_expr = sched["cron"]
    desc      = sched.get("description", "Scheduled")
    
    events.put_rule(
        Name = rule_name,
        ScheduleExpression = cron_expr,
        State = "ENABLED",
        Description = desc,
    )
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
    events.put_targets(
        Rule = rule_name,
        Targets = [{"Id": "1", "Arn": target_arn}],
    )
    statement_id = f"EventBridge-{rule_name}"
    try:
        lam.remove_permission(FunctionName=fn, StatementId=statement_id)
    except Exception:
        pass
    lam.add_permission(
        FunctionName = fn,
        StatementId  = statement_id,
        Action       = "lambda:InvokeFunction",
        Principal    = "events.amazonaws.com",
        SourceArn    = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
    )
    return {"scheduled": True, "rule_name": rule_name, "cron": cron_expr}


def invoke_once(fn: str) -> dict:
    try:
        resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8", errors="replace")
        result = {"status_code": resp.get("StatusCode"),
                  "function_error": resp.get("FunctionError")}
        try:
            parsed = json.loads(body)
            if isinstance(parsed.get("body"), str):
                result["result"] = json.loads(parsed["body"])
            else:
                result["result"] = parsed
        except Exception:
            result["body_raw"] = body[:500]
        return result
    except Exception as e:
        return {"invoke_fail": str(e)[:300]}


def lambda_handler(event, context):
    payload = event.get("payload") or {}
    out = {}
    for fn_name, spec in payload.items():
        rec = {}
        try:
            zb = build_zip(spec["files"])
            rec["zip_size"] = len(zb)
            rec["create"] = ensure_function(spec["config"], zb)
            time.sleep(2)  # let it settle
            rec["schedule"] = ensure_schedule(spec["config"])
            time.sleep(1)
            rec["invoke"] = invoke_once(fn_name)
        except Exception as e:
            rec["error"] = str(e)[:400]
        out[fn_name] = rec
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Load embedded payload
    with open(PAYLOAD_PATH) as f:
        payload = json.load(f)

    # Build worker zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", WORKER)
    worker_zip = buf.getvalue()

    # Create or update the worker Lambda
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=512, Timeout=600, Code={"ZipFile": worker_zip},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=worker_zip)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(FunctionName=NAME, Timeout=600, MemorySize=512)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    time.sleep(2)
    # Invoke with the payload
    resp = lam.invoke(
        FunctionName=NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"payload": payload}).encode("utf-8"),
    )
    body = resp["Payload"].read().decode("utf-8", errors="replace")
    out["worker_status"] = resp.get("StatusCode")
    out["worker_err"] = resp.get("FunctionError")
    try:
        parsed = json.loads(body)
        out["result"] = json.loads(parsed.get("body", "{}"))
    except Exception:
        out["raw"] = body[:3000]

    # Clean up the worker
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
