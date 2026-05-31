#!/usr/bin/env python3
"""Step 1003 — Direct-create the two missing Lambdas (description ≤240 chars).

1002 failed because the Description field exceeded AWS's 256-char limit.
Description in config.json now truncated to 78 + 36 chars respectively;
worker also defensively clips to 240 chars at write time.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1003_create_missing_v2.json"
NAME = "justhodl-create-missing-v2"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

PAYLOAD_PATH = "aws/ops/pending/_create_missing_payload.json"

lam = boto3.client("lambda", region_name="us-east-1")


WORKER = r'''
import base64, io, json, time, zipfile
import boto3

ACCOUNT_ID = "857687956942"
ROLE_ARN   = "arn:aws:iam::857687956942:role/lambda-execution-role"
REGION     = "us-east-1"

lam    = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def build_zip(files_b64):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, b64 in files_b64.items():
            zf.writestr(name, base64.b64decode(b64))
    return buf.getvalue()


def ensure_function(cfg, zip_bytes):
    fn = cfg["function_name"]
    desc = (cfg.get("description") or "")[:240]  # defensive truncation
    common = dict(
        Runtime    = cfg.get("runtime", "python3.12"),
        Handler    = cfg.get("handler", "lambda_function.lambda_handler"),
        Role       = cfg.get("role", ROLE_ARN),
        Description = desc,
        Timeout    = cfg.get("timeout", 60),
        MemorySize = cfg.get("memory", 256),
    )
    try:
        lam.get_function(FunctionName=fn)
        lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        lam.update_function_configuration(FunctionName=fn, **common)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        return {"action": "updated"}
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=fn, **common,
            Code={"ZipFile": zip_bytes},
            Architectures=cfg.get("architectures", ["x86_64"]),
            Publish=False,
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=fn)
        return {"action": "created"}


def ensure_schedule(cfg):
    fn = cfg["function_name"]
    sched = cfg.get("schedule")
    if not sched:
        return {"scheduled": False}
    rule = sched["rule_name"]
    events.put_rule(Name=rule, ScheduleExpression=sched["cron"],
                     State="ENABLED", Description=sched.get("description", "")[:240])
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
    events.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
    sid = f"EventBridge-{rule}"
    try: lam.remove_permission(FunctionName=fn, StatementId=sid)
    except Exception: pass
    lam.add_permission(
        FunctionName=fn, StatementId=sid,
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule}",
    )
    return {"scheduled": True, "rule": rule, "cron": sched["cron"]}


def invoke_once(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:500]
        return out
    except Exception as e:
        return {"fail": str(e)[:300]}


def lambda_handler(event, context):
    payload = event.get("payload") or {}
    out = {}
    for name, spec in payload.items():
        try:
            zb = build_zip(spec["files"])
            create = ensure_function(spec["config"], zb)
            time.sleep(2)
            schedule = ensure_schedule(spec["config"])
            time.sleep(1)
            invoke = invoke_once(name)
            out[name] = {"zip_size": len(zb), "create": create,
                         "schedule": schedule, "invoke": invoke}
        except Exception as e:
            out[name] = {"error": str(e)[:400]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    with open(PAYLOAD_PATH) as f:
        payload = json.load(f)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", WORKER)
    worker_zip = buf.getvalue()

    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=512, Timeout=600, Code={"ZipFile": worker_zip})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=worker_zip)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(FunctionName=NAME, Timeout=600, MemorySize=512)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
        Payload=json.dumps({"payload": payload}).encode("utf-8"))
    body = resp["Payload"].read().decode("utf-8", errors="replace")
    out["worker_status"] = resp.get("StatusCode")
    out["worker_err"] = resp.get("FunctionError")
    try:
        parsed = json.loads(body)
        out["result"] = json.loads(parsed.get("body", "{}"))
    except Exception:
        out["raw"] = body[:3000]

    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
