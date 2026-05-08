#!/usr/bin/env python3
"""Step 367 — diagnose why broadcast Lambda rejects admin token.
Reads CloudWatch logs from broadcast Lambda to see the actual error."""
import io, json, os, time, zipfile
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/367_broadcast_admin_diag.json"
NAME = "justhodl-tmp-broadcast-diag"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error, time
import boto3
from botocore.exceptions import ClientError

logs = boto3.client("logs", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

BROADCAST_URL = "https://p6kvtojb2y6r4orgbtxh7ld3nu0pfktz.lambda-url.us-east-1.on.aws"

def lambda_handler(event, context):
    out = {}
    # 1. Read the same admin token the verifier reads
    try:
        token = ssm.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
        out["1_ssm_admin_token_read"] = {"ok": True, "length": len(token), "preview": token[:8]+"..."+token[-4:]}
    except Exception as e:
        out["1_ssm_admin_token_read"] = {"ok": False, "error": str(e)}
        token = None

    # 2. Try /send via Function URL with correct token + tag the request so we can find it in logs
    tag = f"diag-367-{int(time.time())}"
    if token:
        try:
            req = urllib.request.Request(BROADCAST_URL + "/", method="POST",
                data=json.dumps({"channel": "system", "tag": tag, "title": "diag", "body": "test"}).encode(),
                headers={"Content-Type": "application/json", "X-Justhodl-Admin-Token": token})
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    out["2_broadcast_admin"] = {"status": r.status, "body": r.read().decode("utf-8")[:300]}
            except urllib.error.HTTPError as e:
                out["2_broadcast_admin"] = {"status": e.code, "body": e.read().decode("utf-8")[:300]}
        except Exception as e:
            out["2_broadcast_admin"] = {"error": str(e)}

    # 3. Tail CloudWatch logs for the broadcast Lambda
    try:
        log_group = "/aws/lambda/openbb-websocket-broadcast"
        # Get most recent log streams
        streams = logs.describe_log_streams(logGroupName=log_group, orderBy="LastEventTime",
                                              descending=True, limit=3)["logStreams"]
        recent_events = []
        cutoff = int((time.time() - 300) * 1000)  # last 5 min
        for s in streams:
            evs = logs.get_log_events(logGroupName=log_group, logStreamName=s["logStreamName"],
                                       startTime=cutoff, limit=50, startFromHead=False)
            for ev in evs.get("events", []):
                recent_events.append({"ts": ev.get("timestamp"), "msg": ev.get("message", "")[:300].strip()})
        # Sort by ts desc, take last 30
        recent_events.sort(key=lambda x: x.get("ts", 0), reverse=True)
        out["3_recent_logs"] = recent_events[:30]
    except Exception as e:
        out["3_recent_logs"] = {"error": str(e)}

    # 4. Inspect broadcast Lambda config — env vars + role
    try:
        cfg = lam.get_function(FunctionName="openbb-websocket-broadcast")["Configuration"]
        out["4_lambda_config"] = {
            "handler": cfg.get("Handler"),
            "runtime": cfg.get("Runtime"),
            "role": cfg.get("Role"),
            "env": cfg.get("Environment", {}).get("Variables", {}),
            "last_modified": cfg.get("LastModified"),
            "code_size": cfg.get("CodeSize"),
        }
    except Exception as e:
        out["4_lambda_config"] = {"error": str(e)}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
