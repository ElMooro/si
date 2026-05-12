#!/usr/bin/env python3
"""Step 475 — Trigger daily-brief + alpha-alerts to verify Telegram delivery
end-to-end. Returns the Claude-synthesized brief markdown so we can show it."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/475_brief_alerts_test.json"
NAME = "justhodl-tmp-475"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Invoke alpha-daily-brief
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-daily-brief",
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["brief_invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
    except Exception as e:
        out["brief_invoke_err"] = str(e)[:400]

    # 2. Read the brief markdown
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alpha-brief.md")
        out["brief_md"] = obj["Body"].read().decode("utf-8")
        out["brief_size_kb"] = round(len(out["brief_md"])/1024, 2)
    except Exception as e:
        out["brief_md_err"] = str(e)[:200]

    # 3. Invoke alpha-alerts (will fire if any new tier-S/A/regime change)
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-alerts",
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["alerts_invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
    except Exception as e:
        out["alerts_invoke_err"] = str(e)[:400]

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
                            MemorySize=512, Timeout=240, Code={"ZipFile": zb})
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
