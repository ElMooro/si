#!/usr/bin/env python3
"""Step 438 — Force-invoke ma-tracker (force=1 to bust any in-memory cache),
verify S3 file is written, confirm the page can read it via direct S3 URL."""
import io, json, os, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/438_ma_s3_verify.json"
NAME = "justhodl-tmp-438"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Force-invoke M&A Lambda (force=1 to bypass cache, ensure fresh fetch + S3 write)
    try:
        payload = json.dumps({"queryStringParameters": {"force": "1"}}).encode()
        resp = lam.invoke(FunctionName="justhodl-ma-tracker",
                             InvocationType="RequestResponse", Payload=payload)
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
            out["invoke"] = {
                "status_code": parsed.get("statusCode"),
                "n_deals": len((inner or {}).get("deals", [])),
                "elapsed_seconds": (inner or {}).get("elapsed_seconds"),
                "generated_at": (inner or {}).get("generated_at"),
            }
        except Exception as e:
            out["invoke"] = {"parse_err": str(e), "raw": body[:300]}
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    # 2. Verify S3 file exists + check timestamp
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/ma-latest.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["s3_state"] = {
            "size_kb": round(len(body)/1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "content_type": obj.get("ContentType"),
            "cache_control": obj.get("CacheControl"),
            "generated_at": d.get("generated_at"),
            "n_deals": len(d.get("deals", [])),
            "n_profiles": len(d.get("profiles", {})),
            "summary_keys": list((d.get("summary") or {}).keys()),
        }
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    # 3. Verify the page can read it via direct HTTPS (CORS check)
    try:
        url = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/ma-latest.json"
        with urllib.request.urlopen(url, timeout=10) as r:
            n = len(r.read())
            out["public_https"] = {"status": r.status, "size_bytes": n, "url": url}
    except Exception as e:
        out["public_https_err"] = str(e)[:200]

    # 4. Verify EventBridge rule exists
    try:
        rules = events.list_rule_names_by_target(
            TargetArn="arn:aws:lambda:us-east-1:857687956942:function:justhodl-ma-tracker"
        )
        out["eventbridge"] = {"rules": rules.get("RuleNames", [])}
        for r in rules.get("RuleNames", []):
            try:
                rule = events.describe_rule(Name=r)
                out["eventbridge"][r] = {
                    "schedule": rule.get("ScheduleExpression"),
                    "state": rule.get("State"),
                }
            except Exception: pass
    except Exception as e:
        out["events_err"] = str(e)[:200]

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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
