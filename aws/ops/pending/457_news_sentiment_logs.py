#!/usr/bin/env python3
"""Step 457 — Read actual log events from latest justhodl-news-sentiment run
to figure out why S3 sentiment/data.json doesn't exist despite daily runs.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/457_news_sentiment_logs.json"
NAME = "justhodl-tmp-457"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Check S3 file with proper time import
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live", Key="sentiment/data.json")
        out["sentiment_data_json"] = {
            "exists": True,
            "size_kb": round(head["ContentLength"]/1024, 1),
            "last_modified": head["LastModified"].isoformat(),
            "age_hours": round((time.time() - head["LastModified"].timestamp())/3600, 1),
        }
    except Exception as e:
        out["sentiment_data_json"] = {"exists": False, "err": str(e)[:200]}

    # List all keys under sentiment/
    try:
        listing = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="sentiment/")
        out["sentiment_prefix_keys"] = [{
            "key": o["Key"],
            "size_kb": round(o["Size"]/1024, 1),
            "last_modified": o["LastModified"].isoformat(),
        } for o in listing.get("Contents") or []]
    except Exception as e:
        out["s3_list_err"] = str(e)[:200]

    # Read recent log events from the news-sentiment Lambda
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-news-sentiment",
            orderBy="LastEventTime", descending=True, limit=2)
        all_events = []
        for s in streams.get("logStreams", []):
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-news-sentiment",
                logStreamName=s["logStreamName"], limit=50, startFromHead=False)
            for e in ev.get("events", []):
                all_events.append({
                    "ts": e["timestamp"],
                    "msg": e["message"].strip()[:300],
                    "stream": s["logStreamName"][-30:],
                })
        all_events.sort(key=lambda e: -e["ts"])
        out["recent_log_events"] = all_events[:40]
    except Exception as e:
        out["logs_err"] = str(e)[:300]

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
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
