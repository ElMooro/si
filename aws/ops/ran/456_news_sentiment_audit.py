#!/usr/bin/env python3
"""Step 456 — Audit news-sentiment Lambda state:
  1. Lambda last-invoke time + last-modified
  2. S3 sentiment/data.json size + age + sample
  3. EventBridge rule state
  4. Screener page reference check
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/456_news_sentiment_audit.json"
NAME = "justhodl-tmp-456"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Lambda config
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-news-sentiment")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            "function_url_arn": cfg.get("Configuration", {}).get("FunctionUrl"),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # Recent invocations from CloudWatch logs
    try:
        log_group = "/aws/lambda/justhodl-news-sentiment"
        streams = logs.describe_log_streams(logGroupName=log_group,
            orderBy="LastEventTime", descending=True, limit=3)
        out["recent_streams"] = [{
            "stream_name": s["logStreamName"],
            "last_event_ms": s.get("lastEventTimestamp"),
        } for s in streams.get("logStreams", [])]
    except Exception as e:
        out["logs_err"] = str(e)[:200]

    # EventBridge rule
    try:
        rule = events.describe_rule(Name="justhodl-sentiment-daily")
        out["eventbridge"] = {
            "schedule": rule.get("ScheduleExpression"),
            "state": rule.get("State"),
            "description": rule.get("Description"),
        }
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    # S3 sentiment file
    try:
        obj = s3.head_object(Bucket="justhodl-dashboard-live", Key="sentiment/data.json")
        out["s3_head"] = {
            "size_kb": round(obj["ContentLength"]/1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "age_hours": round((time.time() - obj["LastModified"].timestamp())/3600, 1),
        }
        # Fetch content
        body = s3.get_object(Bucket="justhodl-dashboard-live", Key="sentiment/data.json")["Body"].read()
        data = json.loads(body)
        out["s3_data"] = {
            "generated_at": data.get("generated_at"),
            "count": data.get("count"),
            "bullish_count": data.get("bullish_count"),
            "bearish_count": data.get("bearish_count"),
            "neutral_count": data.get("neutral_count"),
            "elapsed_seconds": data.get("elapsed_seconds"),
        }
        # Sample 5 stocks with non-neutral sentiment
        slist = data.get("sentiment") or []
        non_neutral = [s for s in slist if s.get("sentimentSignal") != "neutral"]
        out["s3_samples"] = non_neutral[:8]
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
import time as _t

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _t.sleep(2)
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
