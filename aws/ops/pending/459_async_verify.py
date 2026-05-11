#!/usr/bin/env python3
"""Step 459 — Async-invoke pattern for news-sentiment v2.

Pattern: fire-and-forget invoke, then poll S3 file timestamp + read CloudWatch
logs in a loop. Returns when fresh data appears OR after 5 min cap.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/459_async_verify.json"
NAME = "justhodl-tmp-459"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-news-sentiment")
    out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}

    # Async invoke (Event type) — fire-and-forget
    started_at = time.time()
    lam.invoke(
        FunctionName="justhodl-news-sentiment",
        InvocationType="Event",
        Payload=json.dumps({"force": True}).encode())
    out["invoked_at_ts"] = started_at

    # Poll S3 for fresh file (newer than started_at)
    out["polls"] = []
    final_data = None
    for poll_n in range(20):  # 20 × 15s = 5 min max
        time.sleep(15)
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live",
                                    Key="sentiment/data.json")
            lm_ts = head["LastModified"].timestamp()
            age_since_invoke = lm_ts - started_at
            out["polls"].append({"poll": poll_n+1, "age_since_invoke_s": round(age_since_invoke, 1),
                                   "size_kb": round(head["ContentLength"]/1024, 1)})
            if age_since_invoke > 0:  # fresh file
                # Read it
                obj = s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="sentiment/data.json")
                final_data = json.loads(obj["Body"].read())
                break
        except Exception as e:
            out["polls"].append({"poll": poll_n+1, "err": str(e)[:100]})

    if final_data:
        out["fresh"] = True
        out["summary"] = {
            "generated_at": final_data.get("generated_at"),
            "model": final_data.get("model"),
            "source": final_data.get("source"),
            "stocks_with_news": final_data.get("stocks_with_news"),
            "stocks_scored": final_data.get("stocks_scored"),
            "bullish_count": final_data.get("bullish_count"),
            "bearish_count": final_data.get("bearish_count"),
            "neutral_count": final_data.get("neutral_count"),
            "elapsed_seconds": final_data.get("elapsed_seconds"),
        }
        slist = final_data.get("sentiment") or []
        non_neut = [s for s in slist if s.get("sentimentSignal") != "neutral"]
        bulls = sorted(non_neut, key=lambda s: -s.get("sentimentScore", 0))[:10]
        bears = sorted(non_neut, key=lambda s: s.get("sentimentScore", 0))[:10]
        def trim(lst):
            return [{"sym": s["symbol"], "score": s["sentimentScore"],
                      "signal": s["sentimentSignal"],
                      "reason": s.get("sentimentReason","")[:130]} for s in lst]
        out["top_bulls"] = trim(bulls)
        out["top_bears"] = trim(bears)
    else:
        out["fresh"] = False
        # Fetch latest log events for diagnostics
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-news-sentiment",
                orderBy="LastEventTime", descending=True, limit=2)
            evs = []
            for s in streams.get("logStreams", [])[:1]:
                e = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-news-sentiment",
                    logStreamName=s["logStreamName"], limit=30, startFromHead=False)
                for ev in e.get("events", []):
                    evs.append({"ts": ev["timestamp"], "msg": ev["message"].strip()[:300]})
            evs.sort(key=lambda x: -x["ts"])
            out["latest_logs"] = evs[:20]
        except Exception as e:
            out["logs_err"] = str(e)[:200]
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
                            MemorySize=256, Timeout=600, Code={"ZipFile": zb})
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
