#!/usr/bin/env python3
"""Step 410 — Inspect CloudWatch logs of justhodl-stock-screener to see
why the data isn't being refreshed (errors? rate limits? still running?)"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/410_screener_logs.json"
NAME = "justhodl-tmp-screener-logs"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
logs = boto3.client("logs", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    lg = "/aws/lambda/justhodl-stock-screener"

    # Latest log streams
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime",
            descending=True, limit=5)
        out["recent_streams"] = []
        all_lines = []
        for st in streams.get("logStreams", []):
            stream_name = st["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream_name,
                                      startFromHead=False, limit=100)
            lines = [(e["timestamp"], e["message"].strip()) for e in ev.get("events", [])]
            out["recent_streams"].append({
                "stream": stream_name,
                "first_event_ts": st.get("firstEventTimestamp"),
                "last_event_ts": st.get("lastEventTimestamp"),
                "n_lines_fetched": len(lines),
            })
            for ts, msg in lines:
                all_lines.append((ts, stream_name[:10], msg))
        # Show most recent 80 lines across all streams
        all_lines.sort()
        recent = all_lines[-100:]
        out["log_tail"] = [{"ts": ts, "stream": st, "msg": m[:240]}
                            for ts, st, m in recent]
    except Exception as e:
        out["log_err"] = str(e)[:300]

    # Check screener current state
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks") or []
        out["data"] = {
            "generated_at": d.get("generated_at"),
            "with_revenue": sum(1 for s in stocks if s.get("revenue") is not None),
            "with_steal": sum(1 for s in stocks if s.get("stealScore") is not None),
        }
    except Exception as e:
        out["data_err"] = str(e)[:200]

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
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
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
