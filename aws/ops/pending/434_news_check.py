#!/usr/bin/env python3
"""Step 434 — Directly check data.json + alerts log without waiting."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/434_news_check.json"
NAME = "justhodl-tmp-434"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Check current data.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        stocks = d.get("stocks") or []
        out["data"] = {
            "generated_at": d.get("generated_at"),
            "size_kb": round(len(body)/1024, 1),
            "n_stocks": len(stocks),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        # News field coverage
        cov = {}
        for f in ["newsCount30d","newsCount7d","newsSentiment30d","latestHeadline"]:
            cov[f] = sum(1 for s in stocks if s.get(f) is not None)
        out["news_coverage"] = cov

        # Top news momentum
        top = sorted([s for s in stocks if (s.get("newsCount7d") or 0) > 0],
                      key=lambda x: -(x.get("newsCount7d") or 0))[:12]
        out["top_news_volume"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                      "c30": s.get("newsCount30d"), "c7": s.get("newsCount7d"),
                                      "sent": s.get("newsSentiment30d"),
                                      "headline": (s.get("latestHeadline") or "")[:80]}
                                     for s in top]
        # Top positive sentiment
        pos = sorted([s for s in stocks if (s.get("newsCount30d") or 0) >= 5],
                       key=lambda x: -(x.get("newsSentiment30d") or 0))[:8]
        out["top_pos_sentiment"] = [{"sym": s["symbol"], "c30": s.get("newsCount30d"),
                                        "sent": s.get("newsSentiment30d"),
                                        "headline": (s.get("latestHeadline") or "")[:80]}
                                       for s in pos]
        # Top negative sentiment
        neg = sorted([s for s in stocks if (s.get("newsCount30d") or 0) >= 5],
                       key=lambda x: (x.get("newsSentiment30d") or 0))[:8]
        out["top_neg_sentiment"] = [{"sym": s["symbol"], "c30": s.get("newsCount30d"),
                                        "sent": s.get("newsSentiment30d"),
                                        "headline": (s.get("latestHeadline") or "")[:80]}
                                       for s in neg]
    except Exception as e:
        out["err"] = str(e)[:200]

    # Screener log tail
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=80)
            for e in ev.get("events", []):
                m = e["message"].strip()
                if any(k in m for k in ("DONE:","[alerts]","[inst]","[history]","REPORT",
                                          "[just-crossed]")):
                    lines.append((e["timestamp"], m[:200]))
        lines.sort()
        out["screener_log"] = [{"ts": ts, "msg": m} for ts, m in lines[-15:]]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    # Alerts Lambda log tail
    try:
        lg = "/aws/lambda/justhodl-screener-alerts"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=30)
            for e in ev.get("events", []):
                lines.append((e["timestamp"], e["message"].strip()[:200]))
        lines.sort()
        out["alerts_log"] = [{"ts": ts, "msg": m} for ts, m in lines[-15:]]
    except Exception as e:
        out["alerts_log_err"] = str(e)[:200]

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
                            MemorySize=512, Timeout=60, Code={"ZipFile": zb})
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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
