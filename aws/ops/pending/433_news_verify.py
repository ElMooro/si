#!/usr/bin/env python3
"""Step 433 — Fire screener refresh with news endpoint added. Wait, then
verify news fields populated + check that chained alerts Lambda fired
automatically (should appear in CloudWatch logs)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/433_news_verify.json"
NAME = "justhodl-tmp-433"
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

    # 1. Fire screener (async)
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="Event",
            Payload=json.dumps({"force": True}).encode())
        out["fire"] = {"status": resp.get("StatusCode")}
    except Exception as e:
        out["fire_err"] = str(e)[:200]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Wait for screener to complete (~3.5 min)
    print("Waiting 240s for screener to complete...")
    time.sleep(240)

    # 3. Read data.json + check news coverage
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
        for f in ["newsCount30d","newsCount7d","newsSentiment30d","latestHeadline","latestNewsDate"]:
            cov[f] = sum(1 for s in stocks if s.get(f) is not None)
        out["news_coverage"] = cov

        # Top news momentum
        top = sorted([s for s in stocks if (s.get("newsCount7d") or 0) > 0],
                      key=lambda x: -(x.get("newsCount7d") or 0))[:15]
        out["top_news_volume"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                      "count_30d": s.get("newsCount30d"),
                                      "count_7d": s.get("newsCount7d"),
                                      "sentiment": s.get("newsSentiment30d"),
                                      "headline": (s.get("latestHeadline") or "")[:70]}
                                     for s in top]

        # Top positive sentiment (sorted by sentiment desc, min 5 articles for stability)
        pos = sorted([s for s in stocks if (s.get("newsCount30d") or 0) >= 5
                                                and (s.get("newsSentiment30d") or -999) > -100],
                       key=lambda x: -(x.get("newsSentiment30d") or 0))[:10]
        out["top_pos_sentiment"] = [{"sym": s["symbol"],
                                        "count_30d": s.get("newsCount30d"),
                                        "sentiment": s.get("newsSentiment30d"),
                                        "headline": (s.get("latestHeadline") or "")[:70]}
                                       for s in pos]

        # Top negative sentiment
        neg = sorted([s for s in stocks if (s.get("newsCount30d") or 0) >= 5
                                                and (s.get("newsSentiment30d") or 999) < 100],
                       key=lambda x: (x.get("newsSentiment30d") or 0))[:10]
        out["top_neg_sentiment"] = [{"sym": s["symbol"],
                                        "count_30d": s.get("newsCount30d"),
                                        "sentiment": s.get("newsSentiment30d"),
                                        "headline": (s.get("latestHeadline") or "")[:70]}
                                       for s in neg]
    except Exception as e:
        out["data_err"] = str(e)[:200]

    # 4. Check that chained alerts Lambda fired (look for invocation in its log)
    try:
        lg = "/aws/lambda/justhodl-screener-alerts"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        recent_lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=20)
            for e in ev.get("events", []):
                recent_lines.append((e["timestamp"], e["message"].strip()[:180]))
        recent_lines.sort()
        out["alerts_log"] = [{"ts": ts, "msg": m} for ts, m in recent_lines[-10:]]
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
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
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
