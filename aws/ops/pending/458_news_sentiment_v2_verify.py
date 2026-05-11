#!/usr/bin/env python3
"""Step 458 — Force-invoke news-sentiment v2 with FMP backend, verify
results show real bullish/bearish signals (not 503 neutrals).
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/458_news_sentiment_v2_verify.json"
NAME = "justhodl-tmp-458"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-news-sentiment")
    out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}

    # Force invoke with force=true to bypass cache
    resp = lam.invoke(
        FunctionName="justhodl-news-sentiment",
        InvocationType="RequestResponse",
        Payload=json.dumps({"force": True}).encode())
    body = resp["Payload"].read().decode("utf-8")
    parsed = json.loads(body)
    inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
    out["invoke"] = inner

    # Read fresh sidecar
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="sentiment/data.json")
    body = obj["Body"].read()
    p = json.loads(body)
    out["s3_size_kb"] = round(len(body)/1024, 1)
    out["s3_summary"] = {
        "generated_at": p.get("generated_at"),
        "model": p.get("model"),
        "source": p.get("source"),
        "stocks_with_news": p.get("stocks_with_news"),
        "stocks_scored": p.get("stocks_scored"),
        "bullish_count": p.get("bullish_count"),
        "bearish_count": p.get("bearish_count"),
        "neutral_count": p.get("neutral_count"),
        "elapsed_seconds": p.get("elapsed_seconds"),
    }
    # Top 10 most-bullish & most-bearish
    slist = p.get("sentiment") or []
    scored = [s for s in slist if s.get("sentimentSignal") != "neutral"]
    bulls = sorted(scored, key=lambda s: -s.get("sentimentScore", 0))[:10]
    bears = sorted(scored, key=lambda s: s.get("sentimentScore", 0))[:10]
    def trim(lst):
        return [{"sym": s["symbol"], "score": s["sentimentScore"],
                 "signal": s["sentimentSignal"],
                 "reason": s.get("sentimentReason","")[:120],
                 "n_headlines": len(s.get("headlines") or [])} for s in lst]
    out["top_bulls"] = trim(bulls)
    out["top_bears"] = trim(bears)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 100s for deploy...")
    _time.sleep(100)
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
