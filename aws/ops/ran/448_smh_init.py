#!/usr/bin/env python3
"""Step 448 — After deploy publishes smart-money-holdings:
  1. Force-invoke synchronously to build first inverse map
  2. Verify S3 file shape + coverage
  3. Sample a few well-known stocks to confirm holders
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/448_smh_init.json"
NAME = "justhodl-tmp-448"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

WATCH_SYMBOLS = ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "TSLA",
                  "AMZN", "BRK.B", "JPM", "BAC", "AXP", "KO",
                  "OXY", "CHTR", "CRWD", "META", "LULU"]

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-holdings")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Sync invoke (long timeout — can take 30-90s)
    try:
        resp = lam.invoke(
            FunctionName="justhodl-smart-money-holdings",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["invoke"] = inner
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # Read S3 file
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money-holdings.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["s3"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "as_of_quarter": p.get("as_of_quarter"),
            "n_funds_scanned": p.get("n_funds_scanned"),
            "n_funds_attempted": p.get("n_funds_attempted"),
            "n_symbols": p.get("n_symbols"),
        }
        # Sample watch symbols
        holdings = p.get("holdings") or {}
        out["watch_symbols"] = {}
        for sym in WATCH_SYMBOLS:
            h = holdings.get(sym) or []
            out["watch_symbols"][sym] = {
                "n_holders": len(h),
                "top3": [{"name": x["name"], "val_b": round((x.get("value") or 0)/1e9, 2),
                          "shares_m": round((x.get("shares") or 0)/1e6, 1)}
                         for x in h[:3]]
            }
        # Top 10 funds by total value
        out["top10_funds"] = [{
            "name": f.get("name"),
            "n_holdings": f.get("n_holdings"),
            "total_value_b": round((f.get("total_value") or 0)/1e9, 1),
        } for f in (p.get("funds") or [])[:10]]
        # Most-popular symbols (held by most concentrated funds)
        sym_counts = sorted([(s, len(hs)) for s, hs in holdings.items()],
                            key=lambda x: -x[1])
        out["most_popular_symbols"] = [{"sym": s, "n_funds": n} for s, n in sym_counts[:15]]
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    # Public HTTPS
    try:
        url = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/smart-money-holdings.json"
        r = urllib.request.urlopen(url, timeout=15)
        out["public_https"] = {"status": r.status, "size": len(r.read())}
    except Exception as e:
        out["public_https_err"] = str(e)[:200]

    # EventBridge schedule
    try:
        rules = events.list_rules(NamePrefix="justhodl-smart-money-holdings")
        out["eventbridge"] = [{"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                  "state": r.get("State")}
                                 for r in rules.get("Rules") or []]
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 100s for deploy...")
    time.sleep(100)
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
