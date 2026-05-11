#!/usr/bin/env python3
"""Step 443 — After deploy publishes smart-money-tracker:
  1. Force-invoke synchronously to populate S3
  2. Check S3 file shape
  3. Verify field shapes match what page expects
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/443_smart_money_verify.json"
NAME = "justhodl-tmp-443"
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
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Sync invoke
    try:
        resp = lam.invoke(
            FunctionName="justhodl-smart-money-tracker",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["invoke"] = inner
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # Read S3
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["s3"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "as_of_quarter": p.get("as_of_quarter"),
            "n_filers": p.get("n_filers"),
        }
        # Sample top 5 filers
        out["top5"] = [{
            "cik": f.get("cik"),
            "name": f.get("investor_name"),
            "size": f.get("portfolio_size"),
            "added": f.get("securities_added"),
            "removed": f.get("securities_removed"),
            "mv": f.get("market_value"),
            "prev_mv": f.get("previous_market_value"),
            "qoq_pct": f.get("qoq_change_pct"),
            "net_act": f.get("net_activity"),
        } for f in (p.get("filers") or [])[:5]]
        # Summary buckets
        sm = p.get("summary") or {}
        out["summary_counts"] = {
            "most_active": len(sm.get("most_active") or []),
            "biggest_gainers": len(sm.get("biggest_gainers") or []),
            "biggest_decliners": len(sm.get("biggest_decliners") or []),
            "biggest_increasers": len(sm.get("biggest_increasers") or []),
            "biggest_reducers": len(sm.get("biggest_reducers") or []),
        }
        out["top_gainer"] = (sm.get("biggest_gainers") or [None])[0]
        out["top_decliner"] = (sm.get("biggest_decliners") or [None])[0]
        out["top_active"] = (sm.get("most_active") or [None])[0]
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    # Public HTTPS check
    try:
        url = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/smart-money.json"
        r = urllib.request.urlopen(url, timeout=15)
        out["public_https"] = {"status": r.status, "size": len(r.read())}
    except Exception as e:
        out["public_https_err"] = str(e)[:200]

    # EventBridge schedule
    try:
        rules = events.list_rules(NamePrefix="justhodl-smart-money")
        out["eventbridge"] = [{"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                  "state": r.get("State")}
                                 for r in rules.get("Rules") or []]
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for deploy...")
    time.sleep(90)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
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
