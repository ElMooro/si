#!/usr/bin/env python3
"""Step 445 — Verify smart-money-tracker v2 returns proper QoQ data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/445_smart_money_v2.json"
NAME = "justhodl-tmp-445"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

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

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["s3_size_kb"] = round(len(body)/1024, 1)
        out["as_of"] = p.get("as_of_date")
        out["n_filers"] = p.get("n_filers")
        # Top 10 by AUM with QoQ
        out["top10"] = [{
            "cik": f.get("cik"),
            "name": (f.get("investor_name") or "")[:42],
            "mv_b": round((f.get("market_value") or 0)/1e9, 1),
            "qoq_pct": f.get("qoq_change_pct"),
            "added": f.get("securities_added"),
            "removed": f.get("securities_removed"),
            "size": f.get("portfolio_size"),
            "perf_pct": f.get("performance_pct"),
        } for f in (p.get("filers") or [])[:10]]
        # Summary buckets
        sm = p.get("summary") or {}
        out["biggest_gainers"] = sm.get("biggest_gainers", [])[:5]
        out["biggest_decliners"] = sm.get("biggest_decliners", [])[:5]
        out["best_performers"] = sm.get("best_performers", [])[:5]
        out["most_active"] = sm.get("most_active", [])[:5]
    except Exception as e:
        out["s3_err"] = str(e)[:200]
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
                            MemorySize=256, Timeout=240, Code={"ZipFile": zb})
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
