#!/usr/bin/env python3
"""Step 465 — Verify Stage 16.6:
  - 64 of 64 funds should be successful (no silent drops)
  - Engine No. 1 should appear with 6 holdings from Q3 2025
  - Per-fund as_of_quarter should show different fallback usage
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/465_stage16_6_verify.json"
NAME = "justhodl-tmp-465"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-holdings")
    out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}

    # Force-invoke
    resp = lam.invoke(FunctionName="justhodl-smart-money-holdings",
                        InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    parsed = json.loads(body)
    out["invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed

    # Read sidecar
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money-holdings.json")
    body = obj["Body"].read()
    p = json.loads(body)
    out["s3_size_mb"] = round(len(body)/1024/1024, 2)
    out["n_symbols"] = p.get("n_symbols")
    out["n_funds_scanned"] = p.get("n_funds_scanned")
    out["n_funds_attempted"] = p.get("n_funds_attempted")

    # All funds
    funds = p.get("funds") or []
    out["fund_count"] = len(funds)

    # Look for Engine No. 1 specifically
    engine = next((f for f in funds if f.get("cik") == "0001835549"), None)
    out["engine_no_1"] = engine

    # Show all funds with their as_of quarters (if set)
    out["all_funds_brief"] = []
    for f in funds:
        out["all_funds_brief"].append({
            "name": f.get("name"),
            "n_holdings": f.get("n_holdings"),
            "total_b": round((f.get("total_value") or 0)/1e9, 2),
            # These fields may not be in old sidecar if Lambda hasnt run yet
        })

    # Concentration stats
    holdings = p.get("holdings") or {}
    flagship = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 10)
    high_conv = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 5)
    out["stats"] = {"flagship": flagship, "high_conv": high_conv}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    _time.sleep(75)
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
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:20000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
