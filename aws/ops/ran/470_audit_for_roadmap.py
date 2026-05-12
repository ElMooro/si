#!/usr/bin/env python3
"""Step 470 — Locate macro regime data + audit signal-logger/calibrator state.
Need to know:
  - Where is current regime stored? (for #3 regime watchlist)
  - Does signal-logger/calibrator actually feed back? (for #6 backtest learning)
  - Existing alert/Telegram infrastructure? (for #4 alpha feed)
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/470_audit_for_roadmap.json"
NAME = "justhodl-tmp-470"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    BUCKET = "justhodl-dashboard-live"

    # 1. Find macro regime data in S3
    macro_keys_to_try = [
        "macro/regime.json",
        "macro/nowcast.json",
        "macro/lce.json",
        "khalid_index/data.json",
        "lce/data.json",
        "nowcast/data.json",
        "regime/data.json",
    ]
    out["macro_paths"] = {}
    for k in macro_keys_to_try:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=k)
            out["macro_paths"][k] = {"exists": True, "size_kb": round(head["ContentLength"]/1024, 1),
                                       "last_modified": head["LastModified"].isoformat()}
        except Exception as e:
            out["macro_paths"][k] = {"exists": False}

    # 2. List ALL S3 keys to find macro-related ones
    try:
        listing = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=200)
        keys = sorted([o["Key"] for o in listing.get("Contents") or []])
        # Filter to relevant prefixes
        relevant = [k for k in keys if any(t in k.lower() for t in 
                       ["macro","regime","khalid","lce","nowcast","forecast","backtest","calibrat","signal"])]
        out["relevant_s3_keys"] = relevant
        out["total_s3_keys"] = len(keys)
    except Exception as e:
        out["s3_list_err"] = str(e)[:200]

    # 3. List Lambda functions related to calibration/backtest/telegram/alerts
    try:
        all_funcs = []
        paginator = lam.get_paginator("list_functions")
        for page in paginator.paginate():
            for f in page["Functions"]:
                name = f["FunctionName"]
                if any(t in name.lower() for t in 
                          ["signal","outcome","calibrat","backtest","telegram","alert","brief",
                           "nowcast","khalid","macro","regime","lce","options"]):
                    all_funcs.append({
                        "name": name,
                        "last_modified": f.get("LastModified"),
                        "memory": f.get("MemorySize"),
                        "timeout": f.get("Timeout"),
                    })
        out["relevant_lambdas"] = all_funcs
    except Exception as e:
        out["lambda_list_err"] = str(e)[:200]

    # 4. DynamoDB tables related to signals/outcomes
    try:
        all_tables = []
        paginator = ddb.get_paginator("list_tables")
        for page in paginator.paginate():
            for t in page["TableNames"]:
                if any(x in t.lower() for x in ["signal","outcome","backtest","alert","trade"]):
                    desc = ddb.describe_table(TableName=t)
                    all_tables.append({
                        "name": t,
                        "item_count": desc["Table"].get("ItemCount"),
                        "size_bytes": desc["Table"].get("TableSizeBytes"),
                        "keys": [k["AttributeName"] for k in desc["Table"].get("KeySchema", [])],
                    })
        out["relevant_ddb_tables"] = all_tables
    except Exception as e:
        out["ddb_err"] = str(e)[:200]

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
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
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
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
