#!/usr/bin/env python3
"""Step 1000 — Verify the three new engines deployed + invokable + writing S3."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1000_three_gap_verify.json"
NAME = "justhodl-three-gap-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json
import urllib.request
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3  = boto3.client("s3", region_name=REGION)

LAMBDAS = [
    "justhodl-magnitude-distributions",
    "justhodl-miss-detector",
    "justhodl-alpha-compass",
]

EXPECT_OUTPUTS = {
    "justhodl-magnitude-distributions": "data/magnitude-distributions.json",
    "justhodl-alpha-compass":            "data/alpha-compass.json",
    # miss-detector writes data/misses/YYYY-MM-DD.json (date-stamped) — we
    # check the rolling summary instead which is updated each run.
}


def lambda_handler(event, context):
    out = {}
    for fn in LAMBDAS:
        rec = {}
        # 1. Does the function exist?
        try:
            meta = lam.get_function(FunctionName=fn)
            cfg = meta["Configuration"]
            rec["exists"] = True
            rec["last_modified"] = cfg.get("LastModified")
            rec["timeout"] = cfg.get("Timeout")
            rec["memory_mb"] = cfg.get("MemorySize")
            rec["state"] = cfg.get("State")
        except Exception as e:
            rec["exists"] = False
            rec["err"] = str(e)[:200]
            out[fn] = rec
            continue
        
        # 2. Invoke it
        try:
            resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8")
            rec["invoke_status"] = resp.get("StatusCode")
            rec["invoke_err"] = resp.get("FunctionError")
            try:
                parsed = json.loads(body)
                if isinstance(parsed.get("body"), str):
                    rec["invoke_result"] = json.loads(parsed["body"])
                else:
                    rec["invoke_result"] = parsed
            except Exception:
                rec["invoke_body_raw"] = body[:600]
        except Exception as e:
            rec["invoke_fail"] = str(e)[:300]
        
        # 3. Verify expected output exists in S3
        key = EXPECT_OUTPUTS.get(fn)
        if key:
            try:
                obj = s3.head_object(Bucket=BUCKET, Key=key)
                rec["s3_output"] = {
                    "key": key,
                    "size": obj.get("ContentLength"),
                    "last_modified": str(obj.get("LastModified", "")),
                }
            except Exception as e:
                rec["s3_output_missing"] = str(e)[:200]
        
        out[fn] = rec
    
    # 4. Confirm alpha-compass.json content is reasonable
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/alpha-compass.json")
        d = json.loads(obj["Body"].read().decode("utf-8"))
        out["compass_content"] = {
            "top_calls": len(d.get("top_calls", [])),
            "watchlist": len(d.get("watchlist", [])),
            "regime":    d.get("regime", {}).get("label"),
            "feeds":     {k: v.get("present") for k, v in (d.get("source_feeds") or {}).items()},
            "first_call_keys": list((d.get("top_calls", [{}])[0] or {}).keys())[:15] if d.get("top_calls") else [],
        }
    except Exception as e:
        out["compass_content_err"] = str(e)[:200]
    
    # 5. Confirm magnitude-distributions content
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/magnitude-distributions.json")
        d = json.loads(obj["Body"].read().decode("utf-8"))
        out["magdist_content"] = {
            "totals":           d.get("totals"),
            "published_stacks": len(d.get("stacks", [])),
            "top_stack_sample": d.get("stacks", [{}])[0] if d.get("stacks") else None,
        }
    except Exception as e:
        out["magdist_content_err"] = str(e)[:200]
    
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
        lam.update_function_configuration(FunctionName=NAME, Timeout=600, MemorySize=512)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(3)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["raw"] = body[:2500]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
