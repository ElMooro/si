#!/usr/bin/env python3
"""Step 1001 — Re-verify all 3 after deploy retrigger + alpha-compass bug fix."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1001_three_gap_verify_v2.json"
NAME = "justhodl-three-gap-v2"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json
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


def lambda_handler(event, context):
    out = {}
    for fn in LAMBDAS:
        rec = {}
        try:
            meta = lam.get_function(FunctionName=fn)
            cfg = meta["Configuration"]
            rec["exists"] = True
            rec["last_modified"] = cfg.get("LastModified")
            rec["state"] = cfg.get("State")
        except Exception as e:
            rec["exists"] = False
            rec["err"] = str(e)[:200]
            out[fn] = rec
            continue
        
        # Invoke
        try:
            resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8")
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
        
        out[fn] = rec
    
    # Check S3 outputs
    out["s3_outputs"] = {}
    for key in ("data/magnitude-distributions.json",
                "data/alpha-compass.json",
                "data/miss-summary.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=key)
            out["s3_outputs"][key] = {"size": obj["ContentLength"],
                                       "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3_outputs"][key] = {"missing": str(e)[:100]}
    
    # Sample alpha-compass content
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/alpha-compass.json")
        d = json.loads(obj["Body"].read().decode("utf-8"))
        out["compass_sample"] = {
            "top_calls": len(d.get("top_calls", [])),
            "watchlist": len(d.get("watchlist", [])),
            "regime":    d.get("regime", {}).get("label"),
            "feeds":     {k: v.get("present") for k, v in (d.get("source_feeds") or {}).items()},
            "first_call": d.get("top_calls", [{}])[0] if d.get("top_calls") else None,
        }
    except Exception as e:
        out["compass_sample_err"] = str(e)[:200]
    
    # Sample magnitude content
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/magnitude-distributions.json")
        d = json.loads(obj["Body"].read().decode("utf-8"))
        out["magdist_sample"] = {
            "totals": d.get("totals"),
            "n_stacks_published": len(d.get("stacks", [])),
            "top_stack": d.get("stacks", [{}])[0] if d.get("stacks") else None,
        }
    except Exception as e:
        out["magdist_sample_err"] = str(e)[:200]
    
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
