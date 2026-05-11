#!/usr/bin/env python3
"""Step 436 — Read Function URL via aws lambda (using the more permissive
GH-Actions IAM doesn't apply here, but we can verify the URL via direct
HTTP if we can find it in ma/app.js after the workflow auto-patched)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/436_ma_url_check.json"
NAME = "justhodl-tmp-436"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Confirm Lambda exists
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ma-tracker")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]

    # Check S3 cache state
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/ma-latest.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["s3_cache"] = {
            "size_kb": round(len(body)/1024, 1),
            "last_modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
            "generated_at": d.get("generated_at"),
            "n_deals": len(d.get("deals", [])),
            "n_profiles": len(d.get("profiles", {})),
            "elapsed_seconds": d.get("elapsed_seconds"),
            "summary": d.get("summary", {}),
        }
    except s3.exceptions.NoSuchKey:
        out["s3_cache"] = "not_yet_created"
    except Exception as e:
        out["s3_cache_err"] = str(e)[:200]

    # Direct invoke (RequestResponse) to populate cache + verify it returns data
    try:
        resp = lam.invoke(
            FunctionName="justhodl-ma-tracker",
            InvocationType="RequestResponse",
            Payload=json.dumps({"queryStringParameters": {}}).encode())
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
            out["invoke"] = {
                "statusCode": parsed.get("statusCode"),
                "n_deals": len((inner or {}).get("deals", [])),
                "n_profiles": len((inner or {}).get("profiles", {})),
                "elapsed_seconds": (inner or {}).get("elapsed_seconds"),
                "generated_at": (inner or {}).get("generated_at"),
            }
            sm = (inner or {}).get("summary", {})
            if sm:
                out["invoke"]["top_sectors"] = sm.get("by_sector", [])[:6]
                out["invoke"]["top_acquirers"] = sm.get("top_acquirers", [])[:6]
        except Exception as e:
            out["invoke"] = {"parse_err": str(e)[:100], "raw": body[:500]}
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
