#!/usr/bin/env python3
"""Step 416 — Stage 7 deploy:
  1) Set S3 lifecycle policy to auto-expire screener snapshots > 30 days
  2) Fire async screener refresh so first snapshot writes today
  3) Return immediately (Lambda runs ~6 min on its own).
After ~7 min, step 417 will verify snapshot + (eventually) just-crossed
once we have 2 days of data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/416_jc_deploy.json"
NAME = "justhodl-tmp-jc-deploy"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def lambda_handler(event, context):
    out = {}

    # 1. Set lifecycle config — expire screener/snapshots/* after 30 days
    try:
        existing = {}
        try:
            existing = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET) or {}
        except s3.exceptions.from_code("NoSuchLifecycleConfiguration"):
            pass
        except Exception:
            pass

        rules = (existing.get("Rules") or [])
        # Filter out any old rule with the same ID so we get a clean upsert
        rules = [r for r in rules if r.get("ID") != "expire-screener-snapshots-30d"]

        rules.append({
            "ID": "expire-screener-snapshots-30d",
            "Filter": {"Prefix": "screener/snapshots/"},
            "Status": "Enabled",
            "Expiration": {"Days": 30},
        })
        s3.put_bucket_lifecycle_configuration(
            Bucket=BUCKET,
            LifecycleConfiguration={"Rules": rules}
        )
        out["lifecycle"] = {"ok": True, "rules_count": len(rules)}
    except Exception as e:
        out["lifecycle"] = {"error": str(e)[:300]}

    # 2. Lambda code metadata check
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
        out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # 3. Fire async force refresh
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="Event",
            Payload=json.dumps({"force": True}).encode())
        out["invoke"] = {"status": resp.get("StatusCode")}
    except Exception as e:
        out["invoke"] = {"error": str(e)[:200]}

    # 4. Pre-existing snapshots check (if any)
    try:
        listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="screener/snapshots/")
        objs = listing.get("Contents") or []
        out["existing_snapshots"] = [{"key": o["Key"], "size": o["Size"],
                                        "modified": str(o["LastModified"])}
                                       for o in objs]
    except Exception as e:
        out["list_err"] = str(e)[:200]

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
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
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
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
