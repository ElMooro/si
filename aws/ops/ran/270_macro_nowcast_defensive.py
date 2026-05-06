#!/usr/bin/env python3
"""Step 270 — Defensive macro-nowcast bootstrap.

Steps 266-269 all reported "Lambda not deployed" or produced no
report. This script:

  - Performs each AWS operation inside an isolated try/except
  - Writes the report INCREMENTALLY after every step so partial
    progress is captured even on early crash
  - Surfaces the exact ClientError code from any failure (likely
    AccessDeniedException on lambda:CreateFunction)

If this step also fails, the report will tell us exactly which
operation hit which error code.
"""
import io
import json
import os
import time
import traceback
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-macro-nowcast"
RULE_NAME = "justhodl-macro-nowcast-6h"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/270_macro_nowcast_defensive.json"


def write_report(out):
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)


def safe(fn, *args, **kwargs):
    """Wrap a callable and return (result, err_dict)."""
    try:
        return fn(*args, **kwargs), None
    except ClientError as e:
        return None, {
            "type": "ClientError",
            "code": e.response.get("Error", {}).get("Code"),
            "message": e.response.get("Error", {}).get("Message"),
            "operation": e.operation_name if hasattr(e, "operation_name") else None,
        }
    except Exception as e:
        return None, {
            "type": type(e).__name__,
            "message": str(e)[:500],
            "traceback": traceback.format_exc()[-1000:],
        }


def main():
    out = {
        "started": datetime.now(timezone.utc).isoformat(),
        "step_log": [],
    }
    write_report(out)

    lam = boto3.client("lambda", region_name=REGION)
    eb = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # STEP 1 — Verify caller identity (so we know what role we're running as)
    sts = boto3.client("sts", region_name=REGION)
    ident, err = safe(sts.get_caller_identity)
    out["caller_identity"] = ident if ident else err
    out["step_log"].append({"step": "sts.get_caller_identity", "ok": err is None})
    write_report(out)

    # STEP 2 — Check if Lambda already exists
    cur, err = safe(lam.get_function, FunctionName=LAMBDA_NAME)
    if cur:
        out["lambda_pre_state"] = {
            "exists": True,
            "arn": cur["Configuration"]["FunctionArn"],
            "state": cur["Configuration"].get("State"),
            "last_modified": cur["Configuration"].get("LastModified"),
        }
        out["step_log"].append({"step": "lam.get_function (pre)", "ok": True, "exists": True})
    else:
        out["lambda_pre_state"] = {"exists": False, "err": err}
        out["step_log"].append({"step": "lam.get_function (pre)", "ok": False, "err": err})
    write_report(out)

    # STEP 3 — Build zip
    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _d, files in os.walk(SOURCE_DIR):
                for fn in files:
                    if fn.endswith(".pyc") or "__pycache__" in root:
                        continue
                    fp = os.path.join(root, fn)
                    zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
        zip_bytes = buf.getvalue()
        out["zip"] = {"bytes": len(zip_bytes), "ok": True}
        out["step_log"].append({"step": "build_zip", "ok": True, "size": len(zip_bytes)})
    except Exception as e:
        out["zip"] = {"err": str(e), "traceback": traceback.format_exc()[-1000:]}
        out["step_log"].append({"step": "build_zip", "ok": False, "err": str(e)})
        write_report(out)
        return 1
    write_report(out)

    # STEP 4 — Create or update Lambda
    if out["lambda_pre_state"]["exists"]:
        # Update path
        _, err = safe(lam.update_function_code,
                      FunctionName=LAMBDA_NAME, ZipFile=zip_bytes, Publish=False)
        out["update_code"] = {"ok": err is None, "err": err}
        out["step_log"].append({"step": "lam.update_function_code", "ok": err is None, "err": err})
        write_report(out)
        if err is not None:
            return 1

        # Wait for update
        time.sleep(3)
        for _ in range(15):
            cur, _ = safe(lam.get_function, FunctionName=LAMBDA_NAME)
            if cur and cur["Configuration"].get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
    else:
        # Create path
        env = {"S3_BUCKET": BUCKET}
        result, err = safe(
            lam.create_function,
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Code={"ZipFile": zip_bytes},
            Description="Composite real-time macro nowcast (7 FRED series, 6h cadence)",
            MemorySize=256,
            Timeout=60,
            Environment={"Variables": env},
            Tags={"project": "justhodl", "purpose": "macro-nowcast"},
        )
        out["create_function"] = {"ok": err is None, "err": err,
                                  "arn": (result or {}).get("FunctionArn")}
        out["step_log"].append({"step": "lam.create_function", "ok": err is None, "err": err})
        write_report(out)
        if err is not None:
            return 1

        # Wait for Active
        time.sleep(5)
        for _ in range(30):
            cur, _ = safe(lam.get_function, FunctionName=LAMBDA_NAME)
            if cur and cur["Configuration"].get("State") == "Active":
                out["step_log"].append({"step": "wait_for_active", "ok": True})
                break
            time.sleep(2)
    write_report(out)

    # STEP 5 — EB rule
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}"
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}"

    _, err = safe(eb.put_rule,
                  Name=RULE_NAME, ScheduleExpression="rate(6 hours)", State="ENABLED",
                  Description="Compute composite macro nowcast every 6h")
    out["put_rule"] = {"ok": err is None, "err": err}
    out["step_log"].append({"step": "eb.put_rule", "ok": err is None, "err": err})
    write_report(out)

    # Permission (idempotent)
    _, err = safe(lam.add_permission,
                  FunctionName=LAMBDA_NAME,
                  StatementId=f"{RULE_NAME}-invoke",
                  Action="lambda:InvokeFunction",
                  Principal="events.amazonaws.com",
                  SourceArn=rule_arn)
    if err and err.get("code") == "ResourceConflictException":
        out["add_permission"] = {"ok": True, "status": "already_exists"}
    else:
        out["add_permission"] = {"ok": err is None, "err": err}
    out["step_log"].append({"step": "lam.add_permission", "ok": err is None or
                            err.get("code") == "ResourceConflictException"})
    write_report(out)

    _, err = safe(eb.put_targets, Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": lambda_arn}])
    out["put_targets"] = {"ok": err is None, "err": err}
    out["step_log"].append({"step": "eb.put_targets", "ok": err is None, "err": err})
    write_report(out)

    # STEP 6 — Sync invoke
    inv, err = safe(lam.invoke, FunctionName=LAMBDA_NAME,
                    InvocationType="RequestResponse", Payload=b"{}")
    if inv:
        try:
            payload = json.loads(inv["Payload"].read())
        except Exception:
            payload = {"raw": "(non-json)"}
        out["sync_invoke"] = {
            "status_code": inv.get("StatusCode"),
            "function_error": inv.get("FunctionError"),
            "payload_preview": json.dumps(payload, default=str)[:1000],
        }
        out["step_log"].append({"step": "lam.invoke", "ok": True,
                                "status": inv.get("StatusCode"),
                                "func_err": inv.get("FunctionError")})
    else:
        out["sync_invoke"] = {"ok": False, "err": err}
        out["step_log"].append({"step": "lam.invoke", "ok": False, "err": err})
    write_report(out)

    # STEP 7 — Verify S3 output
    time.sleep(3)
    body, err = safe(s3.get_object, Bucket=BUCKET, Key=OUTPUT_KEY)
    if body:
        try:
            content = json.loads(body["Body"].read())
            out["s3_output"] = {
                "exists": True,
                "generated_at": content.get("generated_at"),
                "regime": content.get("regime"),
                "composite_z": content.get("composite_z"),
                "raw_score": content.get("raw_score"),
                "n_components": len(content.get("components", [])),
                "sample_keys": list(content.keys())[:15],
            }
        except Exception as parse_err:
            out["s3_output"] = {"err": f"parse failed: {parse_err}"}
    else:
        out["s3_output"] = {"exists": False, "err": err}
    out["step_log"].append({"step": "s3.get_object (output)", "ok": body is not None})
    write_report(out)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    write_report(out)

    print(json.dumps(out, indent=2, default=str)[:4000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
