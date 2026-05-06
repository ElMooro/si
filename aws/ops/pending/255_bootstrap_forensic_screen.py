#!/usr/bin/env python3
"""Step 255 — Bootstrap justhodl-forensic-screen end-to-end.

  1. Create Lambda (or update if it exists)
  2. Create EventBridge rule (rate(12 hours)) + Lambda permission
  3. Initial synchronous invoke (5-10 min — full SP500 scoring)
  4. Verify by reading data/forensic-screen.json
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-forensic-screen"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
RULE_NAME = "justhodl-forensic-screen-12h"
RULE_SCHEDULE = "rate(12 hours)"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
OUTPUT_KEY = "data/forensic-screen.json"
REPORT_PATH = "aws/ops/reports/255_forensic_bootstrap.json"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(SOURCE_DIR):
            for fn in files:
                fpath = os.path.join(root, fn)
                arcname = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname)
    return buf.getvalue()


def ensure_lambda():
    zip_bytes = build_zip()
    env = {"S3_BUCKET": BUCKET, "FMP_KEY": FMP_KEY, "N_TICKERS": "200"}
    try:
        cur = lam.get_function(FunctionName=LAMBDA_NAME)
        print(f"[255] Lambda exists, updating ({len(zip_bytes):,}b)")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512,
            Timeout=600,
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return {"created": False, "arn": cur["Configuration"]["FunctionArn"]}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[255] creating Lambda {LAMBDA_NAME}…")
    resp = lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        Role=ROLE_ARN,
        Code={"ZipFile": zip_bytes},
        Description=("Forensic accounting screen — Beneish M-Score + Sloan accruals + "
                     "WC divergence + goodwill bloat for top 200 SP500. 12h schedule."),
        MemorySize=512,
        Timeout=600,
        Environment={"Variables": env},
        Tags={"project": "justhodl", "purpose": "forensic-screen"},
    )
    lam.get_waiter("function_active_v2").wait(FunctionName=LAMBDA_NAME)
    return {"created": True, "arn": resp["FunctionArn"]}


def ensure_eventbridge_rule(lambda_arn):
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=RULE_SCHEDULE, State="ENABLED",
        Description="Trigger justhodl-forensic-screen every 12 hours",
    )
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": lambda_arn}])
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="EventBridgeInvoke12H",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def initial_invoke():
    print(f"[255] invoking {LAMBDA_NAME} synchronously (this may take 5-10min for full SP500)…")
    started = time.time()
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = time.time() - started
    func_err = resp.get("FunctionError")
    payload_raw = resp["Payload"].read()
    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = {"raw": payload_raw[:500].decode(errors="replace")}
    return {"function_error": func_err, "payload": payload,
            "status_code": resp.get("StatusCode"), "elapsed_s": round(elapsed, 1)}


def verify():
    out = {}
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
        out["generated_at"] = body.get("generated_at")
        out["n_scored_ok"] = body.get("n_scored_ok")
        out["n_skipped"] = body.get("n_skipped_missing_data")
        out["summary"] = body.get("summary")
        out["thresholds"] = body.get("thresholds")
        out["top_3_concerning"] = [
            {
                "symbol": r["symbol"],
                "concern_score": r.get("concern_score"),
                "m_score": r.get("m_score"),
                "sloan_accruals": r.get("sloan_accruals"),
            }
            for r in (body.get("most_concerning_top_25") or [])[:3]
        ]
        out["top_3_cleanest"] = [
            {"symbol": r["symbol"], "m_score": r.get("m_score")}
            for r in (body.get("cleanest_top_25") or [])[:3]
        ]
    except Exception as e:
        out["err"] = str(e)
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        lam_info = ensure_lambda()
        out["lambda"] = lam_info
        ensure_eventbridge_rule(lam_info["arn"])
        out["rule"] = {"name": RULE_NAME, "schedule": RULE_SCHEDULE, "state": "ENABLED"}
        out["initial_invoke"] = initial_invoke()
        time.sleep(3)
        out["verify"] = verify()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
