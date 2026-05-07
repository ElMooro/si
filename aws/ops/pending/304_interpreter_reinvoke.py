#!/usr/bin/env python3
"""Step 304 — Re-invoke divergence-interpreter after defensive fix.

Step 303 errored because nowcast components is a LIST not a dict.
Lambda code now handles both. Sync invoke + capture full output.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-divergence-interpreter"
BUCKET = "justhodl-dashboard-live"
KEY = "data/divergence-interpreted.json"
REPORT = "aws/ops/reports/304_interpreter_reinvoke.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Wait for deploy
        time.sleep(45)

        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
            LogType="Tail",
        )
        out["status_code"] = resp.get("StatusCode")
        out["function_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            out["response"] = json.loads(body)
        except Exception:
            out["response_raw"] = body[:500]

        # Pull S3 output
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=KEY)
            data = json.loads(obj["Body"].read())
            out["interpretation"] = {
                "regime": data.get("regime"),
                "composite_index": data.get("divergence_composite_index"),
                "n_extreme": data.get("n_extreme"),
                "n_flagged": data.get("n_flagged"),
                "alert_reasons": data.get("alert_reasons"),
                "claude_meta": data.get("claude_meta"),
                "interpretation_full": data.get("interpretation"),
            }
        except Exception as e:
            out["s3_err"] = str(e)[:200]

        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()
