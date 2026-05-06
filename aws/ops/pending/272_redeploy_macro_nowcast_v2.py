#!/usr/bin/env python3
"""Step 272 — Redeploy macro-nowcast v2 (FRED-direct fetch).

The Lambda exists from step 270's deploy. v2 replaces the broken
report.json schema reading with direct FRED API calls. This script:

  1. Builds zip from refreshed source/
  2. update_function_code → wait for Active
  3. update_function_configuration → add FRED_KEY env var
  4. Sync invoke → verify components actually compute now
  5. Persist a clean summary report
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-macro-nowcast"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/272_macro_nowcast_v2.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Build zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _d, files in os.walk(SOURCE_DIR):
                for fn in files:
                    if fn.endswith(".pyc") or "__pycache__" in root:
                        continue
                    fp = os.path.join(root, fn)
                    zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
        zip_bytes = buf.getvalue()
        out["zip_size_bytes"] = len(zip_bytes)
        print(f"[272] zip {len(zip_bytes):,}b")

        # Update code
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        out["code_updated"] = True

        # Update env to add FRED_KEY
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=60,
            Environment={"Variables": {"S3_BUCKET": BUCKET, "FRED_KEY": FRED_KEY}},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        out["env_updated"] = True

        # Sync invoke
        print("[272] sync invoking…")
        invoke_started = time.time()
        inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                         Payload=b"{}")
        payload = json.loads(inv["Payload"].read())
        out["invoke"] = {
            "status": inv.get("StatusCode"),
            "func_err": inv.get("FunctionError"),
            "elapsed_s": round(time.time() - invoke_started, 2),
            "payload": payload,
        }

        if not inv.get("FunctionError"):
            time.sleep(2)
            try:
                body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
                out["output"] = {
                    "regime": body.get("regime"),
                    "regime_color": body.get("regime_color"),
                    "raw_score": body.get("raw_score"),
                    "normalized_score": body.get("normalized_score"),
                    "coverage_pct": body.get("coverage_pct"),
                    "n_components_used": body.get("n_components_used"),
                    "n_components_failed": body.get("n_components_failed"),
                    "components": [
                        {"fred_id": c["fred_id"], "label": c["label"],
                         "z": c.get("z"), "contribution": c.get("contribution"),
                         "raw_value": c.get("raw_value"), "n_obs": c.get("n_obs"),
                         "latest_date": c.get("latest_date"), "error": c.get("error")}
                        for c in (body.get("components") or [])
                    ],
                }
            except Exception as e:
                out["output_read_err"] = str(e)[:200]

        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4500])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
