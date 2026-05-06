#!/usr/bin/env python3
"""Step 274 — Redeploy macro-nowcast v2.1 with historical replay.

v2.1 adds compute_historical_scores() which replays the nowcast
month-by-month over 10 years using only data available at each
point. Output JSON gains historical_scores[] and historical_summary{}.

Also bump timeout to 120s since the replay walks ~120 months × 7
series × ~300 obs each (typically completes in 10-30s).
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
REPORT_PATH = "aws/ops/reports/274_macro_nowcast_v21.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
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
        out["zip_size_bytes"] = len(zip_bytes)

        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        out["code_updated"] = True

        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256,
            Timeout=120,
            Environment={"Variables": {"S3_BUCKET": BUCKET, "FRED_KEY": FRED_KEY}},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        out["env_updated"] = True

        print("[274] sync invoking…")
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
                hist = body.get("historical_scores") or []
                hist_summary = body.get("historical_summary") or {}
                out["output_summary"] = {
                    "version": body.get("v"),
                    "regime": body.get("regime"),
                    "score": body.get("normalized_score"),
                    "coverage_pct": body.get("coverage_pct"),
                    "n_historical_months": len(hist),
                    "first_historical": hist[0] if hist else None,
                    "last_historical": hist[-1] if hist else None,
                    "min_historical": (min(hist, key=lambda h: h["score"]) if hist else None),
                    "max_historical": (max(hist, key=lambda h: h["score"]) if hist else None),
                    "historical_summary": hist_summary,
                    "regime_distribution": hist_summary.get("regime_distribution"),
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
