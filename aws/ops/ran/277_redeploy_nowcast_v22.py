#!/usr/bin/env python3
"""Step 277 — Redeploy macro-nowcast v2.2 (regime-SPY forward returns)."""
import io
import json
import os
import time
import zipfile
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-macro-nowcast"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/277_nowcast_v22.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
PAGE_URL = "https://justhodl.ai/macro-data.html"
DATA_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/data/macro-nowcast.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Build + deploy
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _d, files in os.walk(SOURCE_DIR):
                for fn in files:
                    if fn.endswith(".pyc") or "__pycache__" in root:
                        continue
                    fp = os.path.join(root, fn)
                    zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
        zip_bytes = buf.getvalue()
        out["zip_bytes"] = len(zip_bytes)
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        out["deployed"] = True

        # Sync invoke
        invoke_started = time.time()
        inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                         Payload=b"{}")
        elapsed = round(time.time() - invoke_started, 2)
        payload = json.loads(inv["Payload"].read())
        out["invoke"] = {
            "status": inv.get("StatusCode"),
            "func_err": inv.get("FunctionError"),
            "elapsed_s": elapsed,
            "body": payload.get("body"),
        }

        if not inv.get("FunctionError"):
            time.sleep(2)
            body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
            perf = body.get("regime_spy_performance") or {}
            out["data_summary"] = {
                "version": body.get("v"),
                "regime": body.get("regime"),
                "score": body.get("normalized_score"),
                "n_historical": len(body.get("historical_scores") or []),
                "regime_spy_performance": perf,
            }

        # Verify the page picked up the new render function
        try:
            page_html = fetch(PAGE_URL + "?_=" + str(int(time.time())))
            out["page_check"] = {
                "size": len(page_html),
                "has_renderRegimeSpyTable": "renderRegimeSpyTable" in page_html,
                "has_nowcastRegimeSpyTable_div": 'id="nowcastRegimeSpyTable"' in page_html,
            }
        except Exception as e:
            out["page_check_err"] = str(e)[:200]

        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
