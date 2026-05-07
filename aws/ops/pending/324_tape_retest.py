#!/usr/bin/env python3
"""Step 324 — Re-test tape-reader after universe loading fix."""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-tape-reader"
REPORT = "aws/ops/reports/324_tape_retest.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
    out["lambda_last_modified"] = cfg.get("LastModified")

    print(f"[324] Lambda last modified: {cfg.get('LastModified')}")
    print(f"[324] Sync invoke (3-5min expected for ~1500 universe x 21 baseline calls)…")
    started = time.time()
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
    out["status"] = resp.get("StatusCode")
    out["function_error"] = resp.get("FunctionError")
    out["duration_s"] = round(time.time() - started, 1)
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["response"] = json.loads(body)
    except Exception:
        out["response_raw"] = body[:500]

    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/tape-reader.json")
    data = json.loads(obj["Body"].read())
    out["s3_size_kb"] = round(obj["ContentLength"]/1024, 1)
    out["n_universe"] = data.get("n_universe")
    out["n_with_data"] = data.get("n_with_data")
    out["market_breadth"] = data.get("market_breadth")
    out["top_10"] = [
        {
            "ticker": r["ticker"], "score": r["score"],
            "rel_vol": r.get("rel_volume"), "block_ratio": r.get("block_ratio"),
            "range_exp": r.get("range_expansion"),
            "tags": r.get("classifications"),
            "rationale": (r.get("rationale") or "")[:120],
        } for r in (data.get("top_loud_tape") or [])[:10]
    ]
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
