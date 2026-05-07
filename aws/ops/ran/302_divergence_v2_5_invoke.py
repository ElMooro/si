#!/usr/bin/env python3
"""Step 302 — Invoke divergence-engine-v2 (now v2.5) and inspect results.

After expansion to ~62 pairs across 17 categories. Verify:
  1. Lambda runs without errors
  2. n_relationships now ~62 (was 32)
  3. Coverage (n_with_data) is decent
  4. New categories (crisis_leading, europe_labor, etc.) populate
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-divergence-engine-v2"
BUCKET = "justhodl-dashboard-live"
KEY = "data/divergence-v2.json"
REPORT = "aws/ops/reports/302_divergence_v2_5_invoke.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Sync invoke
        print(f"[302] invoking {LAMBDA_NAME} (sync, ~3-15s expected)…")
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

        # Pull the full output from S3
        time.sleep(2)
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        data = json.loads(obj["Body"].read())

        out["s3_size_kb"] = round(obj["ContentLength"] / 1024, 1)
        out["s3_last_modified"] = obj["LastModified"].isoformat()

        # Distill
        out["n_relationships"] = data.get("n_relationships")
        out["n_with_data"] = data.get("n_with_data")
        out["composite_index"] = data.get("composite_divergence_index")
        out["by_status"] = data.get("by_status")
        out["fetch_errors"] = data.get("fetch_errors")
        out["categories"] = {
            cat: {
                "count": len(items),
                "extreme": sum(1 for r in items if r.get("status") == "extreme"),
                "flagged": sum(1 for r in items if r.get("status") == "flagged"),
                "no_data": sum(1 for r in items if r.get("status") == "no_data"),
            }
            for cat, items in (data.get("by_category") or {}).items()
        }
        # Top 10 most extreme by abs divergence
        all_rels = data.get("all_relationships", [])
        with_data = [r for r in all_rels if r.get("status") in ("normal", "flagged", "extreme")]
        with_data.sort(key=lambda x: abs(x.get("divergence_z") or 0), reverse=True)
        out["top_10_dislocations"] = [
            {
                "name": r["name"],
                "category": r.get("category"),
                "divergence_z": r.get("divergence_z"),
                "z_a": r.get("z_a"),
                "z_b": r.get("z_b"),
                "status": r.get("status"),
            }
            for r in with_data[:10]
        ]
        # Pairs that returned no_data (worth knowing for follow-up fixes)
        no_data = [r for r in all_rels if r.get("status") == "no_data"]
        out["no_data_pairs"] = [
            {"name": r["name"], "series_a": r.get("series_a"), "series_b": r.get("series_b")}
            for r in no_data
        ]

        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
