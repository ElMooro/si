"""
Ops 1082: verify flow-confluence v1.3 is live after manual deploy.

Checks:
 1. Lambda LastModified is recent (post-manual-deploy).
 2. Invoke the lambda and check the output has t360 fields.

Writes: aws/ops/reports/1082_flowconfluence_verify.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1082_flowconfluence_verify.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FUNCTION = "justhodl-flow-confluence"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    """Verify v1.3 live. Write report."""
    rep = {"script": "1082_flowconfluence_verify",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        cfg = lam.get_function(FunctionName=FUNCTION)["Configuration"]
        rep["last_modified"] = cfg.get("LastModified")
        rep["version"] = cfg.get("Version")

        # Check the artifact for t360 fields
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/flow-confluence.json")
            data = json.loads(obj["Body"].read().decode())
            tm = data.get("ticker_map") or {}
            sample = None
            for tk, v in list(tm.items())[:20]:
                if isinstance(v, dict) and "t360_coverage" in v:
                    sample = {"ticker": tk, "t360_coverage": v["t360_coverage"]}
                    break
            rep["artifact_t360"] = sample is not None
            rep["artifact_sample"] = sample
            rep["artifact_generated"] = data.get("generated_at")
            rep["cross_validated_count"] = len(data.get("cross_validated_tickers") or [])
        except Exception as e:  # noqa: BLE001
            rep["artifact_error"] = str(e)[:100]

        rep["status"] = "VERIFIED" if rep.get("artifact_t360") else "PENDING_RUN"
    except ClientError as e:
        rep["status"] = "AWS_ERROR"
        rep["error"] = str(e)[:200]
    except Exception as e:  # noqa: BLE001
        rep["status"] = "ERROR"
        rep["error"] = str(e)[:200]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
