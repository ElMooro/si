"""
Ops 1079: verify ticker-360 wiring in flow-confluence and best-ideas.

Checks (read-only):
 1. Both lambdas exist and show recent LastModified (v1.3/v1.2 deployed).
 2. data/flow-confluence.json and data/best-ideas.json have t360 fields
    (proves the enrichment code ran on their last scheduled run).

Writes: aws/ops/reports/1079_t360_wiring.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1079_t360_wiring.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def check_lambda(fn):
    """Check lambda exists. Never raises."""
    try:
        cfg = lam.get_function(FunctionName=fn)["Configuration"]
        return {"exists": True, "last_modified": cfg.get("LastModified")}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": "unknown", "error": str(e)[:80]}


def check_artifact(key, t360_field, sample_key):
    """Check artifact has t360 enrichment. Never raises."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode())
        sample = data.get(sample_key) or {}
        # find first entry with the t360 field
        found = None
        if isinstance(sample, dict):
            for tk, v in list(sample.items())[:50]:
                if isinstance(v, dict) and t360_field in v:
                    found = {"ticker": tk, t360_field: v[t360_field]}
                    break
        elif isinstance(sample, list):
            for v in sample[:50]:
                if isinstance(v, dict) and t360_field in v:
                    found = {"ticker": v.get("ticker") or v.get("symbol"),
                             t360_field: v[t360_field]}
                    break
        return {"exists": True,
                "generated_at": data.get("generated_at"),
                "t360_present": found is not None,
                "sample": found,
                "cross_validated": data.get("cross_validated_tickers", [])[:5]
                if "cross_validated_tickers" in data else None}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": True, "error": str(e)[:100]}


def main():
    """Verify wiring, write report."""
    rep = {"script": "1079_t360_wiring",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    rep["flow_confluence_lambda"] = check_lambda("justhodl-flow-confluence")
    rep["best_ideas_lambda"] = check_lambda("justhodl-best-ideas")
    rep["flow_confluence_artifact"] = check_artifact(
        "data/flow-confluence.json", "t360_coverage", "ticker_map")
    rep["best_ideas_artifact"] = check_artifact(
        "data/best-ideas.json", "t360_coverage", "stack")
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2500])


if __name__ == "__main__":
    main()
