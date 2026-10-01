"""
Ops 1075: check ticker-360 artifact + async re-invoke if missing.

1073's sync invoke timed out client-side; the lambda may have finished.
If data/ticker-360.json is missing, re-invoke ASYNC (Event) so the 600s
lambda timeout governs, then report.

Writes: aws/ops/reports/1075_t360_artifact.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1075_t360_artifact.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FN = "justhodl-ticker-360"
KEY = "data/ticker-360.json"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def artifact():
    """Head + parse the artifact. Never raises."""
    try:
        h = s3.head_object(Bucket=BUCKET, Key=KEY)
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        data = json.loads(obj["Body"].read().decode())
        return {"exists": True, "size": h.get("ContentLength"),
                "generated_at": data.get("generated_at"),
                "universe_size": data.get("universe_size"),
                "indexed_tickers": data.get("indexed_tickers"),
                "sample": sorted((data.get("tickers") or {}).keys())[:10]}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": True, "parse_error": str(e)[:100]}


def main():
    """Check artifact, async invoke if missing, write report."""
    rep = {"script": "1075_t360_artifact",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    rep["before"] = artifact()
    if not rep["before"].get("exists"):
        try:
            r = lam.invoke(FunctionName=FN, InvocationType="Event",
                           Payload=json.dumps({}).encode())
            rep["async_invoke"] = {"status_code": r.get("StatusCode"),
                                   "queued": r.get("StatusCode") == 202}
        except Exception as e:  # noqa: BLE001
            rep["async_invoke"] = {"error": str(e)[:150]}
    else:
        rep["async_invoke"] = {"skipped": "artifact already exists"}
    rep["after"] = rep["before"]
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2000])


if __name__ == "__main__":
    main()
