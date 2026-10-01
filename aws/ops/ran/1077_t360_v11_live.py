"""
Ops 1077: confirm ticker-360 v1.1 is live, re-invoke, verify index.

1076 found the artifact still at v1.0. Check the lambda's LastModified,
re-invoke async, and poll for the v1.1 artifact.

Writes: aws/ops/reports/1077_t360_v11_live.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1077_t360_v11_live.json")
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
    """Read the artifact summary. Never raises."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        data = json.loads(obj["Body"].read().decode())
        tk = data.get("tickers") or {}
        cov = sorted(((v.get("coverage_count", 0), t)
                      for t, v in tk.items()), reverse=True)[:10]
        return {"exists": True, "version": data.get("version"),
                "generated_at": data.get("generated_at"),
                "universe_size": data.get("universe_size"),
                "indexed_tickers": data.get("indexed_tickers"),
                "top_coverage": [{"ticker": t, "coverage": c} for c, t in cov]}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": True, "parse_error": str(e)[:100]}


def main():
    """Check lambda version, invoke, poll, write report."""
    rep = {"script": "1077_t360_v11_live",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        cfg = lam.get_function(FunctionName=FN)["Configuration"]
        rep["lambda"] = {"last_modified": cfg.get("LastModified"),
                         "code_sha": (cfg.get("CodeSha256") or "")[:12]}
    except Exception as e:  # noqa: BLE001
        rep["lambda"] = {"error": str(e)[:100]}
    before = artifact()
    rep["before"] = {k: before.get(k) for k in
                     ("version", "generated_at", "indexed_tickers")}
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({}).encode())
        rep["async_invoke"] = {"status_code": r.get("StatusCode")}
    except Exception as e:  # noqa: BLE001
        rep["async_invoke"] = {"error": str(e)[:150]}
    deadline = time.time() + 540
    after = before
    while time.time() < deadline:
        time.sleep(30)
        after = artifact()
        if (after.get("version") == "1.1"
                and after.get("generated_at") != before.get("generated_at")):
            break
    rep["after"] = after
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2500])


if __name__ == "__main__":
    main()
