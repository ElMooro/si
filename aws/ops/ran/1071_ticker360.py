"""
Ops 1071: verify ticker-360 cross-enrichment (read-only + one invoke).

Checks:
 1. justhodl-ticker-360 lambda exists (deploy succeeded).
 2. ticker_360 hub imports and enrich() works on a sample ticker.
 3. data/ticker-360.json artifact status.

Writes: aws/ops/reports/1071_ticker360.json (committed back by run-ops).
One controlled invoke of the new producer is allowed (it only writes its
own output key). Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1071_ticker360.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FN = "justhodl-ticker-360"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    """Verify lambda, hub, and artifact. Write the report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rep = {"script": "1071_ticker360", "read_at": now}
    try:
        cfg = lam.get_function(FunctionName=FN)["Configuration"]
        rep["lambda"] = {"exists": True,
                         "last_modified": cfg.get("LastModified"),
                         "timeout": cfg.get("Timeout"),
                         "memory": cfg.get("MemorySize")}
    except ClientError:
        rep["lambda"] = {"exists": False}
    # hub import + enrich smoke test
    try:
        import ticker_360
        rep["hub"] = {"import_ok": True,
                      "domains": len(ticker_360.SOURCES)}
        view = ticker_360.enrich("NVDA", s3)
        conf = view.get("confluence", {})
        rep["smoke_nvda"] = {
            "domains_available": conf.get("domains_available"),
            "coverage_count": conf.get("coverage_count"),
            "covering": conf.get("domains_covering", [])[:8]}
    except Exception as e:  # noqa: BLE001
        rep["hub"] = {"import_ok": False, "error": str(e)[:120]}
    # artifact status
    try:
        h = s3.head_object(Bucket=BUCKET, Key="data/ticker-360.json")
        rep["artifact"] = {"exists": True,
                           "size": h.get("ContentLength"),
                           "last_modified": h.get("LastModified").isoformat()
                           if h.get("LastModified") else None}
    except ClientError:
        rep["artifact"] = {"exists": False}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2500])


if __name__ == "__main__":
    main()
