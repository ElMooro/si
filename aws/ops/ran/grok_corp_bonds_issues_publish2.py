#!/usr/bin/env python3
"""Publish the company book now that the 600s function is deployed.

The earlier script waited for a code change and then the task died at 300s
before writing the packet. This one invokes the function that is already live.
"""
import json

import boto3
from botocore.config import Config

FN = "justhodl-corp-bonds"
BUCKET = "justhodl-dashboard-live"
KEY = "data/corp-bonds.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=640, connect_timeout=10))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    cfg = lam.get_function(FunctionName=FN)["Configuration"]
    print("timeout", cfg.get("Timeout"), "update", cfg.get("LastUpdateStatus"))
    if cfg.get("Timeout") != 600 or cfg.get("LastUpdateStatus") != "Successful":
        raise SystemExit("justhodl-corp-bonds is not the 600s build")
    resp = lam.invoke(
        FunctionName=FN,
        InvocationType="RequestResponse",
        Payload=json.dumps({"action": "publish"}).encode(),
    )
    payload = json.loads(resp["Payload"].read() or b"{}")
    print("invoke", resp.get("StatusCode"), resp.get("FunctionError"), str(payload)[:600])
    if resp.get("FunctionError"):
        raise SystemExit("publish failed")
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    issues = body.get("issues") or {}
    bonds = issues.get("bonds") or []
    sample = bonds[0] if bonds else {}
    if not isinstance(sample, dict):
        sample = {}
    summary = {
        "version": body.get("version"),
        "generated_at": body.get("generated_at"),
        "n_bonds": issues.get("n_bonds"),
        "n_issuers": issues.get("n_issuers"),
        "n_sectors": issues.get("n_sectors"),
        "n_with_rate": issues.get("n_with_rate"),
        "n_with_change": issues.get("n_with_change"),
        "demand": issues.get("demand"),
        "as_of": issues.get("as_of"),
        "status": issues.get("status"),
        "sample_keys": sorted(sample.keys()),
        "funds": [
            {k: row.get(k) for k in ("id", "status", "n", "as_of")}
            for row in (issues.get("funds") or [])
        ],
    }
    print(json.dumps(summary, default=str))
    if any(k in sample for k in ("cusip", "isin", "ISIN", "CUSIP")):
        raise SystemExit("identifier leaked into a public bond row")
    if body.get("version") != "1.1.0" or (issues.get("n_bonds") or 0) < 1000:
        raise SystemExit("company book missing after publish")


if __name__ == "__main__":
    main()
