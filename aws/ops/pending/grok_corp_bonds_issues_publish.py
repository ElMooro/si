#!/usr/bin/env python3
"""Wait until justhodl-corp-bonds has the new code, then publish the company book.

Apply-lane dispatches this before the Lambda deploy has finished. Invoking
early would let the old function overwrite data/corp-bonds.json, so this
waits for CodeSha256 to change and only then calls publish.
"""
import json
import time

import boto3
from botocore.config import Config

FN = "justhodl-corp-bonds"
BUCKET = "justhodl-dashboard-live"
KEY = "data/corp-bonds.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=330, connect_timeout=10))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    base = lam.get_function(FunctionName=FN)["Configuration"]["CodeSha256"]
    deadline = time.time() + 12 * 60
    while time.time() < deadline:
        cfg = lam.get_function(FunctionName=FN)["Configuration"]
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("CodeSha256") != base:
            break
        time.sleep(15)
    else:
        raise SystemExit("justhodl-corp-bonds code did not update within 12 minutes")
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"action": "publish"}).encode())
    payload = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError"):
        raise SystemExit("publish failed: %s" % payload)
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    issues = body.get("issues") or {}
    print(json.dumps({
        "ok": bool(payload.get("ok")),
        "version": body.get("version"),
        "issue_bonds": issues.get("n_bonds"),
        "issue_issuers": issues.get("n_issuers"),
        "n_with_rate": issues.get("n_with_rate"),
        "n_with_change": issues.get("n_with_change"),
        "as_of": body.get("as_of"),
        "generated_at": body.get("generated_at"),
    }))
    if body.get("version") != "1.1.0" or (issues.get("n_bonds") or 0) < 1000:
        raise SystemExit("company book missing after publish: %s bonds" % issues.get("n_bonds"))


if __name__ == "__main__":
    main()
