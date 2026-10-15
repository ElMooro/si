#!/usr/bin/env python3
"""Publish once the flow build is the live corp-bonds function."""
import json
import time

import boto3
from botocore.config import Config

FN = "justhodl-corp-bonds"
BUCKET = "justhodl-dashboard-live"
KEY = "data/corp-bonds.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=640, connect_timeout=10))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    base = lam.get_function(FunctionName=FN)["Configuration"]["CodeSha256"]
    deadline = time.time() + 18 * 60
    while time.time() < deadline:
        cfg = lam.get_function(FunctionName=FN)["Configuration"]
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("CodeSha256") != base and int(cfg.get("Timeout") or 0) >= 600:
            break
        time.sleep(15)
    else:
        raise SystemExit("justhodl-corp-bonds did not take the flow build")
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"action": "publish"}).encode())
    payload = json.loads(resp["Payload"].read() or b"{}")
    print("invoke", resp.get("StatusCode"), resp.get("FunctionError"), str(payload)[:700])
    if resp.get("FunctionError"):
        raise SystemExit("publish failed")
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    issues = body.get("issues") or {}
    bonds = issues.get("bonds") or []
    sample = bonds[0] if bonds else {}
    leaked = [b.get("k") for b in bonds if any(k in b for k in ("cusip", "isin", "sedol"))]
    print(json.dumps({
        "version": body.get("version"),
        "n_bonds": issues.get("n_bonds"),
        "n_left": issues.get("n_left"),
        "n_with_dur": issues.get("n_with_dur"),
        "n_with_move": issues.get("n_with_move"),
        "n_with_flow": issues.get("n_with_flow"),
        "n_with_ytd": issues.get("n_with_ytd"),
        "dur": issues.get("dur"),
        "move_as_of": issues.get("move_as_of"),
        "ytd_as_of": issues.get("ytd_as_of"),
        "as_of": issues.get("as_of"),
        "sample_keys": sorted(sample.keys()),
        "leaked": len(leaked),
    }))
    if leaked:
        raise SystemExit("identifier leaked")
    if body.get("version") != "1.3.0":
        raise SystemExit("version %s" % body.get("version"))
    if (issues.get("n_with_move") or 0) < 1000 or (issues.get("n_with_dur") or 0) < 10000:
        raise SystemExit("mover book missing")
    if (issues.get("n_with_flow") or 0) < 1000 or (issues.get("n_with_ytd") or 0) < 1000:
        raise SystemExit("flow book missing")


if __name__ == "__main__":
    main()
