"""
Ops 1058: targeted deep-read for the remaining unknowns.

1. Dump report["upgrades"] from the 1054 S3 report (the 1057 heuristic missed it).
2. Read the 1051 S3 report: what did ops 1051 actually do per R-engine schedule?
3. List EventBridge Scheduler schedules matching the 9 R-engine names under ANY
   justhodl- prefix, to resolve whether the schedules exist under a different name.

Writes: aws/ops/reports/1058_details.json (committed back by run-ops workflow).
Never raises. Output capped.
"""
from __future__ import annotations

import json
import os
import time

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_PATH = os.path.join("aws", "ops", "reports", "1058_details.json")
CAP = 1200

ENGINES = [
    "justhodl-factor-decomposition", "justhodl-fx-decomposition",
    "justhodl-fedwatch-rate-probability", "justhodl-cftc-deep-view",
    "justhodl-sec-filing-diff", "justhodl-transcript-query",
    "justhodl-peer-comparison", "justhodl-screen-builder",
    "justhodl-supply-chain-linkage",
]


def cap(obj, depth=0):
    """Truncate long strings/nests. Never raises."""
    try:
        if isinstance(obj, str):
            return obj[:CAP] + ("..." if len(obj) > CAP else "")
        if isinstance(obj, dict) and depth < 4:
            return {k: cap(v, depth + 1) for k, v in obj.items()}
        if isinstance(obj, list) and depth < 4:
            return [cap(v, depth + 1) for v in obj[:40]]
        return obj if isinstance(obj, (int, float, bool)) or obj is None else "<...>"
    except Exception:  # never break the report
        return "<unserializable>"


def read_s3_json(s3, key):
    """Fetch one S3 JSON. Returns (ok, data_or_error). Never raises."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return True, json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:  # missing key, bad JSON, permissions
        return False, type(e).__name__


def list_matching_schedules(scheduler):
    """Find Scheduler schedules whose name contains any R-engine name. Never raises."""
    found = []
    try:
        paginator = scheduler.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                name = s.get("Name", "")
                if any(e in name for e in ENGINES):
                    found.append({"name": name, "state": s.get("State")})
    except Exception as e:  # permissions / API failure
        return {"error": type(e).__name__}
    return {"matches": found, "count": len(found)}


def main():
    """Gather the three unknowns, write the details file, print it."""
    s3 = boto3.client("s3", region_name="us-east-1")
    scheduler = boto3.client("scheduler", region_name="us-east-1")
    details = {
        "script": "1058_targeted_read",
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    ok, data = read_s3_json(s3, "ops/reports/1054.json")
    if ok and isinstance(data, dict):
        upgrades = data.get("upgrades")
        details["upgrades_map"] = cap(upgrades) if isinstance(upgrades, dict) else {
            "unexpected_type": type(upgrades).__name__}
        details["upgrades_keys"] = sorted(upgrades.keys()) if isinstance(upgrades, dict) else []
    else:
        details["upgrades_map"] = {"unreadable": data}

    ok, data = read_s3_json(s3, "ops/reports/1051.json")
    details["report_1051"] = cap(data) if ok else {"unreadable": data}

    details["scheduler_scan"] = list_matching_schedules(scheduler)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(details, f, indent=2)
    print(json.dumps(details, indent=2)[:18000])


if __name__ == "__main__":
    main()
