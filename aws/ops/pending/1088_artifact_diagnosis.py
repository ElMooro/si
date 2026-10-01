"""
Ops 1088: diagnose why Lambdas run clean but produce no artifacts.

Checks CloudWatch logs for the four non-green units to see:
 - Are they actually completing?
 - Are they writing to S3? If so, what key?
 - Any silent errors or wrong paths?

Writes: aws/ops/reports/1088_artifact_diagnosis.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta

OUT_PATH = os.path.join("aws", "ops", "reports", "1088_artifact_diagnosis.json")
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

UNITS = [
    {"lambda": "justhodl-sec-8k-enrich",
     "expected": "data/sec-8k.json",
     "log_group": "/aws/lambda/justhodl-sec-8k-enrich"},
    {"lambda": "justhodl-short-interest",
     "expected": "data/short-interest-tickers.json",
     "log_group": "/aws/lambda/justhodl-short-interest"},
    {"lambda": "justhodl-corporate-actions",
     "expected": "data/corporate-actions.json",
     "log_group": "/aws/lambda/justhodl-corporate-actions"},
    {"lambda": "justhodl-xbrl-fundamentals",
     "expected": "data/xbrl-fundamentals.json",
     "log_group": "/aws/lambda/justhodl-xbrl-fundamentals"},
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402

logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def get_recent_logs(log_group, limit=5):
    """Get recent log streams and their last events. Never raises."""
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=limit).get("logStreams", [])
        result = []
        for s in streams[:3]:
            name = s["logStreamName"]
            try:
                events = logs.get_log_events(
                    logGroupName=log_group,
                    logStreamName=name,
                    limit=20,
                    startFromHead=False).get("events", [])
                # Get last few messages
                msgs = [e["message"][:200] for e in events[-5:]]
                result.append({"stream": name[-40:], "last_events": msgs})
            except Exception as e:  # noqa: BLE001
                result.append({"stream": name[-40:],
                               "error": str(e)[:60]})
        return result
    except Exception as e:  # noqa: BLE001
        return [{"error": str(e)[:100]}]


def check_s3_prefix(prefix):
    """List objects under a prefix. Never raises."""
    try:
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=10)
        objs = r.get("Contents", [])
        return [{"key": o["Key"],
                 "modified": o["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "size": o["Size"]} for o in objs]
    except Exception as e:  # noqa: BLE001
        return [{"error": str(e)[:80]}]


def main():
    """Diagnose. Write report."""
    rep = {"script": "1088_artifact_diagnosis",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": []}
    for u in UNITS:
        res = {"lambda": u["lambda"], "expected_artifact": u["expected"]}
        res["recent_logs"] = get_recent_logs(u["log_group"])
        # Check what IS in S3 under data/ with similar names
        base = u["expected"].replace("data/", "").split(".")[0].split("-")[0]
        res["s3_search"] = check_s3_prefix(f"data/{base}")
        rep["units"].append(res)

    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps({"status": "COMPLETE",
                      "units": len(rep["units"])}))


if __name__ == "__main__":
    main()
