#!/usr/bin/env python3
"""Step 299 — Drill into the audit findings:

  1. Full orphan list (24+ found in step 298, only 15 shown — capture all)
  2. RUNNING_NO_OUTPUT Lambdas — what are they actually doing?
     (valuations-agent, news-sentiment, stock-screener, backtest-engine,
      calibrator, fmp-fundamentals-agent)
  3. calibrator's 4 errors — what's the actual error?

Output: aws/ops/reports/299_orphans_and_failures.json
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/299_orphans_and_failures.json"

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def list_all_data_files():
    """List every object under data/ in the dashboard bucket."""
    objs = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []):
            objs.append({
                "key": obj["Key"],
                "size_kb": round(obj["Size"] / 1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
                "age_hours": round(
                    (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600, 1
                ),
            })
    return sorted(objs, key=lambda x: x["age_hours"])


def find_html_consumers_for_keys(keys):
    """Return dict mapping each key to list of HTML pages referencing it."""
    import subprocess
    out = {}
    # Pre-fetch all references in HTML files in one grep
    try:
        result = subprocess.run(
            ["grep", "-rohE", "data/[a-z0-9_/-]+\\.json", "--include=*.html", "."],
            capture_output=True, text=True, timeout=30,
        )
        all_refs = set()
        for line in result.stdout.split("\n"):
            line = line.strip()
            if line:
                all_refs.add(line)
    except Exception:
        all_refs = set()

    for key in keys:
        if key in all_refs:
            # Find which files
            try:
                r2 = subprocess.run(
                    ["grep", "-rln", "--include=*.html", key, "."],
                    capture_output=True, text=True, timeout=10,
                )
                pages = [
                    l.strip().lstrip("./")
                    for l in r2.stdout.split("\n")
                    if l.strip() and "/historical/" not in l and "/archive/" not in l
                ]
                out[key] = pages
            except Exception:
                out[key] = []
        else:
            out[key] = []
    return out


def get_recent_logs(fn_name, max_events=15):
    """Get the most recent log events from a Lambda."""
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{fn_name}",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        events_out = []
        for stream in streams[:2]:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{fn_name}",
                logStreamName=stream["logStreamName"], limit=max_events,
            ).get("events", [])
            for e in ev[-max_events:]:
                events_out.append({
                    "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat(),
                    "msg": e["message"][:300].strip(),
                })
        return events_out
    except Exception as e:
        return [{"err": str(e)[:120]}]


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. List all data files in S3
    print("[299] listing all S3 data/ files…")
    all_files = list_all_data_files()
    out["total_data_files"] = len(all_files)

    # 2. Find consumers for every key
    keys = [f["key"] for f in all_files]
    consumers = find_html_consumers_for_keys(keys)

    # 3. Identify orphans (file exists, fresh, no consumer)
    orphans = []
    consumed = []
    for f in all_files:
        if f["age_hours"] > 168:  # skip files older than 7d (probably retired)
            continue
        if not consumers.get(f["key"]):
            orphans.append({
                "key": f["key"],
                "size_kb": f["size_kb"],
                "age_hours": f["age_hours"],
            })
        else:
            consumed.append({
                "key": f["key"],
                "consumers": consumers[f["key"]][:3],
                "age_hours": f["age_hours"],
            })

    out["orphans_count"] = len(orphans)
    out["consumed_count"] = len(consumed)
    out["orphans"] = sorted(orphans, key=lambda x: x["age_hours"])

    # 4. Investigate the 6 RUNNING_NO_OUTPUT Lambdas
    print("[299] investigating RUNNING_NO_OUTPUT Lambdas…")
    rno = ["justhodl-valuations-agent", "fmp-fundamentals-agent",
           "justhodl-news-sentiment", "justhodl-stock-screener",
           "justhodl-backtest-engine", "justhodl-calibrator"]
    out["running_no_output"] = {}
    for fn in rno:
        out["running_no_output"][fn] = {
            "recent_logs": get_recent_logs(fn, 10),
        }

    out["duration_s"] = round(time.time() - started, 1)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps({k: v for k, v in out.items() if k != "running_no_output"},
                     indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
