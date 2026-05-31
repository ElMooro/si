#!/usr/bin/env python3
"""1077 — refresh stale homepage tiles by triggering producer Lambdas.

Uses Event (async) invocation pattern from ops/1059 — fires the Lambda
without holding a connection, then polls S3 for fresh output up to 8min.
This avoids the 15-min runner cap when Lambdas take >5min to complete.

Targets (per ops/1076 freshness check):
  - cot/extremes/current.json (52h stale) → cftc-futures-positioning-agent
  - edge-data.json (601h stale!)          → justhodl-edge-engine
  - flow-data.json (601h stale!)          → justhodl-options-flow
"""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1077_refresh_stale.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)

TARGETS = [
    {"lambda": "cftc-futures-positioning-agent", "key": "cot/extremes/current.json"},
    {"lambda": "justhodl-edge-engine",            "key": "edge-data.json"},
    {"lambda": "justhodl-options-flow",           "key": "flow-data.json"},
]


def initial_state(key):
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        return obj["LastModified"]
    except Exception:
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "refreshes": []}
    
    # Phase 1: capture initial timestamps + async-invoke all targets
    for t in TARGETS:
        name = t["lambda"]
        key = t["key"]
        entry = {"lambda": name, "key": key}
        
        # Initial state
        entry["before_last_modified"] = initial_state(key).isoformat() if initial_state(key) else None
        
        # Async invoke
        print(f"[1077] async-invoke {name}…")
        try:
            r = lam.invoke(FunctionName=name, InvocationType="Event", Payload=b"{}")
            entry["invoke_status"] = r.get("StatusCode")
        except Exception as e:
            entry["invoke_err"] = str(e)[:200]
        out["refreshes"].append(entry)
        time.sleep(2)
    
    # Phase 2: poll S3 for each output to refresh (up to ~7min)
    start = time.time()
    deadline = start + 7 * 60
    
    for entry in out["refreshes"]:
        name = entry["lambda"]
        key = entry["key"]
        before = entry.get("before_last_modified")
        print(f"[1077] polling S3 for {key}…")
        elapsed_check_start = time.time()
        while time.time() < deadline:
            try:
                obj = s3.head_object(Bucket=BUCKET, Key=key)
                lm = obj["LastModified"].isoformat()
                if before and lm > before:
                    entry["refreshed_at"] = lm
                    entry["poll_seconds"] = round(time.time() - elapsed_check_start, 1)
                    entry["new_size"] = obj["ContentLength"]
                    print(f"[1077]   ✓ refreshed: {key} at {lm}")
                    break
            except Exception:
                pass
            time.sleep(15)
        else:
            entry["timeout"] = True
            print(f"[1077]   ⏱ timeout waiting for {key}")
        
        # Also peek at the new content
        if entry.get("refreshed_at"):
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                body = obj["Body"].read()
                try:
                    d = json.loads(body)
                    # Look for null/zero indicators
                    s_content = json.dumps(d)
                    n_zero = s_content.count(": 0,") + s_content.count(": null") + s_content.count(': "0"')
                    n_total = s_content.count(":")
                    entry["new_null_ratio"] = round(n_zero / max(n_total, 1), 2)
                    if isinstance(d, dict):
                        entry["new_top_keys"] = list(d.keys())[:10]
                        meta = d.get("meta") or {}
                        entry["new_generated_at"] = meta.get("generated_at")
                except Exception:
                    entry["new_size_only"] = len(body)
            except Exception as e:
                entry["new_read_err"] = str(e)[:100]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1077] DONE → {REPORT}")


if __name__ == "__main__":
    main()
