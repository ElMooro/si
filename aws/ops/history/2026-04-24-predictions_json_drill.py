#!/usr/bin/env python3
"""
Drill into why predictions.json is 30h stale despite 18 successful
invocations in 48h.

Hypotheses:
  1. Lambda writes to a different S3 key than predictions.json
  2. Lambda has a guard that skips writing unless something changed
  3. Lambda doesn't actually write at all — predictions.json is from
     a different Lambda that's now retired

Investigate by reading the source.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


with report("predictions_json_drill") as r:
    r.heading("Why is predictions.json stale despite Lambda running fine?")

    # ─── Read both ML Lambda sources ───
    for fn_name in ["justhodl-ml-predictions", "MLPredictor"]:
        r.section(f"Source for {fn_name}")
        src_path = REPO_ROOT / f"aws/lambdas/{fn_name}/source/lambda_function.py"
        if not src_path.exists():
            # Try common alternate locations
            for alt in [REPO_ROOT / f"aws/lambdas/{fn_name}/source/index.py",
                        REPO_ROOT / f"aws/lambdas/{fn_name}/source/handler.py"]:
                if alt.exists():
                    src_path = alt
                    break
        if not src_path.exists():
            r.warn(f"  No source found at {src_path}")
            # List what's there
            d = REPO_ROOT / f"aws/lambdas/{fn_name}/source"
            if d.exists():
                files = list(d.iterdir())[:10]
                r.log(f"  Files in source dir: {[f.name for f in files]}")
            continue

        content = src_path.read_text(encoding="utf-8")
        r.log(f"  Source: {src_path.name} ({len(content):,} bytes)")

        # Find S3 put_object calls
        import re
        keys_written = set()
        for m in re.finditer(r"put_object\([^)]*Key\s*=\s*['\"]([^'\"]+)['\"]", content):
            keys_written.add(m.group(1))
        for m in re.finditer(r"Key\s*=\s*f?['\"]([^'\"]+)['\"]", content):
            keys_written.add(m.group(1))

        r.log(f"  S3 keys written by this Lambda: {sorted(keys_written)}")
        if "predictions.json" in keys_written:
            r.ok(f"  ✓ Writes predictions.json")
        else:
            r.log(f"  ✗ Does NOT write predictions.json")

        # Look for skip-conditions / early returns
        skip_patterns = re.findall(r"(if .*\n\s*return.*\n)", content)
        if skip_patterns:
            r.log(f"  Found {len(skip_patterns)} early-return guards in code")

    # ─── Check what S3 keys related to ml-predictions exist ───
    r.section("S3 keys containing 'predict' or 'ml'")
    try:
        # List keys at root
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="", MaxKeys=200)
        keys = resp.get("Contents", [])
        relevant = [k for k in keys if "predict" in k["Key"].lower() or "/ml" in k["Key"].lower() or k["Key"].startswith("ml")]
        r.log(f"  Found {len(relevant)} relevant keys at bucket root:")
        for k in sorted(relevant, key=lambda x: x["LastModified"], reverse=True)[:10]:
            age_h = (datetime.now(timezone.utc) - k["LastModified"]).total_seconds() / 3600
            r.log(f"    {k['Key']:40} {k['Size']:>8} bytes  ({age_h:.1f}h old)")
    except Exception as e:
        r.warn(f"  {e}")

    # Also list ml/ prefix
    r.section("S3 keys under ml/ prefix")
    try:
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="ml/", MaxKeys=50)
        keys = resp.get("Contents", [])
        r.log(f"  Found {len(keys)} keys under ml/")
        for k in sorted(keys, key=lambda x: x["LastModified"], reverse=True)[:10]:
            age_h = (datetime.now(timezone.utc) - k["LastModified"]).total_seconds() / 3600
            r.log(f"    {k['Key']:40} {k['Size']:>8} bytes  ({age_h:.1f}h old)")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── What does the recent log output say about saves? ───
    r.section("Last invocation log output for justhodl-ml-predictions")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-ml-predictions",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for s_ in streams[:1]:
            sname = s_["logStreamName"]
            st_age = (datetime.now(timezone.utc) - datetime.fromtimestamp(
                s_["lastEventTimestamp"]/1000, tz=timezone.utc)).total_seconds()/3600
            r.log(f"  Latest stream ({st_age:.1f}h old): {sname}")
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-ml-predictions",
                logStreamName=sname, limit=30, startFromHead=True,
            )
            for e in ev.get("events", [])[:30]:
                m = e["message"].strip()
                if m and not m.startswith("REPORT") and not m.startswith("END") and not m.startswith("START"):
                    r.log(f"    {m[:240]}")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
