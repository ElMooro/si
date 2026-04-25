#!/usr/bin/env python3
"""
Step 130 — Diagnose intelligence Lambda response + verify Loop 1.

Step 129 successfully patched + deployed the Lambda, but the sync
invoke returned a tiny 262B payload with no risk fields. Likely the
Lambda routes its output to S3 (data/intel-* file) rather than
returning the pred dict in the invoke response.

This step:
  1. Inspect the real lambda_handler's return shape — does it write
     to S3? What key?
  2. Look at recent S3 outputs from this Lambda to find the actual
     output file
  3. Read that file and verify our new calibration fields are present
  4. Look at the raw 262B payload to understand what we got

If the integration IS working (just hitting a different output
channel), we just need to verify via S3. If it's not working,
investigate the ACTUAL handler code path.
"""
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


with report("verify_calibration_via_s3") as r:
    r.heading("Verify Loop 1 integration in justhodl-intelligence (via S3 + handler trace)")

    # ─── 1. Read the lambda_handler — find return paths + S3 writes ────
    r.section("1. Inspect lambda_handler in source")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text()

    # Find lambda_handler
    m = re.search(r"def lambda_handler\(.*?\):", src)
    if m:
        start = m.start()
        # Take the next ~80 lines
        lines = src[start:].split("\n")[:120]
        r.log(f"  lambda_handler starts at L{src[:start].count(chr(10)) + 1}, showing first 120 lines:")
        for i, ln in enumerate(lines, 0):
            if ln.strip():
                r.log(f"    {i+1:>3}: {ln[:200]}")
            if "return" in ln and ("statusCode" in ln or "{" in ln):
                # Stop at the main return statement
                # Show 5 more lines for context
                for j in range(i+1, min(i+6, len(lines))):
                    r.log(f"    {j+1:>3}: {lines[j][:200]}")
                break

    # Find every put_object call
    r.log(f"\n  S3 put_object calls in source:")
    for m in re.finditer(r"put_object\s*\(\s*Bucket\s*=\s*[^,]+,\s*Key\s*=\s*[fr]?['\"]([^'\"]+)['\"]", src):
        r.log(f"    Key: {m.group(1)}")

    # Find every f-string Key= pattern
    r.log(f"\n  f-string Key patterns:")
    for m in re.finditer(r"Key\s*=\s*f['\"]([^'\"]+)['\"]", src):
        r.log(f"    Key: {m.group(1)}")

    # ─── 2. Decode the 262B sync invoke response ────────────────────────
    r.section("2. Re-invoke + inspect the actual response payload")
    import time
    time.sleep(2)
    resp = lam.invoke(FunctionName="justhodl-intelligence",
                       InvocationType="RequestResponse",
                       Payload=b"{}")
    raw = resp.get("Payload").read().decode()
    r.log(f"  Raw payload ({len(raw)}B):")
    r.log(f"    {raw[:1000]}")
    if resp.get("FunctionError"):
        r.warn(f"  FunctionError: {resp.get('FunctionError')}")

    # ─── 3. Look at recent S3 keys this Lambda writes ──────────────────
    r.section("3. Recent S3 keys under intelligence-related paths")
    candidate_prefixes = [
        "intelligence-report.json",  # spotted in step 114
        "data/intelligence",
        "intelligence/",
    ]
    for prefix in candidate_prefixes:
        try:
            resp_l = s3.list_objects_v2(
                Bucket="justhodl-dashboard-live",
                Prefix=prefix.rstrip("/"),
                MaxKeys=5,
            )
            objs = resp_l.get("Contents", [])
            for obj in objs[:5]:
                age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
                r.log(f"    {obj['Key']:60} {obj['Size']:>10}B  age {age_min:.1f}m")
        except Exception as e:
            r.warn(f"    {prefix}: {e}")

    # ─── 4. Read intelligence-report.json (most likely target) ──────────
    r.section("4. Read intelligence-report.json — find calibration fields")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="intelligence-report.json")
        body_raw = obj["Body"].read().decode("utf-8")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  intelligence-report.json: {len(body_raw):,}B, age {age_min:.1f}m")
        data = json.loads(body_raw)
        r.log(f"  Top-level keys: {sorted(data.keys())}")

        # Look for our new fields
        if "pred" in data:
            pred = data["pred"]
            r.log(f"\n  pred keys: {sorted(pred.keys()) if isinstance(pred, dict) else 'not dict'}")
            if isinstance(pred, dict):
                cal = pred.get("calibration")
                if cal:
                    r.log(f"\n  ✅ pred.calibration found:")
                    for k, v in cal.items():
                        r.log(f"    {k:25} {v}")
                risk = pred.get("risk", {})
                if isinstance(risk, dict):
                    r.log(f"\n  pred.risk fields:")
                    for k, v in risk.items():
                        if k != "calibration_meta":
                            r.log(f"    {k:25} {v}")
                    cm = risk.get("calibration_meta")
                    if cm:
                        r.log(f"\n  ✅ pred.risk.calibration_meta:")
                        r.log(f"    is_calibrated: {cm.get('is_calibrated')}")
                        r.log(f"    n_calibrated:  {cm.get('n_calibrated')}")
                        r.log(f"    n_signals:     {cm.get('n_signals')}")
                        for c in cm.get("contributions", []):
                            r.log(f"      {c.get('signal_type'):20} "
                                  f"score={c.get('score'):.1f} "
                                  f"weight={c.get('weight'):.2f} "
                                  f"calibrated={c.get('calibrated')}")
        else:
            r.log(f"  No 'pred' key — output structure differs from expected")
            # Search for our fields anywhere in the doc
            doc_str = json.dumps(data, default=str)
            for keyword in ["calibrated_composite", "raw_composite", "calibration_meta", "is_meaningful"]:
                if keyword in doc_str:
                    r.log(f"  Found '{keyword}' somewhere in the response ✓")
                else:
                    r.log(f"  '{keyword}' NOT in response")

    except Exception as e:
        r.warn(f"  Couldn't read intelligence-report.json: {e}")

    # ─── 5. Most recent log lines from intelligence Lambda ──────────────
    r.section("5. Most recent log stream from intelligence Lambda")
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/justhodl-intelligence",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for s in streams[:1]:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/justhodl-intelligence",
                logStreamName=s["logStreamName"],
                limit=30, startFromHead=False,
            )
            r.log(f"  Stream: {s['logStreamName']}")
            r.log(f"  Last 30 lines:")
            for e in ev.get("events", [])[-30:]:
                msg = e["message"].rstrip()
                r.log(f"    {msg[:240]}")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
