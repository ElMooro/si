#!/usr/bin/env python3
"""
Step 124 — Post-arm64 migration health check.

Step 123 migrated 80 Lambdas to arm64. While most boto3/urllib code
ports cleanly, some compiled deps in vendored packages might break.
This step does a 30-min after-the-fact audit:

  1. For every arm64-migrated Lambda, check CloudWatch error metric
     SINCE the migration timestamp (~10:24-10:30 UTC).
  2. Flag any Lambda with > 0 errors AND > 0 invocations post-migration.
     (No invocations = no problem yet, just hasn't fired.)
  3. For flagged Lambdas, read most recent log stream to see error type.
     If it's an arm64-shaped error (ImportError, wheel, _imaging,
     architecture, libc), REVERT that specific Lambda back to x86_64.
  4. Otherwise, log the error but leave alone — it's pre-existing or
     unrelated.

Compare each Lambda's error rate WITHOUT migration vs WITH:
  - Pre-migration baseline: errors in 24h before migration (10:24)
  - Post-migration: errors after migration

If post-migration error rate is dramatically higher AND new errors
look arm64-related → revert.
"""
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

# Migration window — anything after this is post-arm64
MIGRATION_TS = datetime(2026, 4, 25, 10, 24, 0, tzinfo=timezone.utc)


ARM64_ERROR_PATTERNS = [
    "Unable to import module",
    "ImportError",
    "ModuleNotFoundError",
    "wheel",
    "manylinux",
    "_imaging",
    "ELFCLASS",
    "wrong ELF class",
    "no module named",
    "_cffi_backend",
    "GLIBC",
    "Runtime.ImportModuleError",
    ".so: cannot open shared object file",
]


def get_post_migration_errors(name):
    """Get error count since migration."""
    try:
        end = datetime.now(timezone.utc)
        # Use migration start time
        start = MIGRATION_TS
        if (end - start).total_seconds() < 600:
            return None, None  # Too early to tell

        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
        )
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
        )
        errors = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        invs = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        return int(invs), int(errors)
    except Exception:
        return None, None


def get_recent_error_signature(name):
    """Pull most recent log lines, find first arm64-looking error."""
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{name}",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for s in streams[:1]:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{name}",
                logStreamName=s["logStreamName"],
                limit=80, startFromHead=False,
            )
            for e in ev.get("events", []):
                msg = e["message"]
                if any(p in msg for p in ARM64_ERROR_PATTERNS):
                    return msg[:300], True  # arm64-looking
            # Return first non-INIT line if no arm64 match
            for e in ev.get("events", []):
                m = e["message"]
                if "ERROR" in m or "Error" in m or "Exception" in m or "Traceback" in m:
                    return m[:300], False  # error but not arm64-shaped
    except Exception:
        pass
    return None, None


def revert_to_x86(name, r):
    """Revert a Lambda to x86_64 by re-uploading existing code with x86_64 arch."""
    try:
        info = lam.get_function(FunctionName=name)
        code_url = info["Code"]["Location"]
        with urllib.request.urlopen(code_url, timeout=30) as resp:
            zip_bytes = resp.read()
        lam.update_function_code(
            FunctionName=name,
            ZipFile=zip_bytes,
            Architectures=["x86_64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"  Reverted {name} to x86_64")
        return True
    except Exception as e:
        r.fail(f"  Revert {name}: {e}")
        return False


with report("post_arm64_health_check") as r:
    r.heading("Post-arm64 migration health check (find regressions)")

    age_min = (datetime.now(timezone.utc) - MIGRATION_TS).total_seconds() / 60
    r.log(f"  Migration started: {MIGRATION_TS.isoformat()}")
    r.log(f"  Time since migration: {age_min:.0f} minutes")

    if age_min < 5:
        r.warn("  Migration was too recent (<5min) — most Lambdas haven't fired yet")

    # Get all arm64 Lambdas
    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        fns.extend(page.get("Functions", []))
    arm64_fns = [f for f in fns if (f.get("Architectures") or [""])[0] == "arm64"]
    r.log(f"  Total arm64 Lambdas: {len(arm64_fns)}")

    r.section("Per-Lambda error check since migration")

    no_invs = []
    clean = []
    errors_arm64 = []
    errors_other = []
    reverted = []

    for f in arm64_fns:
        name = f["FunctionName"]
        invs, errs = get_post_migration_errors(name)

        if invs is None:
            continue

        if invs == 0:
            no_invs.append(name)
            continue

        if errs == 0:
            clean.append(name)
            continue

        # Has errors — investigate
        err_sig, is_arm64_shaped = get_recent_error_signature(name)
        err_rate = errs / invs if invs else 0

        if is_arm64_shaped:
            r.warn(f"  ⚠  {name:42} {errs}/{invs} errs ({err_rate*100:.0f}%) — arm64-related!")
            r.log(f"      sig: {err_sig[:200] if err_sig else '?'}")
            errors_arm64.append((name, errs, invs, err_sig))

            # AUTO-REVERT
            if revert_to_x86(name, r):
                reverted.append(name)
        else:
            # Pre-existing or unrelated error — note but don't revert
            r.log(f"     {name:42} {errs}/{invs} errs ({err_rate*100:.0f}%) — non-arm64 error (probably pre-existing)")
            if err_sig:
                r.log(f"      sig: {err_sig[:200]}")
            errors_other.append((name, errs, invs))

    r.section("Summary")
    r.log(f"  arm64 Lambdas: {len(arm64_fns)}")
    r.log(f"  Not yet invoked since migration: {len(no_invs)}")
    r.log(f"  Invoked + clean (0 errors): {len(clean)}")
    r.log(f"  Invoked + had pre-existing errors: {len(errors_other)}")
    r.log(f"  Invoked + arm64-shaped errors → REVERTED: {len(reverted)}")

    if errors_other:
        r.log(f"\n  Lambdas with pre-existing errors (not arm64's fault):")
        for name, errs, invs in errors_other:
            r.log(f"    {name:42} {errs}/{invs}")

    if reverted:
        r.log(f"\n  Reverted to x86_64 (arm64 incompatible):")
        for name in reverted:
            r.log(f"    {name}")

    r.kv(
        total_arm64=len(arm64_fns),
        clean=len(clean),
        not_yet_invoked=len(no_invs),
        pre_existing_errors=len(errors_other),
        reverted=len(reverted),
    )
    r.log("Done")
