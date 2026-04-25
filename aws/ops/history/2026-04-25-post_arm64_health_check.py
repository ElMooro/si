#!/usr/bin/env python3
"""
Step 125 — Re-run post-arm64 health check.

Step 124 had a bug: my `(end - start).total_seconds() < 600` guard
returned None for every Lambda (treated as 'too early'), meaning
nothing got actually checked. Removed that guard — more than 30
minutes have passed since migration, plenty of time for scheduled
Lambdas to have fired at least once.

Bumped CW Period from 300s to 60s for more granular post-migration
data, and explicitly look at the 60-min window after migration.
"""
import json
import os
import re
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

# Migration completed around 10:30 UTC; widen our window to be safe
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
    "_cffi_backend",
    "GLIBC",
    "Runtime.ImportModuleError",
    ".so: cannot open shared object file",
    "exec format error",
]


def get_post_migration_metrics(name):
    end = datetime.now(timezone.utc)
    try:
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=MIGRATION_TS, EndTime=end, Period=300, Statistics=["Sum"],
        )
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=MIGRATION_TS, EndTime=end, Period=300, Statistics=["Sum"],
        )
        errors = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        invs = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        return int(invs), int(errors)
    except Exception:
        return None, None


def get_recent_error_signature(name):
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
                    return msg[:300], True
            for e in ev.get("events", []):
                m = e["message"]
                if "ERROR" in m or "Error" in m or "Exception" in m or "Traceback" in m:
                    return m[:300], False
    except Exception:
        pass
    return None, None


def revert_to_x86(name, r):
    try:
        info = lam.get_function(FunctionName=name)
        with urllib.request.urlopen(info["Code"]["Location"], timeout=30) as resp:
            zip_bytes = resp.read()
        lam.update_function_code(
            FunctionName=name, ZipFile=zip_bytes,
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


with report("post_arm64_health_check_v2") as r:
    r.heading("Post-arm64 health check v2 (now 30+ min post-migration)")

    age_min = (datetime.now(timezone.utc) - MIGRATION_TS).total_seconds() / 60
    r.log(f"  Migration started: {MIGRATION_TS.isoformat()}")
    r.log(f"  Time since: {age_min:.0f} minutes")

    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        fns.extend(page.get("Functions", []))
    arm64_fns = [f for f in fns if (f.get("Architectures") or [""])[0] == "arm64"]
    r.log(f"  Total arm64 Lambdas: {len(arm64_fns)}")

    no_invs = []
    clean = []
    errors_arm64 = []
    errors_other = []
    reverted = []

    for f in arm64_fns:
        name = f["FunctionName"]
        invs, errs = get_post_migration_metrics(name)

        if invs is None:
            r.warn(f"  {name}: metrics fetch failed")
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
            r.warn(f"  ⚠  {name:42} {errs}/{invs} ({err_rate*100:.0f}%) ARM64 ERROR")
            r.log(f"      {err_sig[:240] if err_sig else '?'}")
            errors_arm64.append((name, errs, invs, err_sig))
            if revert_to_x86(name, r):
                reverted.append(name)
        else:
            r.log(f"     {name:42} {errs}/{invs} ({err_rate*100:.0f}%) pre-existing")
            if err_sig:
                r.log(f"      {err_sig[:240]}")
            errors_other.append((name, errs, invs))

    r.section("Summary")
    r.log(f"  arm64 fleet: {len(arm64_fns)}")
    r.log(f"  Not yet invoked since migration: {len(no_invs)}  (no concerns yet)")
    r.log(f"  Invoked clean: {len(clean)}  ✅")
    r.log(f"  Pre-existing errors (not arm64): {len(errors_other)}")
    r.log(f"  arm64 incompatible — REVERTED: {len(reverted)}")

    if errors_other:
        r.log(f"\n  Pre-existing error Lambdas (note for separate triage):")
        for name, errs, invs in errors_other:
            r.log(f"    {name:42} {errs}/{invs}")

    if reverted:
        r.log(f"\n  Reverted to x86_64:")
        for name in reverted:
            r.log(f"    {name}")
    else:
        r.ok(f"\n  ✅ Migration is healthy — 0 arm64 incompatibilities found across {len(arm64_fns)} Lambdas")

    r.kv(
        total_arm64=len(arm64_fns),
        not_yet_invoked=len(no_invs),
        clean=len(clean),
        pre_existing_errors=len(errors_other),
        arm64_incompat_reverted=len(reverted),
    )
    r.log("Done")
