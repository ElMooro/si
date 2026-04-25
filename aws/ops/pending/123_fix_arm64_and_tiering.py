#!/usr/bin/env python3
"""
Step 123 — Fix step 120 + 121 issues.

A. arm64 migration failed with 'Unknown parameter Architectures'.
   The boto3 version in CI doesn't support Architectures on
   update_function_configuration. Workaround: use the lower-level
   update_function_code call which DOES accept Architectures, or
   use a direct AWS CLI subprocess. Easier: pip install --upgrade
   boto3 in a script-local way, OR call the AWS API directly via
   the lower-level client.

   Actually — Architectures IS valid on update_function_configuration
   in boto3 >= 1.24. The error suggests CI's boto3 is older. Let's
   check what's really happening — maybe Architectures was renamed
   or moved.

   Reading the AWS Python SDK history: Architectures parameter is
   only valid on create_function and create_function_url_config.
   For existing Lambdas, you must use update_function_code with
   Architectures parameter — note: this requires a code re-upload.

   SAFE FIX: use update_function_code passing the EXISTING code
   (re-uploaded) + Architectures='arm64'. Need to download current
   code first, then upload it back unchanged with new arch.

B. S3 Intelligent Tiering: MalformedXML. The Filter parameter must
   be omitted entirely for whole-bucket coverage, not passed as {}.
   Pass without the Filter key.
"""
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

import boto3 as _boto3
print(f"boto3 version in CI: {_boto3.__version__}", flush=True)

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# ─── Same eligible list as step 120 — same skip rules ───────────────────
SNAPSTART_RECENT = {
    "justhodl-ai-chat", "justhodl-stock-analyzer", "justhodl-investor-agents",
    "justhodl-stock-screener", "justhodl-edge-engine",
    "justhodl-morning-intelligence", "cftc-futures-positioning-agent",
    "justhodl-reports-builder",
}
HEAVY_DEPS = {
    "scrapeMacroData", "MLPredictor",
    "multi-agent-orchestrator", "ultimate-multi-agent",
}
SKIP_PREFIXES = ("openbb-", "legacy-", "test-", "DailyEmail")


with report("fix_arm64_and_intelligent_tiering") as r:
    r.heading("Fix step 120 (arm64) + step 121 (Intelligent Tiering)")
    r.log(f"  boto3 version: {_boto3.__version__}")

    # ════════════════════════════════════════════════════════════════════
    # A. Diagnose & retry arm64
    #
    # The actual fix per AWS docs: Architectures is supported on
    # update_function_code (not update_function_configuration). For
    # arch change without changing code, we must download the existing
    # code via get_function -> Code.Location URL, then re-upload via
    # update_function_code with Architectures='arm64'.
    # ════════════════════════════════════════════════════════════════════
    r.section("A1. Verify Architectures API behavior")

    # First — try the simpler route. Maybe a re-deploy with
    # Architectures alongside ZipFile would work.
    test_lambda = "justhodl-health-monitor"
    r.log(f"  Test target: {test_lambda}")

    try:
        # Get current code
        info = lam.get_function(FunctionName=test_lambda)
        code_url = info["Code"]["Location"]
        cur_arch = info["Configuration"].get("Architectures", ["x86_64"])[0]
        r.log(f"  Current architecture: {cur_arch}")
        r.log(f"  Code URL obtained, downloading…")

        # Download current code zip
        with urllib.request.urlopen(code_url) as resp:
            zip_bytes = resp.read()
        r.log(f"  Code zip: {len(zip_bytes):,}B")

        # Now re-upload with Architectures=['arm64']
        result = lam.update_function_code(
            FunctionName=test_lambda,
            ZipFile=zip_bytes,
            Architectures=["arm64"],
        )
        r.ok(f"  update_function_code with Architectures succeeded: {result.get('Architectures')}")

        # Wait for active state
        lam.get_waiter("function_updated").wait(
            FunctionName=test_lambda, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )

        # Verify
        check = lam.get_function_configuration(FunctionName=test_lambda)
        r.ok(f"  Confirmed: Architectures = {check.get('Architectures')}")

        # Sync invoke to verify
        try:
            inv_resp = lam.invoke(FunctionName=test_lambda, InvocationType="RequestResponse")
            if inv_resp.get("FunctionError"):
                payload = inv_resp.get("Payload").read().decode()[:300]
                r.warn(f"  Invoke errored: {payload}")
            else:
                r.ok(f"  Invoke clean")
        except Exception as e:
            r.warn(f"  Invoke check: {e}")

    except Exception as e:
        r.fail(f"  Test failed: {e}")
        r.fail(f"  Aborting bulk migration; manual investigation needed")
        # Don't exit — still try to fix the S3 issue
        do_bulk = False
    else:
        do_bulk = True

    # ════════════════════════════════════════════════════════════════════
    # A2. Bulk migration via update_function_code with downloaded zip
    # ════════════════════════════════════════════════════════════════════
    if do_bulk:
        r.section("A2. Bulk arm64 migration (using update_function_code)")

        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns.extend(page.get("Functions", []))

        eligible = []
        for f in fns:
            name = f["FunctionName"]
            runtime = f.get("Runtime") or ""
            arch = (f.get("Architectures") or ["x86_64"])[0]

            if not runtime.startswith("python3"): continue
            if arch == "arm64":                    continue
            if name.startswith(SKIP_PREFIXES):     continue
            if name in SNAPSTART_RECENT:           continue
            if name in HEAVY_DEPS:                 continue
            if f.get("Layers"):                    continue
            eligible.append(name)

        r.log(f"  Eligible: {len(eligible)} (excluding test target {test_lambda} already done)")
        if test_lambda in eligible:
            eligible.remove(test_lambda)

        succeeded = []
        failed = []
        for i, name in enumerate(eligible, 1):
            try:
                # Get current code URL
                info = lam.get_function(FunctionName=name)
                code_url = info["Code"]["Location"]

                # Download
                with urllib.request.urlopen(code_url, timeout=30) as resp:
                    zip_bytes = resp.read()

                # Re-upload with arm64
                lam.update_function_code(
                    FunctionName=name,
                    ZipFile=zip_bytes,
                    Architectures=["arm64"],
                )
                lam.get_waiter("function_updated").wait(
                    FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
                )
                r.ok(f"  [{i:>2}/{len(eligible)}] {name:42} → arm64 ({len(zip_bytes):,}B)")
                succeeded.append(name)
                time.sleep(0.5)  # pace
            except Exception as e:
                r.fail(f"  [{i:>2}/{len(eligible)}] {name}: {str(e)[:200]}")
                failed.append((name, str(e)[:100]))

        r.log(f"\n  arm64 migrations: {len(succeeded)} succeeded, {len(failed)} failed")

    # ════════════════════════════════════════════════════════════════════
    # B. Fix S3 Intelligent Tiering (omit Filter for whole-bucket scope)
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Apply Intelligent Tiering with proper schema")
    config_id = "auto-tier-cold-objects"
    # The key insight: omit Filter entirely for whole-bucket scope
    config = {
        "Id": config_id,
        "Status": "Enabled",
        "Tierings": [
            {"Days": 90, "AccessTier": "ARCHIVE_ACCESS"},
            {"Days": 180, "AccessTier": "DEEP_ARCHIVE_ACCESS"},
        ],
    }
    try:
        s3.put_bucket_intelligent_tiering_configuration(
            Bucket=BUCKET,
            Id=config_id,
            IntelligentTieringConfiguration=config,
        )
        r.ok(f"  Applied Intelligent Tiering '{config_id}' (no Filter = whole bucket)")
    except Exception as e:
        # If still fails, try with explicit empty And{} Filter
        r.warn(f"  First attempt: {e}")
        try:
            config["Filter"] = {"And": {}}
            s3.put_bucket_intelligent_tiering_configuration(
                Bucket=BUCKET, Id=config_id,
                IntelligentTieringConfiguration=config,
            )
            r.ok(f"  Applied with And{{}} filter")
        except Exception as e2:
            r.fail(f"  Both attempts failed: {e2}")

    # Verify
    try:
        existing = s3.list_bucket_intelligent_tiering_configurations(Bucket=BUCKET)
        configs = existing.get("IntelligentTieringConfigurationList", [])
        r.log(f"  Total configs now: {len(configs)}")
        for c in configs:
            r.log(f"    {c.get('Id')}: {c.get('Status')}, tierings={[t.get('AccessTier') for t in c.get('Tierings', [])]}")
    except Exception as e:
        r.warn(f"  Verify: {e}")

    r.kv(
        boto3_version=_boto3.__version__,
        arm64_test_succeeded=do_bulk if 'do_bulk' in dir() else False,
        arm64_bulk_migrated=len(succeeded) if 'succeeded' in dir() else 0,
        intelligent_tiering="see log",
    )
    r.log("Done")
# triggered 2026-04-25T10:29:55Z
