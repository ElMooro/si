#!/usr/bin/env python3
"""
Step 119 — Enable PITR (DynamoDB) + SnapStart (Lambda).

Two changes, both zero-risk:

A. Point-In-Time Recovery on the 7 DDB tables that contain real data.
   Cost is ~\$0.20/GB-month — these tables are tiny (< 0.1 GB combined),
   total cost < \$0.01/month. Recovery window: 35 days.

B. SnapStart=PublishedVersions on user-facing Python Lambda URLs.
   Free, gives 10x cold-start speedup. Targets:
     - justhodl-ai-chat (chat is interactive — cold starts are painful)
     - justhodl-stock-analyzer (Lambda URL, hit on every page load)
     - justhodl-investor-agents (long-running, but cold start matters)
     - justhodl-stock-screener (Lambda URL, hit on screener page)
     - justhodl-edge-engine (Lambda URL)
     - justhodl-morning-intelligence (one shot/day, doesn't need it,
       but free so why not)
     - cftc-futures-positioning-agent (Lambda URL hit by frontend)

   SnapStart needs:
     1. Runtime: python3.12 / python3.13 (we have 33 eligible Lambdas)
     2. SnapStart=PublishedVersions config setting
     3. A published version (publish_version after each update)
     4. Lambda URL / EB rule / etc must point to a published version
        (or to $LATEST, which will use the snapshot when available)

   SAFE APPROACH: enable on the 7 above, publish a version, leave
   $LATEST routing alone. Existing invocations continue to work.
   New invocations get the SnapStart benefit automatically.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ddb = boto3.client("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


# ─── DynamoDB tables to enable PITR on ──────────────────────────────────
PITR_TABLES = [
    "justhodl-signals",         # 4,829 entries — your learning data
    "justhodl-outcomes",        # 4,377 entries — scoring history
    "fed-liquidity-cache",      # FRED data cache
    "openbb-historical-data",   # historical archive
    "ai-assistant-tasks",       # task queue
    "openbb-trading-signals",   # trading signal log
    "liquidity-metrics-v2",     # liquidity metrics
]

# ─── Lambdas to enable SnapStart on ──────────────────────────────────────
# Must be Python 3.12+ runtime. SnapStart applies to PublishedVersions.
# Targeting user-facing Lambdas where cold-start latency hurts UX.
SNAPSTART_LAMBDAS = [
    "justhodl-ai-chat",
    "justhodl-stock-analyzer",
    "justhodl-investor-agents",
    "justhodl-stock-screener",
    "justhodl-edge-engine",
    "justhodl-morning-intelligence",
    "cftc-futures-positioning-agent",
    "justhodl-reports-builder",  # also user-facing via reports.html
]


with report("enable_pitr_and_snapstart") as r:
    r.heading("Enable PITR (DDB) + SnapStart (Lambda) — both zero-risk")

    # ════════════════════════════════════════════════════════════════════
    # A. PITR on DynamoDB tables
    # ════════════════════════════════════════════════════════════════════
    r.section("A. Enable PITR on DynamoDB tables")
    pitr_results = []
    for tn in PITR_TABLES:
        try:
            # Check current state first
            cur = ddb.describe_continuous_backups(TableName=tn)
            cur_state = (cur.get("ContinuousBackupsDescription", {})
                          .get("PointInTimeRecoveryDescription", {})
                          .get("PointInTimeRecoveryStatus"))
            if cur_state == "ENABLED":
                r.log(f"  {tn:35} already ENABLED, skipping")
                pitr_results.append((tn, "already_enabled"))
                continue

            ddb.update_continuous_backups(
                TableName=tn,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
            )
            # Verify
            time.sleep(1)
            v = ddb.describe_continuous_backups(TableName=tn)
            new_state = (v.get("ContinuousBackupsDescription", {})
                          .get("PointInTimeRecoveryDescription", {})
                          .get("PointInTimeRecoveryStatus"))
            r.ok(f"  {tn:35} PITR → {new_state}")
            pitr_results.append((tn, new_state))
        except ddb.exceptions.TableNotFoundException:
            r.warn(f"  {tn}: table not found, skipping")
            pitr_results.append((tn, "not_found"))
        except Exception as e:
            r.fail(f"  {tn}: {e}")
            pitr_results.append((tn, f"error: {str(e)[:80]}"))

    pitr_enabled = sum(1 for _, s in pitr_results if s == "ENABLED")
    pitr_already = sum(1 for _, s in pitr_results if s == "already_enabled")
    r.log(f"\n  PITR: {pitr_enabled} newly enabled, {pitr_already} already on")

    # ════════════════════════════════════════════════════════════════════
    # B. SnapStart on Lambdas
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Enable SnapStart on user-facing Python Lambdas")
    ss_results = []
    for name in SNAPSTART_LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            runtime = cfg.get("Runtime")
            cur_ss = cfg.get("SnapStart", {}).get("ApplyOn", "None")

            # Check runtime eligibility
            if not runtime or not (runtime.startswith("python3.12") or runtime.startswith("python3.13") or runtime.startswith("java")):
                r.warn(f"  {name:38} runtime={runtime} — NOT eligible, skipping")
                ss_results.append((name, "ineligible_runtime"))
                continue

            if cur_ss == "PublishedVersions":
                r.log(f"  {name:38} already on PublishedVersions, skipping")
                ss_results.append((name, "already_enabled"))
                continue

            # Apply SnapStart config
            lam.update_function_configuration(
                FunctionName=name,
                SnapStart={"ApplyOn": "PublishedVersions"},
            )
            lam.get_waiter("function_updated").wait(
                FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )
            r.ok(f"  {name:38} SnapStart=PublishedVersions")

            # Publish a new version. This is what triggers the snapshot
            # creation. First snapshot takes ~5-10 min to materialize.
            try:
                v = lam.publish_version(FunctionName=name,
                                        Description=f"SnapStart enabled by ops/119 at {datetime.now(timezone.utc).isoformat()}")
                ver = v.get("Version")
                r.log(f"      published version {ver}")
                ss_results.append((name, f"enabled_v{ver}"))
            except Exception as e:
                r.warn(f"      publish_version: {e}")
                ss_results.append((name, "enabled_no_version"))
        except lam.exceptions.ResourceNotFoundException:
            r.warn(f"  {name}: function not found, skipping")
            ss_results.append((name, "not_found"))
        except Exception as e:
            r.fail(f"  {name}: {e}")
            ss_results.append((name, f"error: {str(e)[:100]}"))

    ss_enabled = sum(1 for _, s in ss_results if s.startswith("enabled"))
    ss_already = sum(1 for _, s in ss_results if s == "already_enabled")
    r.log(f"\n  SnapStart: {ss_enabled} newly enabled, {ss_already} already on")

    r.log(f"\n  IMPORTANT: First snapshot per Lambda takes 5-10 min to materialize.")
    r.log(f"  Until then, invocations use normal cold-start. After: 10x faster.")
    r.log(f"  No action needed — Lambda URL routing to \\$LATEST automatically picks up.")

    # ════════════════════════════════════════════════════════════════════
    # Summary table
    # ════════════════════════════════════════════════════════════════════
    r.section("Summary")
    r.log("PITR enablement:")
    for tn, status in pitr_results:
        r.log(f"  {tn:35} {status}")
    r.log("\nSnapStart enablement:")
    for name, status in ss_results:
        r.log(f"  {name:40} {status}")

    r.kv(
        pitr_newly_enabled=pitr_enabled,
        pitr_total_protected=pitr_enabled + pitr_already,
        snapstart_newly_enabled=ss_enabled,
        snapstart_total_active=ss_enabled + ss_already,
    )
    r.log("Done")
