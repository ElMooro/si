#!/usr/bin/env python3
"""
Comprehensive audit of prediction-generation + outcome-scoring +
calibration loop.

Goal: answer Khalid's actual question — every data series we pull,
is it being used for predictions? Are predictions scored at multiple
horizons (1d/1w/1m)? Does the system improve from results?

Concretely audit:

  A. Producer→Consumer chains
     For each S3 data file (report.json, crypto-intel.json, flow-data.json,
     screener/data.json, valuations-data.json, intelligence-report.json,
     edge-data.json, predictions.json, etc.):
       - Which Lambda WRITES it?
       - Which Lambda(s) READ it?
       - If no consumer reads it → DEAD DATA

  B. Prediction-emitting Lambdas
     For each Lambda, is there a "prediction" or "signal" emission with
     direction + magnitude + horizon?

  C. Signal-logger coverage
     Compare what's in S3 (potential signals) vs what's actually logged
     to DynamoDB justhodl-signals.

  D. Outcome scoring horizons
     Read outcome-checker source — does it score at 1d, 1w, 1m? Is the
     schedule weekly enough or does it need daily runs?

  E. Calibrator presence
     Lambda exists? Source in repo? Schedule firing? Weights getting
     updated?

  F. Coverage gaps
     Signals logged but never scored → orphan logs
     Sources fetched but never logged → unused intel
"""
import json
import re
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

# Files we expect Lambdas to write to S3 (for analysis pipeline)
EXPECTED_S3_FILES = [
    "data/report.json",
    "data/secretary-latest.json",
    "data/intelligence-report.json",
    "crypto-intel.json",
    "flow-data.json",
    "edge-data.json",
    "predictions.json",
    "valuations-data.json",
    "data.json",            # legacy orphan
    "report.json",          # legacy orphan
    "screener/data.json",
    "screener/picks.json",
    "ml/predictions.json",
    "intelligence-report.json",
    "ath-data.json",
    "repo-data.json",
    "fund_flows.json",
    "stock-picks-data.json",
    "fed-liquidity.json",
]


def find_writers(s3_key):
    """Find which Lambda writes to this S3 key."""
    writers = []
    lambda_dir = REPO_ROOT / "aws/lambdas"
    for fn_dir in lambda_dir.iterdir():
        if not fn_dir.is_dir(): continue
        src = fn_dir / "source/lambda_function.py"
        if not src.exists(): continue
        try:
            content = src.read_text(encoding="utf-8", errors="ignore")
            # Look for s3.put_object with this key, or "Key=...key..."
            patterns = [
                f'Key="{s3_key}"',
                f"Key='{s3_key}'",
                f'Key={s3_key!r}',
                f'"{s3_key}"',
                f"'{s3_key}'",
            ]
            for pat in patterns:
                if pat in content and ("put_object" in content or ".upload" in content):
                    writers.append(fn_dir.name)
                    break
        except Exception:
            pass
    return writers


def find_readers(s3_key):
    """Find which Lambda reads this S3 key."""
    readers = []
    lambda_dir = REPO_ROOT / "aws/lambdas"
    for fn_dir in lambda_dir.iterdir():
        if not fn_dir.is_dir(): continue
        src = fn_dir / "source/lambda_function.py"
        if not src.exists(): continue
        try:
            content = src.read_text(encoding="utf-8", errors="ignore")
            patterns = [
                f'Key="{s3_key}"',
                f"Key='{s3_key}'",
                f'"{s3_key}"',
                f"'{s3_key}'",
            ]
            for pat in patterns:
                if pat in content and ("get_object" in content or "fetch" in content.lower() or "read" in content.lower()):
                    readers.append(fn_dir.name)
                    break
        except Exception:
            pass
    return readers


def s3_exists(key):
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        return True, obj["ContentLength"], age_min
    except Exception:
        return False, 0, 0


with report("prediction_loop_audit") as r:
    r.heading("Prediction Loop Audit — every data point → prediction → outcome")

    # ─── PART A: Producer-Consumer chains ───
    r.section("A. Producer → Consumer chains for each S3 data file")
    dead_files = []
    healthy_files = []
    for key in EXPECTED_S3_FILES:
        exists, size, age_min = s3_exists(key)
        writers = find_writers(key)
        readers = find_readers(key)

        if not exists:
            r.log(f"  ❌ {key:35} (not on S3)")
            continue

        status = "✓"
        flags = []
        if not writers:
            flags.append("no writer found in code")
        if not readers and key not in ("data/report.json",):
            flags.append("no consumer Lambda")
        if not readers and not writers:
            status = "💀 DEAD DATA"
            dead_files.append(key)
        elif not readers:
            status = "⚠ ORPHAN"
            dead_files.append(key)
        else:
            healthy_files.append(key)

        r.log(f"  {status} {key:35} {age_min:6.1f}m old, {size//1024:5}KB")
        if writers:
            r.log(f"     ← writer: {', '.join(writers[:3])}")
        if readers:
            r.log(f"     → readers: {', '.join(readers[:5])}")
        if flags:
            r.log(f"     flags: {', '.join(flags)}")

    r.kv(healthy_chains=len(healthy_files), dead_or_orphan=len(dead_files),
         total_files=len(EXPECTED_S3_FILES))

    # ─── PART B: Lambdas in AWS that aren't in version control ───
    r.section("B. Calibrator + ml-predictions: AWS-only Lambdas (not in repo)")
    aws_fns = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            aws_fns.append(fn["FunctionName"])
    r.log(f"  Total Lambdas in AWS: {len(aws_fns)}")

    # Specifically look for the prediction-loop Lambdas
    versioned = set()
    lambda_dir = REPO_ROOT / "aws/lambdas"
    for fn_dir in lambda_dir.iterdir():
        if fn_dir.is_dir():
            versioned.add(fn_dir.name)

    keywords = ["calibrator", "predict", "outcome", "signal", "screener",
                "valuation", "edge", "morning", "intel", "khalid", "logger", "ml"]
    relevant_aws = set()
    for fn in aws_fns:
        for kw in keywords:
            if kw in fn.lower():
                relevant_aws.add(fn)
                break

    not_in_repo = relevant_aws - versioned
    r.log(f"\n  Prediction-loop Lambdas in AWS but NOT in repo:")
    for fn in sorted(not_in_repo):
        r.log(f"    🔴 {fn}")
    if not not_in_repo:
        r.log("    (all version-controlled)")

    in_repo_relevant = relevant_aws & versioned
    r.log(f"\n  Prediction-loop Lambdas in AWS AND in repo:")
    for fn in sorted(in_repo_relevant):
        r.log(f"    ✓ {fn}")

    r.kv(aws_total=len(aws_fns),
         relevant_in_repo=len(in_repo_relevant),
         relevant_orphan=len(not_in_repo))

    # ─── PART C: Signal logger coverage ───
    r.section("C. Signal logger — what signals are being recorded?")
    try:
        # Distinct signal types in DynamoDB justhodl-signals
        scan = ddb.scan(TableName="justhodl-signals", Limit=200,
                        ProjectionExpression="signal_type")
        types = {}
        for item in scan.get("Items", []):
            t = item.get("signal_type", {}).get("S", "")
            types[t] = types.get(t, 0) + 1
        r.log(f"  Distinct signal types in last 200 entries:")
        for t, count in sorted(types.items(), key=lambda x: -x[1]):
            r.log(f"    {t:30} {count:4}x")
        td = ddb.describe_table(TableName="justhodl-signals")
        r.log(f"\n  Total signals logged: {td['Table'].get('ItemCount', 'unknown')}")
    except Exception as e:
        r.warn(f"  {e}")

    # Also: read signal-logger source to see what it INTENDS to log
    r.log("\n  Signal-logger source — what does it scan for?")
    try:
        sl = (REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py").read_text()
        # Extract every put_item or signal_type assignment
        signal_types_in_code = set()
        for m in re.finditer(r"signal_type['\"]?\s*:\s*['\"]([\w_]+)['\"]", sl):
            signal_types_in_code.add(m.group(1))
        for m in re.finditer(r"['\"](\w+_signal|\w+_score|\w+_regime|\w+_index)['\"]", sl):
            signal_types_in_code.add(m.group(1))
        r.log(f"    Code references {len(signal_types_in_code)} signal type names")
        for st in sorted(signal_types_in_code)[:20]:
            r.log(f"      {st}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── PART D: Outcome scoring horizons ───
    r.section("D. Outcome scoring — what time horizons are compared?")
    try:
        oc = (REPO_ROOT / "aws/lambdas/justhodl-outcome-checker/source/lambda_function.py").read_text()
        # Look for horizon references
        for keyword in ["1d", "7d", "30d", "1 day", "7 day", "30 day", "weekly",
                        "daily", "monthly", "horizon", "lookback", "days_ago"]:
            count = oc.lower().count(keyword.lower())
            if count > 0:
                r.log(f"    '{keyword}': {count} mentions")
        # Schedule
        r.log("\n  EventBridge schedule for outcome-checker:")
        rules = eb.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:us-east-1:857687956942:function:justhodl-outcome-checker"
        ).get("RuleNames", [])
        for rule_name in rules:
            rule = eb.describe_rule(Name=rule_name)
            r.log(f"    [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── PART E: Calibrator status ───
    r.section("E. Calibrator — does it actually run?")
    try:
        # Find calibrator in AWS
        calib_fn = None
        for fn in aws_fns:
            if "calibrat" in fn.lower():
                calib_fn = fn
                break
        if calib_fn:
            r.log(f"  ✓ Found in AWS: {calib_fn}")
            cfg = lam.get_function_configuration(FunctionName=calib_fn)
            r.log(f"    LastModified: {cfg['LastModified']}")
            # Source in repo?
            src = REPO_ROOT / f"aws/lambdas/{calib_fn}/source/lambda_function.py"
            if src.exists():
                r.log(f"    ✓ Source in repo ({src.stat().st_size} bytes)")
            else:
                r.warn(f"    ⚠ Source NOT in repo — version-control it")
            # Recent invocations
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=7)
            cw = boto3.client("cloudwatch", region_name="us-east-1")
            inv_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": calib_fn}],
                StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
            )
            total = sum(p.get("Sum", 0) for p in inv_resp.get("Datapoints", []))
            r.log(f"    Invocations last 7d: {int(total)}")
            # EB rules
            try:
                rules = eb.list_rule_names_by_target(
                    TargetArn=f"arn:aws:lambda:us-east-1:857687956942:function:{calib_fn}"
                ).get("RuleNames", [])
                for rule_name in rules:
                    rule = eb.describe_rule(Name=rule_name)
                    r.log(f"    EB: [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")
            except Exception as e:
                r.warn(f"    EB lookup failed: {e}")
        else:
            r.fail("  ❌ No calibrator Lambda found in AWS — calibration loop is BROKEN")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── PART F: Calibration weights vs signals ratio ───
    r.section("F. Coverage gap — signals logged vs signals weighted")
    try:
        # Active signal types from logger
        scan = ddb.scan(TableName="justhodl-signals", Limit=500,
                        ProjectionExpression="signal_type")
        all_logged_types = set()
        for item in scan.get("Items", []):
            t = item.get("signal_type", {}).get("S", "")
            if t: all_logged_types.add(t)
        # Weights
        weights = json.loads(ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]["Value"])
        weighted_types = set(weights.keys())

        r.log(f"  Signal types in logger:    {len(all_logged_types)}")
        r.log(f"  Signal types weighted:     {len(weighted_types)}")
        r.log(f"\n  Logged but UNWEIGHTED (system isn't learning from these):")
        unweighted = all_logged_types - weighted_types
        for t in sorted(unweighted):
            r.log(f"    🔴 {t}")
        if not unweighted:
            r.log("    (none — full coverage)")
        r.log(f"\n  Weighted but NOT logged (weights stale/orphan):")
        unlogged = weighted_types - all_logged_types
        for t in sorted(unlogged):
            r.log(f"    ⚠ {t}")
        if not unlogged:
            r.log("    (none — clean)")

        r.kv(logged_signals=len(all_logged_types),
             weighted_signals=len(weighted_types),
             logged_unweighted=len(unweighted),
             weighted_unlogged=len(unlogged))
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
