#!/usr/bin/env python3
"""
Lambda Sprawl Audit — Dry Run

Enumerates every Lambda in us-east-1, pulls last-invocation data from
CloudWatch, detects Function URLs and EventBridge rules, and produces a
markdown report classifying each function as:

  🟢 Active       — invoked within LOOKBACK_DAYS
  🟡 Review       — zero invocations but has URL or EventBridge rule
  🔴 Kill         — zero invocations, no URL, no rule → safe to delete

Report is:
  - printed to stdout
  - appended to $GITHUB_STEP_SUMMARY (renders in the Actions UI)
  - written to aws/ops/reports/lambda-audit-YYYY-MM-DD.md (committed back
    to the repo by the workflow for permanent audit trail)

NO DELETIONS. Read-only. Safe to re-run.
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION         = "us-east-1"
LOOKBACK_DAYS  = 90

lam = boto3.client("lambda", region_name=REGION)
cw  = boto3.client("cloudwatch", region_name=REGION)
ev  = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def list_all_lambdas():
    fns = []
    p = lam.get_paginator("list_functions")
    for page in p.paginate():
        fns.extend(page["Functions"])
    return fns


def last_invocation(fn_name: str):
    """Returns (timestamp_or_None, total_invocations_in_window)."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=["Sum"],
        )
    except ClientError:
        return None, 0
    dps = [dp for dp in resp.get("Datapoints", []) if dp["Sum"] > 0]
    if not dps:
        return None, 0
    dps.sort(key=lambda x: x["Timestamp"], reverse=True)
    return dps[0]["Timestamp"], int(sum(dp["Sum"] for dp in dps))


def has_function_url(fn_name: str) -> bool:
    try:
        lam.get_function_url_config(FunctionName=fn_name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False
    except ClientError:
        return False


def eventbridge_rules_for(fn_arn: str):
    try:
        return ev.list_rule_names_by_target(TargetArn=fn_arn).get("RuleNames", [])
    except ClientError:
        return []


def log_group_bytes(fn_name: str):
    try:
        grp = f"/aws/lambda/{fn_name}"
        resp = logs.describe_log_groups(logGroupNamePrefix=grp, limit=1)
        for g in resp.get("logGroups", []):
            if g["logGroupName"] == grp:
                return g.get("storedBytes", 0)
    except ClientError:
        pass
    return 0


def audit():
    functions = list_all_lambdas()
    log(f"Found {len(functions)} Lambda functions in {REGION}")

    rows = []
    for i, fn in enumerate(functions, 1):
        name = fn["FunctionName"]
        log(f"  [{i}/{len(functions)}] {name}")
        last_ts, total_inv = last_invocation(name)
        rows.append({
            "name":             name,
            "runtime":          fn.get("Runtime", "?"),
            "last_modified":    fn["LastModified"][:10],
            "code_kb":          round(fn["CodeSize"] / 1024),
            "memory_mb":        fn["MemorySize"],
            "has_url":          has_function_url(name),
            "eb_rules":         eventbridge_rules_for(fn["FunctionArn"]),
            "last_invocation":  last_ts.strftime("%Y-%m-%d") if last_ts else None,
            "invocations_90d":  total_inv,
            "log_bytes":        log_group_bytes(name),
        })

    def classify(r):
        if r["invocations_90d"] > 0:           return "active"
        if r["has_url"] or r["eb_rules"]:      return "review"
        return "kill"

    for r in rows:
        r["class"] = classify(r)

    # Sort: kill first, then review, then active (active by invocations desc)
    order = {"kill": 0, "review": 1, "active": 2}
    rows.sort(key=lambda r: (order[r["class"]], -r["invocations_90d"], r["name"]))

    return rows


def emit(rows):
    now = datetime.now(timezone.utc)
    buckets = defaultdict(list)
    for r in rows:
        buckets[r["class"]].append(r)

    total_log_bytes = sum(r["log_bytes"] for r in rows)
    kill_log_bytes  = sum(r["log_bytes"] for r in buckets["kill"])

    lines = [
        "# Lambda Sprawl Audit — Dry Run",
        "",
        f"**Region:** `{REGION}`  ",
        f"**Total functions:** {len(rows)}  ",
        f"**Lookback window:** {LOOKBACK_DAYS} days  ",
        f"**Generated:** {now.isoformat(timespec='seconds')}",
        "",
        "## Summary",
        "",
        f"| Status | Count | Total CloudWatch logs |",
        f"|--------|-------|------------------------|",
        f"| 🟢 Active (invoked in last {LOOKBACK_DAYS}d) | **{len(buckets['active'])}** | {fmt_bytes(sum(r['log_bytes'] for r in buckets['active']))} |",
        f"| 🟡 Review (no invocations but has URL or EB rule) | **{len(buckets['review'])}** | {fmt_bytes(sum(r['log_bytes'] for r in buckets['review']))} |",
        f"| 🔴 Kill candidate (safe to delete) | **{len(buckets['kill'])}** | {fmt_bytes(kill_log_bytes)} |",
        f"| **Total** | **{len(rows)}** | **{fmt_bytes(total_log_bytes)}** |",
        "",
        f"**Estimated cleanup:** deleting the {len(buckets['kill'])} kill candidates "
        f"frees {fmt_bytes(kill_log_bytes)} of CloudWatch logs (~$0.03/GB/month storage).",
        "",
        "## 🔴 Kill candidates",
        "",
        f"Zero invocations in {LOOKBACK_DAYS} days, no Function URL, no EventBridge rule. Safe to delete after spot-check.",
        "",
    ]

    if buckets["kill"]:
        lines += [
            "| Function | Runtime | Last modified | Code | Logs |",
            "|----------|---------|---------------|------|------|",
        ]
        for r in buckets["kill"]:
            lines.append(
                f"| `{r['name']}` | {r['runtime']} | {r['last_modified']} | "
                f"{r['code_kb']} KB | {fmt_bytes(r['log_bytes'])} |"
            )
    else:
        lines.append("_None found._")

    lines += [
        "",
        "## 🟡 Review candidates",
        "",
        "No invocations but has a Function URL or EventBridge rule. Could be:",
        "- Scheduled job that's silently failing (check CloudWatch logs)",
        "- Publicly reachable URL that nobody is calling (consider disabling URL then deleting)",
        "- Rarely-invoked manual fixture",
        "",
    ]

    if buckets["review"]:
        lines += [
            "| Function | Runtime | URL | EventBridge rules | Last modified | Logs |",
            "|----------|---------|-----|-------------------|---------------|------|",
        ]
        for r in buckets["review"]:
            url = "✓" if r["has_url"] else ""
            rules = ", ".join(f"`{x}`" for x in r["eb_rules"][:3])
            if len(r["eb_rules"]) > 3:
                rules += f" (+{len(r['eb_rules']) - 3})"
            lines.append(
                f"| `{r['name']}` | {r['runtime']} | {url} | {rules or '—'} | "
                f"{r['last_modified']} | {fmt_bytes(r['log_bytes'])} |"
            )
    else:
        lines.append("_None._")

    lines += [
        "",
        "## 🟢 Active functions",
        "",
        f"Invoked within the last {LOOKBACK_DAYS} days.",
        "",
        "| Function | Runtime | 90d invocations | Last | URL | EB rules |",
        "|----------|---------|-----------------|------|-----|----------|",
    ]
    for r in buckets["active"]:
        rules_count = len(r["eb_rules"])
        lines.append(
            f"| `{r['name']}` | {r['runtime']} | {r['invocations_90d']:,} | "
            f"{r['last_invocation']} | {'✓' if r['has_url'] else ''} | "
            f"{rules_count if rules_count else ''} |"
        )

    lines += [
        "",
        "## Next step",
        "",
        "Review the kill candidates. When ready, Claude will push a companion "
        "script `delete_lambda_sprawl.py` that deletes the approved list — "
        "also idempotent, also dry-runnable with `--dry-run`.",
    ]

    return "\n".join(lines)


def fmt_bytes(b):
    if b == 0: return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def main():
    log("=== Lambda Sprawl Audit (dry run) ===")
    rows = audit()
    md = emit(rows)

    # Write report file (committed back by workflow)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    repo_root = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    report_dir = os.path.join(repo_root, "aws", "ops", "reports")
    os.makedirs(report_dir, exist_ok=True)
    md_path = os.path.join(report_dir, f"lambda-audit-{ts}.md")
    json_path = os.path.join(report_dir, f"lambda-audit-{ts}.json")

    with open(md_path, "w") as f:
        f.write(md)
    with open(json_path, "w") as f:
        json.dump(rows, f, indent=2, default=str)

    log(f"Report written: {md_path}")
    log(f"Data written:   {json_path}")

    # Append to GitHub Actions step summary (renders as rich markdown in UI)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(md)
        log("Step summary updated")

    # Also dump to stdout (tail-able via workflow log API)
    print("\n" + "═" * 60)
    print(md)


if __name__ == "__main__":
    main()
