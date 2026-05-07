#!/usr/bin/env python3
"""Step 298 — Comprehensive health audit of the stock-selection alpha stack.

For each Lambda in the audit set:
  - Does an EventBridge rule schedule it? what cron?
  - When was it last invoked successfully? errors in 7d?
  - Does its S3 output exist? when was it last modified?
  - Output size

Then for each S3 data file produced:
  - Which frontend HTML pages reference it?

Output: aws/ops/reports/298_alpha_stack_health.json + a printable
"health card" per Lambda showing healthy/degraded/orphaned status.

The goal: identify what's truly broken or unused, vs what's working,
so we know where to actually invest. No new Lambdas; just diagnosis.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta
import subprocess

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/298_alpha_stack_health.json"

# Lambdas in the stock-selection alpha stack
LAMBDAS = [
    # Theme & asymmetric setup
    "justhodl-theme-detector",
    "justhodl-theme-tier-classifier",
    "justhodl-theme-rotation-engine",
    "justhodl-asymmetric-hunter",
    "justhodl-asymmetric-scorer",
    "justhodl-volatility-squeeze-hunter",

    # Earnings & fundamentals
    "justhodl-eps-revision-velocity",
    "justhodl-earnings-pead",
    "justhodl-earnings-tracker",
    "justhodl-sector-earnings-diffusion",
    "justhodl-deep-value-screener",
    "justhodl-valuations-agent",
    "fmp-fundamentals-agent",

    # Smart money & filings
    "justhodl-13f-positions",
    "justhodl-sec-13f",
    "justhodl-insider-cluster-scanner",
    "justhodl-insider-trades",
    "justhodl-activist-filings-scanner",
    "justhodl-sec-8k",
    "justhodl-sec-10kq",

    # Sentiment
    "justhodl-news-sentiment",
    "justhodl-aaii-sentiment",
    "justhodl-gdelt-sentiment",

    # Cross-asset / divergence
    "justhodl-cross-asset-regime",
    "justhodl-divergence-scanner",
    "justhodl-sector-rotation",

    # Composite / aggregator
    "justhodl-compound-aggregator",
    "justhodl-stock-screener",

    # Backtest & calibration
    "justhodl-backtest-engine",
    "justhodl-calibrator",
]

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def get_eb_rules_for_target(fn_name):
    """Find EventBridge rules targeting this Lambda."""
    try:
        rules = events.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn_name}"
        ).get("RuleNames", [])
        out = []
        for rname in rules:
            try:
                r = events.describe_rule(Name=rname)
                out.append({
                    "name": rname,
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                })
            except Exception:
                out.append({"name": rname, "err": "describe_failed"})
        return out
    except Exception as e:
        return [{"err": str(e)[:120]}]


def get_recent_metrics(fn_name, days=7):
    """Sum of Invocations + Errors in last N days."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    out = {}
    for metric in ("Invocations", "Errors", "Throttles"):
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=86400 * days,
                Statistics=["Sum"],
            )
            pts = resp.get("Datapoints", [])
            out[metric.lower()] = int(pts[0]["Sum"]) if pts else 0
        except Exception:
            out[metric.lower()] = None
    return out


def find_lambda_outputs(fn_name):
    """Look at the Lambda's source for s3 put_object Key references."""
    src = f"aws/lambdas/{fn_name}/source"
    if not os.path.isdir(src):
        return []
    keys = set()
    try:
        result = subprocess.run(
            ["grep", "-rohE", r"['\"](data/[a-z0-9_/-]+\.json)['\"]", src],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.split("\n"):
            line = line.strip().strip("'\"")
            if line.startswith("data/") and line.endswith(".json"):
                keys.add(line)
    except Exception:
        pass
    return sorted(keys)


def s3_object_freshness(key):
    """Last-modified time of an S3 object in the dashboard bucket."""
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        lm = h["LastModified"]
        size = h["ContentLength"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        return {
            "exists": True,
            "last_modified": lm.isoformat(),
            "age_hours": round(age_h, 1),
            "size_kb": round(size / 1024, 1),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {"exists": False}
        return {"exists": "err", "err": str(e)[:100]}


def find_frontend_consumers(s3_key):
    """Grep HTML files for references to this S3 key."""
    try:
        result = subprocess.run(
            ["grep", "-rln", "--include=*.html", s3_key, "."],
            capture_output=True, text=True, timeout=5,
        )
        return [
            line.strip().lstrip("./")
            for line in result.stdout.split("\n")
            if line.strip() and "/historical/" not in line and "/archive/" not in line
        ][:5]
    except Exception:
        return []


def assess_health(metrics, eb_rules, outputs_freshness):
    """Categorize Lambda health based on signals."""
    inv = metrics.get("invocations") or 0
    err = metrics.get("errors") or 0

    has_schedule = any(r.get("state") == "ENABLED" for r in eb_rules
                       if isinstance(r, dict))

    fresh_outputs = [o for o in outputs_freshness
                      if o.get("exists") is True and o.get("age_hours", 999) < 48]

    stale_outputs = [o for o in outputs_freshness
                     if o.get("exists") is True and o.get("age_hours", 0) >= 48]

    if not has_schedule and inv == 0:
        return "DORMANT"   # not running at all
    if has_schedule and inv == 0:
        return "SCHEDULE_BROKEN"   # rule exists but not firing
    if err > 0 and inv > 0 and err / inv > 0.5:
        return "FAILING"   # most invocations error
    if fresh_outputs:
        return "HEALTHY"
    if stale_outputs and not fresh_outputs:
        return "STALE_OUTPUT"   # produces stale data
    if inv > 0:
        return "RUNNING_NO_OUTPUT"   # invokes but doesn't update S3?
    return "UNKNOWN"


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    for fn in LAMBDAS:
        print(f"  auditing {fn}…")
        info = {}
        try:
            lam.get_function(FunctionName=fn)
            info["exists"] = True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                info["exists"] = False
                info["health"] = "MISSING"
                out["lambdas"][fn] = info
                continue
            raise

        info["eb_rules"] = get_eb_rules_for_target(fn)
        info["metrics_7d"] = get_recent_metrics(fn, 7)
        s3_keys = find_lambda_outputs(fn)
        info["s3_keys_referenced"] = s3_keys
        info["s3_freshness"] = {k: s3_object_freshness(k) for k in s3_keys[:6]}
        info["frontend_consumers"] = {
            k: find_frontend_consumers(k) for k in s3_keys[:6]
        }
        info["health"] = assess_health(
            info["metrics_7d"], info["eb_rules"],
            list(info["s3_freshness"].values()),
        )
        out["lambdas"][fn] = info

    # Aggregate by status
    status_counts = {}
    for fn, info in out["lambdas"].items():
        status_counts[info.get("health", "?")] = status_counts.get(info.get("health", "?"), 0) + 1
    out["summary"] = {
        "total_audited": len(LAMBDAS),
        "by_status": status_counts,
    }

    # Find orphaned outputs (data exists, no frontend consumer)
    orphan_outputs = []
    for fn, info in out["lambdas"].items():
        for key, consumers in (info.get("frontend_consumers") or {}).items():
            fresh = info.get("s3_freshness", {}).get(key, {})
            if fresh.get("exists") is True and not consumers:
                orphan_outputs.append({
                    "lambda": fn,
                    "s3_key": key,
                    "age_hours": fresh.get("age_hours"),
                })
    out["orphaned_outputs"] = orphan_outputs

    out["duration_s"] = round(time.time() - started, 1)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
