"""ops 1094 — EventBridge rule audit + cleanup candidate report.

Goal: reduce the 357-rule inventory below the 300 limit so new schedules
can be created. Never auto-deletes — outputs a categorized review report.

For each rule:
  1. Get rule metadata (name, state, schedule expression)
  2. Get its targets (the Lambdas it invokes)
  3. Check if each target Lambda still exists
  4. Pull 30-day CloudWatch Invocations metric for last-invocation timestamp
  5. Categorize:
       DEAD            — target Lambda deleted (highest delete priority)
       DISABLED        — rule state = DISABLED (no invocations, still counts toward limit)
       STALE_30D       — no invocations in last 30 days (likely deprecated)
       STALE_7D        — no invocations in last 7 days (review carefully)
       HIGH_FREQ       — runs every 1-5 minutes (candidate for cadence relaxation)
       HEALTHY         — regular invocations in last 24h

Output:
  - aws/ops/reports/1094.json (full inventory)
  - s3://justhodl-dashboard-live/data/eventbridge-audit.json (for dashboard reading)
  - Summary printed to log
"""
import os
import json
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
OUT_KEY = "data/eventbridge-audit.json"

events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def list_all_rules():
    """Paginated list_rules — gets every rule in default event bus."""
    rules = []
    token = None
    while True:
        kwargs = {"Limit": 100}
        if token:
            kwargs["NextToken"] = token
        r = events.list_rules(**kwargs)
        rules.extend(r.get("Rules", []))
        token = r.get("NextToken")
        if not token:
            break
    return rules


def list_all_lambda_names():
    """Cache all existing Lambda function names for fast existence checks."""
    names = set()
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            names.add(f.get("FunctionName"))
    return names


def get_rule_targets(rule_name):
    try:
        r = events.list_targets_by_rule(Rule=rule_name)
        return r.get("Targets", [])
    except Exception as e:
        return [{"_err": str(e)[:100]}]


def get_last_invocation(lambda_name, days_back=30):
    """Query CloudWatch for the most recent non-zero Invocations datapoint."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
            StartTime=start,
            EndTime=end,
            Period=86400,  # daily buckets
            Statistics=["Sum"],
        )
        pts = sorted(
            [p for p in r.get("Datapoints", []) if p.get("Sum", 0) > 0],
            key=lambda p: p["Timestamp"],
            reverse=True,
        )
        if pts:
            return {
                "last_invocation_date": pts[0]["Timestamp"].date().isoformat(),
                "days_since": (end.date() - pts[0]["Timestamp"].date()).days,
                "invocations_30d": int(sum(p.get("Sum", 0) for p in r.get("Datapoints", []))),
                "active_days_30d": len(pts),
            }
        return {
            "last_invocation_date": None,
            "days_since": days_back + 1,
            "invocations_30d": 0,
            "active_days_30d": 0,
        }
    except Exception as e:
        return {"err": str(e)[:120]}


def categorize_rule(rule, targets, lambda_exists, last_inv):
    """Classification logic per the audit doctrine."""
    # State check
    state = rule.get("State", "ENABLED")
    schedule = rule.get("ScheduleExpression", "")

    # Get the Lambda target name (most common: 1 target)
    lambda_targets = []
    for t in targets:
        if t.get("_err"):
            continue
        arn = t.get("Arn", "")
        if ":function:" in arn:
            lambda_targets.append(arn.split(":function:")[-1].split(":")[0])

    # DEAD: target Lambda doesn't exist
    if lambda_targets and not all(lambda_exists.get(n, False) for n in lambda_targets):
        missing = [n for n in lambda_targets if not lambda_exists.get(n, False)]
        return ("DEAD", f"Target Lambda(s) deleted: {', '.join(missing)}")

    # DISABLED rules still count toward limit
    if state == "DISABLED":
        return ("DISABLED", "Rule is disabled — delete to reclaim slot")

    # Use last invocation data
    if isinstance(last_inv, dict) and "days_since" in last_inv:
        days = last_inv["days_since"]
        if days > 30:
            return ("STALE_30D", f"No invocations in {days}+ days")
        elif days > 7:
            return ("STALE_7D", f"No invocations in last 7 days ({days} since last)")

    # High-frequency check
    if "rate(1 minute" in schedule or "rate(2 minute" in schedule or "rate(5 minute" in schedule:
        if last_inv.get("invocations_30d", 0) > 1000:
            return ("HIGH_FREQ", f"Runs {schedule} — consider cadence relaxation (memory: ops 748-749 saved ~27k inv/mo by doing this)")

    return ("HEALTHY", "Regular invocations within last 7 days")


def main():
    print("=" * 70)
    print("ops 1094 — EventBridge rule audit")
    print("=" * 70)
    started = time.time()

    # 1. Inventory
    print("Listing EventBridge rules...")
    rules = list_all_rules()
    print(f"  Found {len(rules)} rules")

    print("Listing Lambda functions...")
    lambda_names = list_all_lambda_names()
    print(f"  Found {len(lambda_names)} Lambdas")
    lambda_exists = {n: True for n in lambda_names}

    # 2. For each rule, get targets + classify (parallelized)
    print("Fetching targets + CloudWatch metrics in parallel...")

    def process_rule(rule):
        rule_name = rule["Name"]
        targets = get_rule_targets(rule_name)
        # Get last invocation for first Lambda target (if any)
        last_inv = {}
        for t in targets:
            if t.get("_err"):
                continue
            arn = t.get("Arn", "")
            if ":function:" in arn:
                fn_name = arn.split(":function:")[-1].split(":")[0]
                last_inv = get_last_invocation(fn_name)
                break
        category, reason = categorize_rule(rule, targets, lambda_exists, last_inv)
        return {
            "name": rule_name,
            "state": rule.get("State"),
            "schedule": rule.get("ScheduleExpression"),
            "event_pattern": bool(rule.get("EventPattern")),
            "description": (rule.get("Description") or "")[:120],
            "targets": [t.get("Arn", "?").split(":function:")[-1].split(":")[0]
                        if ":function:" in t.get("Arn", "") else t.get("Arn")
                        for t in targets if not t.get("_err")],
            "last_invocation": last_inv,
            "category": category,
            "reason": reason,
        }

    results = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(process_rule, r): r for r in rules}
        for i, f in enumerate(as_completed(futures)):
            results.append(f.result())
            if (i + 1) % 50 == 0:
                print(f"  Processed {i + 1}/{len(rules)}")

    # 3. Categorize
    by_cat = {}
    for r in results:
        by_cat.setdefault(r["category"], []).append(r)

    # 4. Summary
    summary = {
        "total_rules": len(results),
        "by_category": {cat: len(items) for cat, items in by_cat.items()},
        "by_state": {
            "ENABLED": sum(1 for r in results if r["state"] == "ENABLED"),
            "DISABLED": sum(1 for r in results if r["state"] == "DISABLED"),
        },
        "deletion_candidates": {
            "DEAD": len(by_cat.get("DEAD", [])),
            "DISABLED": len(by_cat.get("DISABLED", [])),
            "STALE_30D": len(by_cat.get("STALE_30D", [])),
        },
        "review_candidates": {
            "STALE_7D": len(by_cat.get("STALE_7D", [])),
            "HIGH_FREQ": len(by_cat.get("HIGH_FREQ", [])),
        },
        "healthy": len(by_cat.get("HEALTHY", [])),
    }
    summary["safe_delete_total"] = (
        summary["deletion_candidates"]["DEAD"]
        + summary["deletion_candidates"]["DISABLED"]
        + summary["deletion_candidates"]["STALE_30D"]
    )
    summary["estimated_slots_reclaimable"] = summary["safe_delete_total"]
    summary["headroom_after_cleanup"] = 300 - (summary["total_rules"] - summary["safe_delete_total"])

    print("\n" + "=" * 70)
    print("AUDIT SUMMARY")
    print("=" * 70)
    print(json.dumps(summary, indent=2))

    # 5. Top deletion candidates (top 25 by impact)
    candidates = (
        [{**r, "delete_priority": 1} for r in by_cat.get("DEAD", [])]
        + [{**r, "delete_priority": 2} for r in by_cat.get("DISABLED", [])]
        + [{**r, "delete_priority": 3} for r in by_cat.get("STALE_30D", [])]
    )

    print(f"\nTOP 20 DELETION CANDIDATES (of {len(candidates)} total):")
    for c in candidates[:20]:
        print(f"  [{c['category']:10s}] {c['name'][:50]:50s} | {c['reason'][:60]}")

    # 6. Build report
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "summary": summary,
        "deletion_candidates_top_50": candidates[:50],
        "all_rules_by_category": by_cat,
    }

    # 7. Save to S3 + repo
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=BUCKET,
        Key=OUT_KEY,
        Body=json.dumps(report, default=str, indent=2).encode(),
        ContentType="application/json",
    )

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1094.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nDone in {report['elapsed_seconds']}s")
    print(f"Report: {out}")
    print(f"S3:     s3://{BUCKET}/{OUT_KEY}")
    print(f"\n⚠ NEVER auto-deletes. Review report before any actual deletes.")


if __name__ == "__main__":
    main()
