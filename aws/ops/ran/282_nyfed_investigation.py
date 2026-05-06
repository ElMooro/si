#!/usr/bin/env python3
"""Step 282 — Investigate the 3 nyfed Lambdas saved by step 281's safeguard.

The original step 261 audit said these were orphans (zero invocations
24h, no EB rule, no Function URL). My step 281 safeguard caught that
they DO have invocations in the past 7 days. Need to figure out:

  1. How many invocations exactly, and on what schedule?
  2. WHO is invoking them? (EB rule? cross-account? manual?)
  3. WHAT do they do? (read source code metadata)
  4. Should they be preserved or deleted?

Output: full picture so Khalid can make the call.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPORT_PATH = "aws/ops/reports/282_nyfed_investigation.json"

LAMBDAS = [
    "nyfed-primary-dealer-fetcher",
    "nyfed-financial-stability-fetcher",
    "nyfedapi-isolated",
]

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def daily_invocations_30d(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Sum"],
        )
        days = []
        for d in sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"]):
            if d["Sum"] > 0:
                days.append({
                    "date": d["Timestamp"].strftime("%Y-%m-%d"),
                    "invocations": int(d["Sum"]),
                })
        return days
    except Exception as e:
        return [{"err": str(e)[:200]}]


def all_eb_rules_with_target(lambda_name):
    """Cross-reference: list ALL EB rules in account + check each
    one's targets for this Lambda. (The list_rule_names_by_target
    API used by 261 audit might miss cross-region or odd-format rules.)"""
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{lambda_name}"
    matches = []

    # First try the targeted query
    try:
        r = eb.list_rule_names_by_target(TargetArn=arn)
        for n in r.get("RuleNames", []):
            matches.append({"rule_name": n, "via": "list_rule_names_by_target"})
    except Exception as e:
        matches.append({"err": f"target_query: {str(e)[:200]}"})

    # Also iterate all rules and check (slower but bulletproof)
    paginator = eb.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            try:
                tgts = eb.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    if t.get("Arn") == arn:
                        already = any(m.get("rule_name") == rule["Name"] for m in matches)
                        if not already:
                            matches.append({
                                "rule_name": rule["Name"],
                                "schedule": rule.get("ScheduleExpression"),
                                "state": rule.get("State"),
                                "via": "full_scan",
                            })
                        else:
                            # Augment existing match
                            for m in matches:
                                if m.get("rule_name") == rule["Name"]:
                                    m["schedule"] = rule.get("ScheduleExpression")
                                    m["state"] = rule.get("State")
                                    break
            except Exception:
                pass
    return matches


def get_lambda_meta(name):
    try:
        cfg = lam.get_function(FunctionName=name)
        return {
            "arn": cfg["Configuration"]["FunctionArn"],
            "runtime": cfg["Configuration"].get("Runtime"),
            "handler": cfg["Configuration"].get("Handler"),
            "memory_mb": cfg["Configuration"].get("MemorySize"),
            "timeout_s": cfg["Configuration"].get("Timeout"),
            "code_size": cfg["Configuration"].get("CodeSize"),
            "last_modified": cfg["Configuration"].get("LastModified"),
            "description": cfg["Configuration"].get("Description"),
            "env_vars": list((cfg["Configuration"].get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def get_recent_log_messages(name, max_events=5):
    """Read a few recent log events to see what the Lambda actually logs
    when it runs — gives a hint about its purpose."""
    log_group = f"/aws/lambda/{name}"
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=2,
        ).get("logStreams", [])
        events = []
        for s in streams:
            ev = logs.get_log_events(
                logGroupName=log_group, logStreamName=s["logStreamName"],
                limit=max_events, startFromHead=False,
            ).get("events", [])
            for e in ev[:max_events]:
                events.append({
                    "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat(),
                    "msg": e["message"][:300].strip(),
                })
        return events[:max_events]
    except Exception as e:
        return [{"err": str(e)[:200]}]


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "results": []}
    for name in LAMBDAS:
        print(f"[282] investigating {name}…")
        result = {"name": name}
        result["meta"] = get_lambda_meta(name)
        result["invocations_30d"] = daily_invocations_30d(name)
        result["total_invocations_30d"] = sum(
            d.get("invocations", 0) for d in result["invocations_30d"]
        )
        result["eb_rules"] = all_eb_rules_with_target(name)
        result["recent_log_messages"] = get_recent_log_messages(name)
        out["results"].append(result)

    out["duration_s"] = round(time.time() - started, 1)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
