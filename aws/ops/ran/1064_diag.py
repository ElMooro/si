"""
Ops 1064: read-only diagnostics for two anomalies.

1. justhodl-short-interest (3/10 producer): 1054 saw "3 error(s) in window"
   and its artifact data/short-interest-tickers.json is missing. Pull the
   most recent ERROR log events from CloudWatch Logs.
2. justhodl-fedwatch-rate-probability: its legacy Scheduler schedule
   (justhodl-fedwatch-rate-probability-daily) is DISABLED, yet the lambda
   had 16 invocations in 7d. List every trigger (classic rules + Scheduler)
   targeting it so we can decide whether enabling the schedule is safe.

Writes: aws/ops/reports/1064_diag.json (committed back by run-ops).
Read-only: no schedule/lambda/log changes. Never raises.
"""
from __future__ import annotations

import json
import os
import time

import boto3
from botocore.exceptions import ClientError

OUT_PATH = os.path.join("aws", "ops", "reports", "1064_diag.json")
ACCOUNT = "857687956942"
REGION = "us-east-1"


def short_interest_errors(logs):
    """Fetch recent ERROR log events for the short-interest producer.

    Never raises.
    """
    group = "/aws/lambda/justhodl-short-interest"
    try:
        resp = logs.filter_log_events(
            logGroupName=group, filterPattern="ERROR",
            limit=20)
        events = []
        for e in resp.get("events", []):
            msg = e.get("message", "")
            events.append({
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                           time.gmtime(e.get("timestamp", 0) / 1000)),
                "message": msg[:600],
            })
        return {"log_group": group, "error_events": events,
                "count": len(events)}
    except ClientError as e:
        return {"log_group": group,
                "error": e.response.get("Error", {}).get("Code", type(e).__name__)}
    except Exception as e:  # defensive
        return {"log_group": group, "error": type(e).__name__}


def fedwatch_triggers(events, scheduler):
    """List all triggers targeting the fedwatch lambda. Never raises."""
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-fedwatch-rate-probability"
    triggers = []
    try:
        for r in events.list_rules(NamePrefix="justhodl-").get("Rules", []):
            try:
                tgts = events.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
            except ClientError:
                continue
            if any(t.get("Arn") == arn for t in tgts):
                triggers.append({"type": "classic_rule", "name": r["Name"],
                                 "state": r.get("State"),
                                 "expression": r.get("ScheduleExpression")})
    except ClientError as e:
        triggers.append({"type": "classic_rule_scan",
                         "error": e.response.get("Error", {}).get("Code")})
    try:
        token = None
        while True:
            kw = {"MaxResults": 100}
            if token:
                kw["NextToken"] = token
            page = scheduler.list_schedules(**kw)
            for s in page.get("Schedules", []):
                try:
                    desc = scheduler.get_schedule(Name=s["Name"])
                except ClientError:
                    continue
                if (desc.get("Target") or {}).get("Arn") == arn:
                    triggers.append({"type": "scheduler", "name": s["Name"],
                                     "state": desc.get("State"),
                                     "expression": desc.get("ScheduleExpression"),
                                     "created": str(desc.get("CreationDate")),
                                     "last_modified": str(desc.get("LastModificationDate"))})
            token = page.get("NextToken")
            if not token:
                break
    except ClientError as e:
        triggers.append({"type": "scheduler_scan",
                         "error": e.response.get("Error", {}).get("Code")})
    return triggers


def main():
    """Gather the diagnostics, write the report, print it."""
    logs = boto3.client("logs", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    scheduler = boto3.client("scheduler", region_name=REGION)
    report = {
        "script": "1064_diag",
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "short_interest_errors": short_interest_errors(logs),
        "fedwatch_triggers": fedwatch_triggers(events, scheduler),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2)[:12000])


if __name__ == "__main__":
    main()
