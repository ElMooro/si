"""
Ops 1061: read-only deep dive for the R-engine schedule decision.

1. Dump the raw `upgrades` list from the 1054 S3 report (1058 hit a
   type-mismatch branch and did not print it).
2. Describe the 8 legacy R-engine Scheduler schedules found by 1058:
   expression, state, target ARN, creation date.
3. CloudWatch Metrics: invocations + errors per R-engine lambda, last 7d,
   to show whether the legacy schedules are actually firing.

Writes: aws/ops/reports/1061_schedule_dive.json (committed back by run-ops).
Never raises. Read-only: no schedule/lambda changes.
"""
from __future__ import annotations

import datetime
import json
import os
import time

import boto3

OUT_PATH = os.path.join("aws", "ops", "reports", "1061_schedule_dive.json")
S3_BUCKET = "justhodl-dashboard-live"
CAP = 800

ENGINES = [
    "justhodl-factor-decomposition", "justhodl-fx-decomposition",
    "justhodl-fedwatch-rate-probability", "justhodl-cftc-deep-view",
    "justhodl-sec-filing-diff", "justhodl-transcript-query",
    "justhodl-peer-comparison", "justhodl-screen-builder",
    "justhodl-supply-chain-linkage",
]
LEGACY_SCHEDULES = [
    "justhodl-peer-comparison-daily",
    "justhodl-screen-builder-universe-daily",
    "justhodl-fedwatch-rate-probability-daily",
    "justhodl-sec-filing-diff-daily",
    "justhodl-supply-chain-linkage-daily",
    "justhodl-factor-decomposition-weekly",
    "justhodl-fx-decomposition-daily",
    "justhodl-cftc-deep-view-fri",
]


def cap_str(v):
    """Truncate a long string. Never raises."""
    try:
        return v[:CAP] + ("..." if len(v) > CAP else "") if isinstance(v, str) else v
    except Exception:
        return "<unserializable>"


def read_upgrades(s3):
    """Dump the 1054 upgrades list. Never raises."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="ops/reports/1054.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        upgrades = data.get("upgrades")
        if not isinstance(upgrades, list):
            return {"unexpected_type": type(upgrades).__name__}
        out = []
        for u in upgrades[:20]:
            if not isinstance(u, dict):
                out.append(cap_str(u))
                continue
            out.append({
                "name": u.get("name") or u.get("upgrade") or u.get("id"),
                "verdict": u.get("verdict"),
                "notes": cap_str(u.get("notes") or u.get("detail") or ""),
                "extra_keys": sorted(u.keys())[:10],
            })
        return {"count": len(upgrades), "items": out}
    except Exception as e:  # missing key / bad JSON / permissions
        return {"unreadable": type(e).__name__}


def describe_schedules(scheduler):
    """Describe each legacy schedule. Never raises per schedule."""
    out = {}
    for name in LEGACY_SCHEDULES:
        try:
            s = scheduler.get_schedule(Name=name)
            tgt = s.get("Target", {}) or {}
            out[name] = {
                "state": s.get("State"),
                "expression": s.get("ScheduleExpression"),
                "target_arn": cap_str(tgt.get("Arn")),
                "role_arn_set": bool(tgt.get("RoleArn")),
                "created": str(s.get("CreationDate")),
                "last_modified": str(s.get("LastModificationDate")),
            }
        except Exception as e:  # not found / permissions
            out[name] = {"error": type(e).__name__}
    return out


def lambda_activity(lam, cw):
    """7d invocations + errors per engine lambda. Never raises per lambda."""
    out = {}
    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - datetime.timedelta(days=7)
    for fn in ENGINES:
        item = {}
        try:
            lam.get_function(FunctionName=fn)
            item["exists"] = True
        except Exception:  # not found / permissions
            item["exists"] = False
            out[fn] = item
            continue
        for metric, key in (("Invocations", "invocations_7d"),
                            ("Errors", "errors_7d")):
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName=metric,
                    Dimensions=[{"Name": "FunctionName", "Value": fn}],
                    StartTime=start, EndTime=end, Period=604800,
                    Statistics=["Sum"])
                pts = resp.get("Datapoints", [])
                item[key] = int(sum(p.get("Sum", 0) for p in pts))
            except Exception:  # metrics unavailable
                item[key] = "unknown"
        out[fn] = item
    return out


def main():
    """Gather upgrades, schedule details, and lambda activity; write report."""
    s3 = boto3.client("s3", region_name="us-east-1")
    scheduler = boto3.client("scheduler", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1")
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    report = {
        "script": "1061_schedule_dive",
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "upgrades_1054": read_upgrades(s3),
        "legacy_schedules": describe_schedules(scheduler),
        "lambda_activity_7d": lambda_activity(lam, cw),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:16000])


if __name__ == "__main__":
    main()
