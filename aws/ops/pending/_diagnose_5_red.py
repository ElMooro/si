"""
Diagnose the 5 RED items from the system-health dashboard.

For each, determine the actual cause:
  1. s3:repo-data.json (20h ago)
  2. s3:intelligence-report.json (20h ago)
  3. lambda:justhodl-intelligence (3 inv / 24h, expected ≥4)
  4. lambda:justhodl-nyfed-dealer-survey (0 inv / 24h)
  5. lambda:justhodl-oecd-cli (0 inv / 24h)

For each Lambda we check:
  - EB rule(s) attached: enabled? schedule? last invocation?
  - CloudWatch invocations + errors over 24h, 72h, 7d
  - Last log stream timestamp (proxies last actual invocation)
  - LastModified (deploy time)

For each S3 file:
  - LastModified, content size, content snippet
  - Producer Lambda's recent activity
"""
from __future__ import annotations
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
events = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def now():
    return datetime.now(timezone.utc)


def lambda_inv_count(name: str, hours: int = 24):
    end = now()
    start = end - timedelta(hours=hours)
    inv = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": name}],
        StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
    )
    err = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": name}],
        StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
    )
    return {
        "invocations": int(sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))),
        "errors": int(sum(p.get("Sum", 0) for p in err.get("Datapoints", []))),
    }


def get_last_log_event(name: str):
    """Get the timestamp of the most recent log event in the Lambda's group."""
    log_group = f"/aws/lambda/{name}"
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=1,
        )
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]
            ts_ms = stream.get("lastEventTimestamp")
            if ts_ms:
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                return {
                    "stream": stream["logStreamName"],
                    "timestamp": ts.isoformat(),
                    "age_hours": round((now() - ts).total_seconds() / 3600, 1),
                }
    except Exception as e:
        return {"error": str(e)}
    return None


def get_recent_log_events(name: str, hours: int = 24, max_events: int = 30):
    """Pull recent log events across all streams, filter to ERROR/Exception/Traceback."""
    log_group = f"/aws/lambda/{name}"
    end = now()
    start = end - timedelta(hours=hours)
    try:
        # Use filter_log_events for cross-stream search
        out = []
        kwargs = {
            "logGroupName": log_group,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "filterPattern": '?ERROR ?Exception ?Traceback ?error ?fail',
            "limit": max_events,
        }
        events_resp = logs.filter_log_events(**kwargs)
        for e in events_resp.get("events", []):
            ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
            out.append({
                "ts": ts.isoformat(timespec="seconds"),
                "msg": e["message"][:300].strip(),
            })
        return out
    except Exception as e:
        return [{"error": str(e)}]


def get_eb_rules_for_lambda(name: str):
    """Find EB rules whose target is this Lambda."""
    fn_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{name}"
    out = []
    paginator = events.get_paginator("list_rule_names_by_target")
    try:
        for page in paginator.paginate(TargetArn=fn_arn):
            for rule_name in page.get("RuleNames", []):
                rule = events.describe_rule(Name=rule_name)
                out.append({
                    "name": rule_name,
                    "schedule": rule.get("ScheduleExpression"),
                    "state": rule.get("State"),
                    "description": rule.get("Description", "")[:80],
                })
    except Exception as e:
        out.append({"error": str(e)})
    return out


def s3_object_info(key: str):
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last = head["LastModified"]
        size = head["ContentLength"]
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        # Show top-level keys of JSON
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                top_keys = list(data.keys())[:10]
                gen_at = data.get("generated_at") or data.get("as_of") or data.get("timestamp")
            else:
                top_keys = ["[non-dict]"]
                gen_at = None
        except Exception:
            top_keys = ["[parse error]"]
            gen_at = None
        return {
            "last_modified": last.isoformat(),
            "age_hours": round((now() - last).total_seconds() / 3600, 1),
            "size_bytes": size,
            "top_keys": top_keys,
            "internal_generated_at": gen_at,
        }
    except ClientError as e:
        return {"error": e.response["Error"]["Code"]}


def main():
    with report("diagnose_5_red_items") as r:
        r.heading("Diagnose 5 RED items from system-health dashboard")

        # ─────── 1. justhodl-repo-monitor → repo-data.json ───────
        r.section("1. repo-data.json (20h stale)")
        info = s3_object_info("repo-data.json")
        r.log(f"  last_modified: {info.get('last_modified')}")
        r.log(f"  age_hours: {info.get('age_hours')}")
        r.log(f"  size: {info.get('size_bytes')}")
        r.log(f"  top_keys: {info.get('top_keys')}")
        r.log(f"  internal_gen_at: {info.get('internal_generated_at')}")

        rm_inv = lambda_inv_count("justhodl-repo-monitor", hours=24)
        rm_inv_72 = lambda_inv_count("justhodl-repo-monitor", hours=72)
        r.log(f"\n  justhodl-repo-monitor: 24h={rm_inv['invocations']} inv / {rm_inv['errors']} err")
        r.log(f"  justhodl-repo-monitor: 72h={rm_inv_72['invocations']} inv / {rm_inv_72['errors']} err")

        rm_log = get_last_log_event("justhodl-repo-monitor")
        r.log(f"  last log event: {rm_log}")

        rm_rules = get_eb_rules_for_lambda("justhodl-repo-monitor")
        r.log(f"  EB rules: {rm_rules}")

        if rm_inv["errors"] > 0:
            r.log(f"  recent ERRORs (last 24h):")
            for e in get_recent_log_events("justhodl-repo-monitor", hours=24, max_events=10):
                r.log(f"    [{e.get('ts', '?')}] {e.get('msg', e)[:240]}")

        # ─────── 2 + 3. justhodl-intelligence → intelligence-report.json ───────
        r.section("2+3. justhodl-intelligence + intelligence-report.json")
        info = s3_object_info("intelligence-report.json")
        r.log(f"  last_modified: {info.get('last_modified')}")
        r.log(f"  age_hours: {info.get('age_hours')}")
        r.log(f"  size: {info.get('size_bytes')}")
        r.log(f"  top_keys: {info.get('top_keys')}")

        intel_inv = lambda_inv_count("justhodl-intelligence", hours=24)
        intel_inv_72 = lambda_inv_count("justhodl-intelligence", hours=72)
        r.log(f"\n  justhodl-intelligence: 24h={intel_inv['invocations']} inv / {intel_inv['errors']} err")
        r.log(f"  justhodl-intelligence: 72h={intel_inv_72['invocations']} inv / {intel_inv_72['errors']} err")

        intel_log = get_last_log_event("justhodl-intelligence")
        r.log(f"  last log event: {intel_log}")

        intel_rules = get_eb_rules_for_lambda("justhodl-intelligence")
        r.log(f"  EB rules: {intel_rules}")

        if intel_inv["errors"] > 0:
            r.log(f"  recent ERRORs (last 24h):")
            for e in get_recent_log_events("justhodl-intelligence", hours=24, max_events=10):
                r.log(f"    [{e.get('ts', '?')}] {e.get('msg', e)[:240]}")

        # ─────── 4. justhodl-nyfed-dealer-survey ───────
        r.section("4. justhodl-nyfed-dealer-survey (0 inv / 24h)")
        ny_inv = lambda_inv_count("justhodl-nyfed-dealer-survey", hours=24)
        ny_inv_72 = lambda_inv_count("justhodl-nyfed-dealer-survey", hours=72)
        ny_inv_168 = lambda_inv_count("justhodl-nyfed-dealer-survey", hours=168)
        r.log(f"  24h: {ny_inv['invocations']} inv / {ny_inv['errors']} err")
        r.log(f"  72h: {ny_inv_72['invocations']} inv / {ny_inv_72['errors']} err")
        r.log(f"  168h (7d): {ny_inv_168['invocations']} inv / {ny_inv_168['errors']} err")

        ny_log = get_last_log_event("justhodl-nyfed-dealer-survey")
        r.log(f"  last log event: {ny_log}")

        ny_rules = get_eb_rules_for_lambda("justhodl-nyfed-dealer-survey")
        r.log(f"  EB rules: {ny_rules}")

        # ─────── 5. justhodl-oecd-cli ───────
        r.section("5. justhodl-oecd-cli (0 inv / 24h)")
        oe_inv = lambda_inv_count("justhodl-oecd-cli", hours=24)
        oe_inv_72 = lambda_inv_count("justhodl-oecd-cli", hours=72)
        oe_inv_168 = lambda_inv_count("justhodl-oecd-cli", hours=168)
        r.log(f"  24h: {oe_inv['invocations']} inv / {oe_inv['errors']} err")
        r.log(f"  72h: {oe_inv_72['invocations']} inv / {oe_inv_72['errors']} err")
        r.log(f"  168h (7d): {oe_inv_168['invocations']} inv / {oe_inv_168['errors']} err")

        oe_log = get_last_log_event("justhodl-oecd-cli")
        r.log(f"  last log event: {oe_log}")

        oe_rules = get_eb_rules_for_lambda("justhodl-oecd-cli")
        r.log(f"  EB rules: {oe_rules}")


if __name__ == "__main__":
    main()
