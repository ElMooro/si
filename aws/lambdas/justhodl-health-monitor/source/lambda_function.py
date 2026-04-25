"""
justhodl-health-monitor

Runs every 15 minutes. For each component in expectations.py, checks
reality (S3 file age + size, Lambda errors + invocations, DynamoDB
item count, SSM age, EB rule state) and writes a unified dashboard
JSON to s3://justhodl-dashboard-live/_health/dashboard.json.

Also writes a state-transition log to _health/transitions.jsonl so
the alerting layer can detect green→red transitions and only fire
Telegram on flips (not every 15 min).
"""
import json
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

# Inlined from aws/ops/health/expectations.py at deploy time
# (see deploy script — it copies the file into the Lambda zip)
from expectations import EXPECTATIONS, status_for_age, status_for_size, severity_rank

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def now():
    return datetime.now(timezone.utc)


def isoformat(dt):
    return dt.isoformat() if dt else None


# ─── Component checkers ──────────────────────────────────────────────

def check_s3_file(spec):
    """Check an S3 object's age + size."""
    key = spec["key"]
    out = {"id": f"s3:{key}", "type": "s3_file", "key": key,
           "note": spec.get("note", ""), "severity": spec.get("severity", "important"),
           "known_broken": spec.get("known_broken", False)}
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last_mod = head["LastModified"]
        size = head["ContentLength"]
        age_sec = (now() - last_mod).total_seconds()
        out["last_modified"] = isoformat(last_mod)
        out["age_sec"] = age_sec
        out["size_bytes"] = size
        out["age_status"] = status_for_age(age_sec, spec.get("fresh_max"), spec.get("warn_max"))
        out["size_status"] = status_for_size(size, spec.get("expected_size"))
        # Combined: worst of the two
        statuses = [out["age_status"], out["size_status"]]
        out["status"] = "red" if "red" in statuses else "yellow" if "yellow" in statuses else "green" if "green" in statuses else "unknown"
        # If known_broken: never alarm. Force to "info" regardless.
        if spec.get("known_broken"):
            out["status"] = "info"
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            out["status"] = "red"
            out["error"] = "missing"
        else:
            out["status"] = "unknown"
            out["error"] = str(e)
    return out


def check_lambda(spec):
    """Check a Lambda's recent error rate + invocation count."""
    name = spec["name"]
    out = {"id": f"lambda:{name}", "type": "lambda", "name": name,
           "note": spec.get("note", ""), "severity": spec.get("severity", "important")}
    try:
        # Last 24h metrics
        end = now()
        start = end - timedelta(hours=24)
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
        total_inv = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        total_err = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        out["invocations_24h"] = int(total_inv)
        out["errors_24h"] = int(total_err)
        out["error_rate_24h"] = round(total_err / max(total_inv, 1), 4)

        # Status
        max_err_rate = spec.get("max_error_rate", 0.20)
        min_inv = spec.get("min_invocations_24h", 0)
        if total_inv < min_inv:
            out["status"] = "red"
            out["reason"] = f"only {int(total_inv)} invocations in 24h (expected ≥{min_inv})"
        elif out["error_rate_24h"] > max_err_rate:
            out["status"] = "red"
            out["reason"] = f"error rate {out['error_rate_24h']:.1%} exceeds {max_err_rate:.0%}"
        elif out["error_rate_24h"] > max_err_rate * 0.5:
            out["status"] = "yellow"
            out["reason"] = f"error rate {out['error_rate_24h']:.1%} elevated"
        else:
            out["status"] = "green"
    except Exception as e:
        out["status"] = "unknown"
        out["error"] = str(e)
    return out


def check_dynamodb(spec):
    """Check a DynamoDB table's item count."""
    table = spec["table"]
    out = {"id": f"ddb:{table}", "type": "dynamodb", "table": table,
           "note": spec.get("note", ""), "severity": spec.get("severity", "important")}
    try:
        td = ddb.describe_table(TableName=table)["Table"]
        item_count = td.get("ItemCount", 0)
        size_bytes = td.get("TableSizeBytes", 0)
        out["item_count"] = item_count
        out["size_bytes"] = size_bytes
        min_items = spec.get("min_items", 0)
        if item_count < min_items:
            out["status"] = "red"
            out["reason"] = f"item count {item_count} below expected {min_items}"
        else:
            out["status"] = "green"
    except Exception as e:
        out["status"] = "unknown"
        out["error"] = str(e)
    return out


def check_ssm(spec):
    """Check an SSM parameter's age."""
    name = spec["name"]
    out = {"id": f"ssm:{name}", "type": "ssm", "name": name,
           "note": spec.get("note", ""), "severity": spec.get("severity", "important")}
    try:
        resp = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": [name]}]
        )
        params = resp.get("Parameters", [])
        if not params:
            out["status"] = "red"
            out["error"] = "parameter not found"
            return out
        p = params[0]
        last_mod = p.get("LastModifiedDate")
        age_sec = (now() - last_mod).total_seconds() if last_mod else None
        out["last_modified"] = isoformat(last_mod)
        out["age_sec"] = age_sec
        out["status"] = status_for_age(age_sec, spec.get("fresh_max"), spec.get("warn_max"))
    except Exception as e:
        out["status"] = "unknown"
        out["error"] = str(e)
    return out


def check_eb_rule(spec):
    """Check an EventBridge rule's state."""
    name = spec["name"]
    out = {"id": f"eb:{name}", "type": "eb_rule", "name": name,
           "note": spec.get("note", ""), "severity": spec.get("severity", "important")}
    try:
        rule = eb.describe_rule(Name=name)
        state = rule.get("State", "?")
        schedule = rule.get("ScheduleExpression", "?")
        out["state"] = state
        out["schedule"] = schedule
        expected = spec.get("expected_state", "ENABLED")
        if state == expected:
            out["status"] = "green"
        else:
            out["status"] = "red"
            out["reason"] = f"state={state}, expected={expected}"
    except eb.exceptions.ResourceNotFoundException:
        out["status"] = "red"
        out["error"] = "rule not found"
    except Exception as e:
        out["status"] = "unknown"
        out["error"] = str(e)
    return out


CHECKERS = {
    "s3_file": check_s3_file,
    "lambda": check_lambda,
    "dynamodb": check_dynamodb,
    "ssm": check_ssm,
    "eb_rule": check_eb_rule,
}


def lambda_handler(event, context):
    started = now()
    components = []

    # Walk every spec, run the appropriate checker
    for spec_id, spec in EXPECTATIONS.items():
        checker = CHECKERS.get(spec.get("type"))
        if not checker:
            print(f"[SKIP] unknown type: {spec_id}")
            continue
        try:
            result = checker(spec)
            components.append(result)
        except Exception as e:
            print(f"[ERR] {spec_id}: {e}")
            components.append({
                "id": spec_id,
                "status": "unknown",
                "error": str(e),
                "severity": spec.get("severity", "important"),
            })

    # Sort: red first, then yellow, then unknown, then green; ties broken by severity
    status_rank = {"red": 0, "yellow": 1, "info": 2, "unknown": 3, "green": 4}
    components.sort(key=lambda c: (status_rank.get(c.get("status"), 9), severity_rank(c.get("severity", ""))))

    # Counts
    counts = {"green": 0, "yellow": 0, "red": 0, "info": 0, "unknown": 0}
    for c in components:
        counts[c.get("status", "unknown")] = counts.get(c.get("status", "unknown"), 0) + 1

    # Top-level system status
    if counts["red"] > 0:
        system = "red"
    elif counts["yellow"] > 0:
        system = "yellow"
    elif counts["unknown"] > 0:
        system = "unknown"
    else:
        system = "green"

    dashboard = {
        "generated_at": isoformat(started),
        "checked_at_unix": int(time.time()),
        "duration_sec": (now() - started).total_seconds(),
        "system_status": system,
        "counts": counts,
        "total_components": len(components),
        "components": components,
    }

    # Write dashboard.json
    s3.put_object(
        Bucket=BUCKET,
        Key="_health/dashboard.json",
        Body=json.dumps(dashboard, indent=2, default=str),
        ContentType="application/json",
        CacheControl="max-age=60",
    )

    # Append a one-line summary to history (jsonl)
    summary = {
        "ts": isoformat(started),
        "system": system,
        "counts": counts,
    }
    try:
        # Read current jsonl, append, rewrite (small file)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="_health/history.jsonl")
            existing = obj["Body"].read().decode()
        except ClientError:
            existing = ""
        lines = existing.strip().split("\n") if existing.strip() else []
        lines.append(json.dumps(summary, default=str))
        # Keep last 500 entries (~5 days at 15-min cadence)
        lines = lines[-500:]
        s3.put_object(
            Bucket=BUCKET,
            Key="_health/history.jsonl",
            Body="\n".join(lines).encode(),
            ContentType="application/x-ndjson",
        )
    except Exception as e:
        print(f"[HISTORY] {e}")

    print(f"[DONE] system={system} red={counts['red']} yellow={counts['yellow']} green={counts['green']}")
    return {"statusCode": 200, "body": json.dumps(dashboard, default=str)[:500]}
