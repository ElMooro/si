#!/usr/bin/env python3
"""
Step 78 — Was carry_risk actually missing, or was the scan paginating
out before finding it?

DynamoDB scan with FilterExpression returns up to 1MB of scanned items
BEFORE filtering. So a small scan against a large table easily misses
items that match the filter. Need to paginate fully.

Also useful: trigger one more signal-logger run RIGHT NOW and tail the
logs to see what intelligence-report.json contained at the moment of
the run.
"""
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)
ddb_client = boto3.client("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("paginated_scan_carry_risk") as r:
    r.heading("Was carry_risk missing or did scan paginate out before finding it?")

    # ─── A. What does intelligence-report.json have RIGHT NOW? ───
    r.section("A. Current intelligence-report.json scores")
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="intelligence-report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    data = json.loads(obj["Body"].read())
    scores = data.get("scores", {})
    r.log(f"  Age: {age_min:.1f} min")
    r.log(f"  scores: {json.dumps(scores, indent=2)}")

    # ─── B. Trigger signal-logger SYNC and check immediately ───
    r.section("B. Synchronous signal-logger invoke")
    resp = lam.invoke(FunctionName="justhodl-signal-logger", InvocationType="RequestResponse")
    body = resp.get("Payload").read().decode()
    r.log(f"  Status: {resp['StatusCode']}, body: {body[:200]}")

    # ─── C. Read latest logger log line-by-line ───
    r.section("C. Latest signal-logger log output")
    streams = logs.describe_log_streams(
        logGroupName="/aws/lambda/justhodl-signal-logger",
        orderBy="LastEventTime", descending=True, limit=1,
    ).get("logStreams", [])
    if streams:
        sname = streams[0]["logStreamName"]
        r.log(f"  Stream: {sname}")
        ev = logs.get_log_events(
            logGroupName="/aws/lambda/justhodl-signal-logger",
            logStreamName=sname, limit=200, startFromHead=False,
        )
        # Filter to ml_risk / carry_risk specific lines
        for e in ev.get("events", [])[-100:]:
            m = e["message"].strip()
            if "ml_risk" in m.lower() or "carry_risk" in m.lower() or "[LOG]" in m:
                if "ml_risk" in m or "carry_risk" in m:
                    r.log(f"    {m[:240]}")

    # ─── D. Paginated scan: find ALL carry_risk records ever ───
    r.section("D. Paginated scan — total carry_risk records in table")
    table = ddb.Table("justhodl-signals")
    from boto3.dynamodb.conditions import Attr

    total = 0
    recent_count = 0
    sample = []
    cutoff_recent = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())

    last_key = None
    pages = 0
    while True:
        kwargs = dict(
            FilterExpression=Attr("signal_type").eq("carry_risk"),
            ProjectionExpression="signal_id, signal_value, logged_at, logged_epoch",
        )
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items = resp.get("Items", [])
        total += len(items)
        for item in items:
            le = item.get("logged_epoch")
            if le and int(le) >= cutoff_recent:
                recent_count += 1
            if len(sample) < 5:
                sample.append(item)
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key:
            break
        if pages > 10:
            r.log(f"  (stopping at 10 pages to avoid timeout)")
            break

    r.log(f"  Total carry_risk records: {total} (across {pages} pages)")
    r.log(f"  In last 10 min: {recent_count}")
    r.log(f"\n  Sample carry_risk records (most recent first):")
    sample.sort(key=lambda x: x.get("logged_epoch", 0), reverse=True)
    for s_ in sample[:5]:
        r.log(f"    logged_at={s_.get('logged_at')}, signal_value={s_.get('signal_value')}")

    r.kv(
        carry_risk_total_records=total,
        carry_risk_in_last_10min=recent_count,
        ml_risk_score_now=scores.get("ml_risk_score"),
        carry_risk_score_now=scores.get("carry_risk_score"),
    )
    r.log("Done")
