#!/usr/bin/env python3
"""
Verify the price-fetch fix actually worked end-to-end.

Step 57 deployed the fix and async-triggered a backfill. Now check:
  A. Latest outcome-checker invocation: success/failure?
  B. Did it actually score outcomes (not "No price for X" everywhere)?
  C. Outcomes table item count growth (was 738 before)
  D. Sample 5 fresh outcomes — are they correct=True/False (not None)?
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.client("dynamodb", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("verify_price_fix") as r:
    r.heading("Verify price-fetch fix worked — outcomes actually scoring")

    # ─── A. Latest invocation status ───
    r.section("A. Outcome-checker recent invocations + errors")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-outcome-checker"}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
        )
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-outcome-checker"}],
            StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
        )
        dur = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-outcome-checker"}],
            StartTime=start, EndTime=end, Period=300,
            Statistics=["Average", "Maximum"],
        )
        total_inv = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        total_err = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        max_dur = max((p.get("Maximum", 0) for p in dur.get("Datapoints", [])), default=0)
        r.log(f"  Last 30 min: {int(total_inv)} invocations, {int(total_err)} errors")
        r.log(f"  Max duration: {max_dur:.0f}ms")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── B. Sample log lines from latest run ───
    r.section("B. Sample log output — are prices being fetched?")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-outcome-checker",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        if not streams:
            r.warn("  No log streams found")
        else:
            s = streams[0]
            stream_age = (datetime.now(timezone.utc) - datetime.fromtimestamp(
                s["lastEventTimestamp"]/1000, tz=timezone.utc)).total_seconds() / 60
            r.log(f"  Latest stream: {s['logStreamName']} ({stream_age:.1f} min old)")

            start_ts = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-outcome-checker",
                logStreamName=s["logStreamName"],
                startTime=start_ts, limit=500, startFromHead=True,
            )
            events = ev.get("events", [])

            no_price = 0
            correct = 0
            wrong = 0
            polygon_403 = 0
            fmp_403 = 0
            successful_fetches = 0
            sample_correct = []
            sample_wrong = []

            for e in events:
                m = e.get("message", "").strip()
                if "No price for" in m:
                    no_price += 1
                if "✅ CORRECT" in m:
                    correct += 1
                    if len(sample_correct) < 3:
                        sample_correct.append(m[:200])
                if "❌ WRONG" in m:
                    wrong += 1
                    if len(sample_wrong) < 3:
                        sample_wrong.append(m[:200])
                if "Polygon HTTP 403" in m: polygon_403 += 1
                if "FMP HTTP 403" in m: fmp_403 += 1

            r.log(f"  Log line counts (last 30 min):")
            r.log(f"    Scored CORRECT: {correct}")
            r.log(f"    Scored WRONG:   {wrong}")
            r.log(f"    No price:       {no_price}")
            r.log(f"    Polygon 403s:   {polygon_403}")
            r.log(f"    FMP 403s:       {fmp_403}")
            r.log("")
            if correct + wrong > 0:
                pct_scored = 100 * (correct + wrong) / max(correct + wrong + no_price, 1)
                r.ok(f"  {correct + wrong} predictions scored, {pct_scored:.0f}% of attempts succeeded")
            elif no_price > 0:
                r.fail(f"  Still failing — {no_price} 'No price' lines, no scores produced")
            else:
                r.warn(f"  No score lines yet — Lambda may still be running through queue")

            r.log(f"\n  Sample CORRECT lines:")
            for s_c in sample_correct[:3]:
                r.log(f"    {s_c}")
            r.log(f"\n  Sample WRONG lines:")
            for s_w in sample_wrong[:3]:
                r.log(f"    {s_w}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── C. Outcomes table growth ───
    r.section("C. Outcomes table item count")
    try:
        td = ddb.describe_table(TableName="justhodl-outcomes")
        new_count = td["Table"].get("ItemCount", "unknown")
        new_size = td["Table"].get("TableSizeBytes", 0)
        r.log(f"  Items now:    {new_count}")
        r.log(f"  Items before: 738 (per Step 54 baseline)")
        if isinstance(new_count, int):
            growth = new_count - 738
            r.log(f"  Net growth:   {growth:+d}")
            if growth > 100:
                r.ok(f"  Backfill processing real outcomes")
            elif growth > 0:
                r.log(f"  Slight growth — may still be running")
            else:
                r.warn(f"  No growth — backfill may have failed silently")
        r.log(f"  Size:         {new_size:,} bytes")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── D. Sample fresh outcomes ───
    r.section("D. Sample 5 most-recent outcomes — correct=True/False?")
    try:
        from boto3.dynamodb.conditions import Attr
        ddb_res = boto3.resource("dynamodb", region_name=REGION)
        table = ddb_res.Table("justhodl-outcomes")

        # Scan for most recent items (no GSI on logged_at, so just scan)
        resp = table.scan(Limit=200)
        items = resp.get("Items", [])

        # Sort by checked_at descending
        def get_checked(i):
            return i.get("checked_at", "")
        items.sort(key=get_checked, reverse=True)

        r.log(f"  Most recent 10 outcomes (by checked_at):")
        for item in items[:10]:
            stype = item.get("signal_type", "?")
            window = item.get("window_key", "?")
            pred = item.get("predicted_dir", "?")
            correct = item.get("correct")
            outcome = item.get("outcome", {}) or {}
            actual = outcome.get("actual_direction", "?")
            ret = outcome.get("return_pct") or outcome.get("excess_return", "?")
            checked = item.get("checked_at", "?")[:19]
            r.log(f"    [{checked}] {stype}/{window}: predicted={pred}, actual={actual}, "
                  f"return={ret}, correct={correct}")

        # Counts by correct status
        n_true = sum(1 for i in items if i.get("correct") is True)
        n_false = sum(1 for i in items if i.get("correct") is False)
        n_none = sum(1 for i in items if i.get("correct") not in (True, False))
        r.log(f"\n  In sample of {len(items)}:")
        r.log(f"    correct=True:  {n_true}")
        r.log(f"    correct=False: {n_false}")
        r.log(f"    correct=None:  {n_none}")
        if n_true + n_false > 0:
            r.ok(f"  Real outcomes being recorded — calibrator will work next Sunday")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
