#!/usr/bin/env python3
"""
Polish round — verify recent fixes still healthy + investigate stale
data files.

Tasks:
  A. Did BRK.B fix land?  → s3 stocks dict should now contain BRK.B
  B. Did baseline_price fix hold up?  → check signals from last 2 hours
  C. predictions.json staleness  → who writes it, what's the schedule,
     is the producing Lambda erroring?
  D. valuations-data.json 23-day staleness  → same investigation
"""
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


with report("polish_audit") as r:
    r.heading("Polish audit — verify recent fixes + investigate stale data")

    # ─── A. BRK.B fix landed? ───
    r.section("A. Did BRK.B make it into the latest report.json?")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        rpt = json.loads(obj["Body"].read())
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  report.json age: {age_min:.1f} min")
        stocks = rpt.get("stocks") or {}
        r.log(f"  Total stocks: {len(stocks)}")

        for ticker in ["BRK.B", "BRK-B", "BRKB"]:
            if ticker in stocks:
                info = stocks[ticker]
                if isinstance(info, dict):
                    price = info.get("price")
                    name = info.get("name", "?")
                    r.ok(f"  ✓ Found '{ticker}': name='{name}', price=${price}")
                else:
                    r.log(f"  ⚠ Found '{ticker}' but shape is {type(info).__name__}: {info}")
                break
        else:
            r.warn(f"  ✗ BRK.B not in stocks dict. Sample tickers: {sorted(list(stocks.keys()))[:10]}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── B. baseline_price fix still landing? ───
    r.section("B. Are signals from the last 2h still getting baseline_price?")
    try:
        from boto3.dynamodb.conditions import Attr
        cutoff_ts = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp())
        table = ddb.Table("justhodl-signals")
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
            ProjectionExpression="signal_type, baseline_price, baseline_benchmark_price, logged_epoch",
        )
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = table.scan(
                FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
                ExclusiveStartKey=resp["LastEvaluatedKey"],
                ProjectionExpression="signal_type, baseline_price, baseline_benchmark_price, logged_epoch",
            )
            items += resp.get("Items", [])

        r.log(f"  Signals logged in last 2h: {len(items)}")
        with_bp = sum(1 for i in items if i.get("baseline_price") not in (None, "", 0, "0"))
        r.log(f"  With baseline_price: {with_bp} ({100*with_bp/max(len(items),1):.0f}%)")
        types = Counter(i.get("signal_type") for i in items)
        for t, c in sorted(types.items(), key=lambda x: -x[1])[:8]:
            type_items = [i for i in items if i.get("signal_type") == t]
            type_with = sum(1 for i in type_items if i.get("baseline_price") not in (None, "", 0, "0"))
            r.log(f"    {t:30}  {type_with}/{c}")
        if len(items) > 0 and with_bp / len(items) >= 0.95:
            r.ok(f"  ✓ Fix holding — {100*with_bp/len(items):.0f}% have baseline")
        else:
            r.warn(f"  Coverage <95% — investigate")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── C. predictions.json staleness ───
    r.section("C. Why is predictions.json stale?")
    try:
        # Check the file
        obj = s3.head_object(Bucket=BUCKET, Key="predictions.json")
        age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
        r.log(f"  predictions.json: {age_h:.1f}h old, {obj['ContentLength']} bytes")
        r.log(f"  Last modified: {obj['LastModified'].isoformat()}")

        # Find writer Lambda via grep + check its EB schedule
        # We know from earlier audit: writer is "justhodl-ml-predictions" or similar
        for fn_name in ["justhodl-ml-predictions", "MLPredictor"]:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn_name)
                r.log(f"\n  Lambda: {fn_name}")
                r.log(f"    LastModified: {cfg['LastModified']}")
                r.log(f"    State: {cfg.get('State')}")
                r.log(f"    StateReason: {cfg.get('StateReason', '(none)')}")
                r.log(f"    LastUpdateStatus: {cfg.get('LastUpdateStatus')}")
                r.log(f"    LastUpdateStatusReason: {cfg.get('LastUpdateStatusReason', '(none)')}")

                # EB schedules
                target_arn = cfg["FunctionArn"]
                rules = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
                r.log(f"    EB rules: {rules}")
                for rule_name in rules:
                    rule = eb.describe_rule(Name=rule_name)
                    r.log(f"      [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")

                # Recent invocations + errors
                end = datetime.now(timezone.utc)
                start = end - timedelta(hours=48)
                inv = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Invocations",
                    Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                    StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
                )
                err = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Errors",
                    Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                    StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
                )
                t_inv = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
                t_err = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
                r.log(f"    Last 48h: {int(t_inv)} invocations, {int(t_err)} errors")

                # Last log lines if errors
                if t_err > 0 or t_inv == 0:
                    streams = logs.describe_log_streams(
                        logGroupName=f"/aws/lambda/{fn_name}",
                        orderBy="LastEventTime", descending=True, limit=1,
                    ).get("logStreams", [])
                    if streams:
                        sname = streams[0]["logStreamName"]
                        st_age = (datetime.now(timezone.utc) - datetime.fromtimestamp(
                            streams[0]["lastEventTimestamp"]/1000, tz=timezone.utc)).total_seconds()/3600
                        r.log(f"    Latest stream: {sname} ({st_age:.1f}h old)")
                        ev = logs.get_log_events(
                            logGroupName=f"/aws/lambda/{fn_name}",
                            logStreamName=sname, limit=15, startFromHead=False,
                        )
                        r.log(f"    Last log lines:")
                        for e in ev.get("events", [])[-10:]:
                            r.log(f"      {e['message'].strip()[:200]}")
            except lam.exceptions.ResourceNotFoundException:
                r.log(f"  Lambda '{fn_name}' not found, skipping")
            except Exception as e:
                r.log(f"  Lambda '{fn_name}' check failed: {e}")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── D. valuations-data.json 23-day staleness ───
    r.section("D. Why is valuations-data.json 23 days stale?")
    try:
        obj = s3.head_object(Bucket=BUCKET, Key="valuations-data.json")
        age_d = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 86400
        r.log(f"  valuations-data.json: {age_d:.1f} days old, {obj['ContentLength']} bytes")
        r.log(f"  Last modified: {obj['LastModified'].isoformat()}")

        fn_name = "justhodl-valuations-agent"
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            r.log(f"\n  Lambda: {fn_name}")
            r.log(f"    LastModified: {cfg['LastModified']}")
            r.log(f"    State: {cfg.get('State')}")
            r.log(f"    StateReason: {cfg.get('StateReason', '(none)')}")
            r.log(f"    LastUpdateStatus: {cfg.get('LastUpdateStatus')}")
            r.log(f"    Timeout: {cfg.get('Timeout')}s, Memory: {cfg.get('MemorySize')}MB")

            # EB schedules
            target_arn = cfg["FunctionArn"]
            rules = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
            for rule_name in rules:
                rule = eb.describe_rule(Name=rule_name)
                r.log(f"      [{rule.get('State')}] {rule_name}: {rule.get('ScheduleExpression')}")

            # Last invocations
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=30)
            inv = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
            )
            err = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
            )
            r.log(f"    Daily breakdown last 30 days (invocations / errors):")
            inv_by_day = {p["Timestamp"].date().isoformat(): int(p.get("Sum", 0)) for p in inv.get("Datapoints", [])}
            err_by_day = {p["Timestamp"].date().isoformat(): int(p.get("Sum", 0)) for p in err.get("Datapoints", [])}
            all_days = sorted(set(list(inv_by_day.keys()) + list(err_by_day.keys())))
            for d in all_days[-10:]:
                ic = inv_by_day.get(d, 0)
                ec = err_by_day.get(d, 0)
                marker = " ⚠" if ec > 0 else ""
                r.log(f"      {d}: {ic} inv / {ec} err{marker}")
            t_inv = sum(inv_by_day.values())
            t_err = sum(err_by_day.values())
            r.log(f"    Total last 30d: {t_inv} invocations, {t_err} errors")

            # If errors, get a sample
            if t_err > 0:
                streams = logs.describe_log_streams(
                    logGroupName=f"/aws/lambda/{fn_name}",
                    orderBy="LastEventTime", descending=True, limit=2,
                ).get("logStreams", [])
                for s_ in streams[:1]:
                    sname = s_["logStreamName"]
                    ev = logs.get_log_events(
                        logGroupName=f"/aws/lambda/{fn_name}",
                        logStreamName=sname, limit=20, startFromHead=False,
                    )
                    r.log(f"\n    Last 10 log lines from {sname}:")
                    for e in ev.get("events", [])[-15:]:
                        m = e["message"].strip()
                        if m and not m.startswith("REPORT") and not m.startswith("END") and not m.startswith("START"):
                            r.log(f"      {m[:250]}")
        except Exception as e:
            r.log(f"  Lambda check failed: {e}")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
