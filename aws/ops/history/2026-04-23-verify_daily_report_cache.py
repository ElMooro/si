#!/usr/bin/env python3
"""
Verify daily-report-v3 after the FRED cache patch.

The previous workflow timed out waiting for the sync invoke to return,
but the patched Lambda was successfully deployed. Since the scheduled
EventBridge rule fires every 5 min, by now it should have run at least
once against the new code.

Check:
  1. data/fred-cache.json on S3 — proves the new cache-write code ran
  2. data/report.json last-modified and WALCL/RRP/TGA values populated
  3. secretary-latest.json (last secretary scan) used cached vs live
  4. CloudWatch metrics for daily-report-v3 errors in the last 10 min
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_daily_report_cache") as r:
    r.heading("Verify daily-report-v3 FRED cache patch is working")

    # 1. fred-cache.json
    r.section("1. fred-cache.json (shared cache)")
    try:
        c = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        cache = json.loads(c["Body"].read().decode())
        lm = c["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        r.log(f"  Size: {c['ContentLength']} bytes")
        r.log(f"  LastModified: {lm.isoformat()} (age: {age_min:.1f} min)")
        r.log(f"  Series count: {len(cache)}")
        r.log(f"  Sample series:")
        for key in ("WALCL", "RRPONTSYD", "WTREGEN", "VIXCLS", "NAPM", "CPIAUCSL"):
            v = cache.get(key)
            if v and isinstance(v, list) and v:
                latest = v[0] if isinstance(v[0], dict) else None
                if latest:
                    r.log(f"    {key}: {latest.get('value')} on {latest.get('date')}")
                else:
                    r.log(f"    {key}: unexpected shape")
            elif v and isinstance(v, dict):
                r.log(f"    {key}: value={v.get('value')} (secretary-style entry)")
            else:
                r.log(f"    {key}: missing")
        r.kv(check="fred-cache", size=c["ContentLength"], series=len(cache),
             age_min=round(age_min, 1))
    except Exception as e:
        r.warn(f"  fred-cache.json not yet created: {e}")
        r.kv(check="fred-cache", status="missing", error=str(e)[:80])

    # 2. data/report.json
    r.section("2. data/report.json freshness")
    try:
        rep_obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        rep = json.loads(rep_obj["Body"].read().decode())
        lm = rep_obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        r.log(f"  Size: {rep_obj['ContentLength']} bytes")
        r.log(f"  LastModified: {lm.isoformat()} (age: {age_min:.1f} min)")
        r.log(f"  version: {rep.get('version')}")
        r.log(f"  generated_at: {rep.get('generated_at')}")

        # Check that FRED series have values now
        fred = rep.get("fred", {}) or {}
        has_values = 0
        has_nulls = 0
        for cat, series_dict in fred.items():
            if not isinstance(series_dict, dict):
                continue
            for sid, metrics in series_dict.items():
                if isinstance(metrics, dict):
                    v = metrics.get("current") or metrics.get("value")
                    if v is not None:
                        has_values += 1
                    else:
                        has_nulls += 1
        r.log(f"  FRED series with values: {has_values}")
        r.log(f"  FRED series with nulls: {has_nulls}")

        # Spot-check the core liquidity series
        sys_risk = fred.get("systemic_risk", {})
        walcl = sys_risk.get("WALCL") or {}
        rrp = sys_risk.get("RRPONTSYD") or {}
        tga = sys_risk.get("WTREGEN") or {}
        r.log(f"  WALCL current: {walcl.get('current')}")
        r.log(f"  RRPONTSYD current: {rrp.get('current')}")
        r.log(f"  WTREGEN current: {tga.get('current')}")

        # Net liquidity value from report
        net_liq = rep.get("net_liquidity", {})
        if isinstance(net_liq, dict):
            r.log(f"  Net liquidity: {json.dumps(net_liq)[:200]}")

        r.kv(check="report", age_min=round(age_min, 1),
             fred_with_values=has_values, fred_nulls=has_nulls,
             walcl=walcl.get("current"))
    except Exception as e:
        r.fail(f"  report.json fetch failed: {e}")

    # 3. Recent CloudWatch logs for daily-report-v3
    r.section("3. Recent daily-report-v3 log groups (last 10 min)")
    try:
        log_group = "/aws/lambda/justhodl-daily-report-v3"
        ten_min_ago = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp() * 1000)
        # Find the most recent stream
        streams_resp = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=3,
        )
        for s in streams_resp.get("logStreams", [])[:3]:
            name = s.get("logStreamName", "")
            last = s.get("lastEventTimestamp", 0)
            last_dt = datetime.fromtimestamp(last / 1000, tz=timezone.utc) if last else None
            age = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60 if last_dt else None
            r.log(f"  Stream: {name[-40:]} last event {age:.1f} min ago" if age else f"  Stream: {name[-40:]}")

            # Fetch the last 40 events to see the Phase 1 output (cache hits, fetch counts)
            ev = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=name,
                startTime=ten_min_ago,
                limit=60,
                startFromHead=False,
            )
            # Filter to show relevant lines
            for e in ev.get("events", [])[-40:]:
                msg = e.get("message", "").strip()
                if any(k in msg for k in ("FRED", "Phase 1", "V10", "fresh in cache", "backstop", "ERROR", "TRACEBACK", "429")):
                    r.log(f"    {msg[:200]}")
            break  # only show most recent stream
    except Exception as e:
        r.warn(f"  Log fetch failed: {e}")

    # 4. Lambda metrics — errors in last 10 min
    r.section("4. daily-report-v3 metrics (last 15 min)")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=15)
        for metric in ("Errors", "Invocations", "Duration"):
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": "justhodl-daily-report-v3"}],
                StartTime=start, EndTime=end,
                Period=300,
                Statistics=["Sum" if metric != "Duration" else "Average"],
            )
            points = resp.get("Datapoints", [])
            for p in sorted(points, key=lambda x: x["Timestamp"]):
                ts = p["Timestamp"].isoformat()
                val = p.get("Sum") if metric != "Duration" else p.get("Average")
                unit = "ms" if metric == "Duration" else ""
                r.log(f"  {metric} {ts}: {val}{unit}")
    except Exception as e:
        r.warn(f"  Metrics fetch failed: {e}")

    r.log("Done")
