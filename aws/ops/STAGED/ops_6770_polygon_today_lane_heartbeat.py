"""ops 6770 - READ-ONLY heartbeat of the forming-bar lane: is justhodl-polygon-today firing on its own from the
jhk-tick-session-5min fan-out (not only from ops 6769's hand invoke)? Prints today-manifest.json (generated_at, as_of, n),
the function's CloudWatch invocation / error counts over the last 2 hours in 5-minute buckets, the scheduler's last
tick lines that name it, and the Worker /ohlc SPY last bar. Nothing is written. Re-runnable any time during a session.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import sys
import time

import boto3
import requests
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-polygon-today"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
NY = ZoneInfo("America/New_York")


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6770_polygon_today_lane_heartbeat") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6770 - forming-bar lane heartbeat %s (%s ET)" % (now.isoformat(timespec="minutes"), now.astimezone(NY).strftime("%H:%M")))
        man = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/today-manifest.json")["Body"].read())
        gen = datetime.fromisoformat(man["generated_at"])
        r.log("today-manifest: session %s | generated %s (%.1f min ago) | as_of %s | n %s of %s tickers | seconds %s | SPY %s" % (
            man.get("session"), man.get("generated_at"), (now - gen).total_seconds() / 60, man.get("as_of"), man.get("n"), man.get("n_tickers"), man.get("seconds"), (man.get("sample") or {}).get("SPY")))
        start = now - timedelta(hours=2)
        for metric in ("Invocations", "Errors", "Duration"):
            res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": FN}], StartTime=start, EndTime=now, Period=300,
                                           Statistics=["Sum"] if metric != "Duration" else ["Average"])
            pts = sorted(res.get("Datapoints") or [], key=lambda x: x["Timestamp"])
            r.log("%s (5-min buckets, last 2 h): %s" % (metric, ", ".join("%s=%s" % (p["Timestamp"].astimezone(NY).strftime("%H:%M"), int(p.get("Sum", p.get("Average", 0)))) for p in pts) or "none"))
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-scheduler", startTime=int(start.timestamp() * 1000), filterPattern='"session-5min"', limit=12)
            for e in (ev.get("events") or [])[-6:]:
                r.log("  scheduler %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, tz=NY).strftime("%H:%M:%S"), e["message"].strip()[:220]))
        except Exception as e:  # noqa: BLE001
            r.log("scheduler logs: %s" % type(e).__name__)
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=int(start.timestamp() * 1000), filterPattern='"wrote"', limit=20)
            for e in (ev.get("events") or [])[-6:]:
                r.log("  %s %s: %s" % (FN, datetime.fromtimestamp(e["timestamp"] / 1000, tz=NY).strftime("%H:%M:%S"), e["message"].strip()[:260]))
        except Exception as e:  # noqa: BLE001
            r.log("engine logs: %s" % type(e).__name__)
        try:
            rr = requests.get(PROXY + "/ohlc?ticker=SPY&span=day&mult=1&days=5&nocache=%d" % int(time.time() * 1000), timeout=40)
            bars = (rr.json() or {}).get("bars") or []
            last = bars[-1] if bars else {}
            r.log("Worker /ohlc SPY: HTTP %s | last bar %s ET close %s vol %s" % (rr.status_code, datetime.fromtimestamp(float(last.get("time", 0)), tz=NY).strftime("%Y-%m-%d %H:%M") if last else None, last.get("close"), last.get("value")))
        except Exception as e:  # noqa: BLE001
            r.log("Worker /ohlc: %s" % type(e).__name__)
        r.ok("ops 6770 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
