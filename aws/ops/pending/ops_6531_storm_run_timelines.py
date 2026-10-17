"""ops 6531 - READ-ONLY: why did one miss-calibrator run cost 20x more during the storm?

ops 6530 measured: a calibrator run took 0.75 s on a normal Monday (Oct 5) and 15.3 s on average during the storm
(Oct 15, p50 15.27 s).  Each run makes the same calls either way - S3 reads, one S3 write, two SSM PutParameter
(both failing "TooManyUpdates" in the storm), one Telegram POST (401, dead bot token, urlopen timeout=15 s),
one EventBridge PutEvents.  print() lines carry no request id, but one log stream is one execution environment
running one request at a time, so the lines of a stream between START and END are one run.  This op prints the
line-by-line timeline of a few complete runs (calibrator + coordinator) on Oct 15 12:00 UTC, and the calibrator's
average Duration per 5 minutes on Oct 12 09:10-11:00 (when did runs slow down).  Cost: well under 1 cent.
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CAL, COORD = "justhodl-miss-calibrator", "justhodl-event-coordinator"
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6531.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})
logs = boto3.client("logs", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
UTC = timezone.utc
out = {"op": 6531, "ran_at": datetime.now(UTC).isoformat(), "errors": {}}


def T(*a):
    return datetime(*a, tzinfo=UTC)


def insights(group, query, start, end, limit=100):
    qid = logs.start_query(logGroupName=group, startTime=int(start.timestamp()), endTime=int(end.timestamp()),
                           queryString=query, limit=limit)["queryId"]
    for _ in range(120):
        res = logs.get_query_results(queryId=qid)
        if res["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return [{f["field"]: f["value"] for f in row} for row in res.get("results", [])]
        time.sleep(2)
    return []


def ms(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC).timestamp() * 1000.0


def timelines(r, fn, n_streams):
    group = "/aws/lambda/" + fn
    s, e = T(2026, 10, 15, 12, 0, 0), T(2026, 10, 15, 12, 0, 20)
    reps = insights(group, "filter @type = \"REPORT\" | fields @logStream, @duration | limit 40", s, e, 40)
    streams = []
    for row in reps:
        if row.get("@logStream") and row["@logStream"] not in streams:
            streams.append(row["@logStream"])
        if len(streams) >= n_streams:
            break
    runs_all = []
    for stream in streams:
        lines = insights(group, "fields @timestamp, @message | filter @logStream = \"%s\" | sort @timestamp asc | limit 60"
                         % stream, T(2026, 10, 15, 12, 0, 0), T(2026, 10, 15, 12, 1, 30), 60)
        run, runs = None, []
        for ln in lines:
            msg = (ln.get("@message") or "").strip()
            t = ms(ln["@timestamp"])
            if msg.startswith("START"):
                run = {"t0": t, "steps": []}
            elif run is not None and msg.startswith("REPORT"):
                run["report"] = msg[:160].replace("\t", " ")
                runs.append(run)
                run = None
            elif run is not None and not msg.startswith("END") and not msg.startswith("XRAY"):
                run["steps"].append((round(t - run["t0"]), msg[:110]))
        runs_all += runs[:2]
        for rn in runs[:2]:
            r.log("%s run (%s):" % (fn, rn.get("report", "")[:110]))
            prev = 0
            for at, msg in rn["steps"]:
                r.log("    +%6d ms (step %6d ms)  %s" % (at, at - prev, msg))
                prev = at
    out[fn] = runs_all
    return runs_all


def main():
    with report("ops_6531_storm_run_timelines") as r:
        r.heading("ops 6531 - where a storm-time run spent its time (read-only)")
        r.section("Calibrator runs, Oct 15 12:00 UTC")
        try:
            timelines(r, CAL, 3)
        except Exception as e:  # noqa: BLE001
            out["errors"]["cal"] = str(e)[:200]
        r.section("Coordinator runs, Oct 15 12:00 UTC")
        try:
            timelines(r, COORD, 2)
        except Exception as e:  # noqa: BLE001
            out["errors"]["coord"] = str(e)[:200]
        r.section("When runs slowed down (Oct 12)")
        try:
            for fn in (CAL, COORD):
                pts = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Duration", StartTime=T(2026, 10, 12, 9, 10),
                    EndTime=T(2026, 10, 12, 11, 0), Period=300, Statistics=["Average"],
                    Dimensions=[{"Name": "FunctionName", "Value": fn}])["Datapoints"]
                series = [(p["Timestamp"].strftime("%H:%M"), round(p["Average"])) for p in
                          sorted(pts, key=lambda p: p["Timestamp"])]
                out["duration_oct12_" + fn] = series
                r.log("%s avg Duration ms per 5 min: %s" % (fn, series))
        except Exception as e:  # noqa: BLE001
            out["errors"]["duration"] = str(e)[:200]
        for k, v in out["errors"].items():
            r.warn("%s: %s" % (k, v))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if out["errors"].get("cal") and out["errors"].get("coord"):
            r.fail("no timelines")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
