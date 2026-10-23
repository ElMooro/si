"""ops 6619 - READ-ONLY probe: justhodl-gdelt-sentiment stopped writing data/gdelt-news.json after 2026-10-21 03:43 UTC
(daily health run 2026-10-23, ops 6618 found it 2.3 d stale, not held). Reads only: CloudWatch Invocations / Errors /
Duration / Throttles per hour over the last 72 h, the function configuration (timeout, memory, state, concurrency), the
last error / timeout lines from its log group, the declared output's head and its `generated_at` / counters. Nothing is
invoked, nothing is written.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-gdelt-sentiment"
KEY = "data/gdelt-news.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def main():
    lam, s3, cw, logs = c("lambda"), c("s3"), c("cloudwatch"), c("logs")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=72)
    with report("ops_6619_gdelt_sentiment_probe") as r:
        r.heading("ops 6619 - %s probe (read-only), %s" % (FN, now.strftime("%Y-%m-%d %H:%M UTC")))

        r.section("function")
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.log("state %s, last modified %s, timeout %ss, memory %s MB, runtime %s, reserved concurrency %s" % (
            cfg.get("State"), cfg.get("LastModified"), cfg.get("Timeout"), cfg.get("MemorySize"), cfg.get("Runtime"),
            json.dumps(cfg.get("ReservedConcurrentExecutions"))))
        try:
            conc = lam.get_function_concurrency(FunctionName=FN)
            r.log("reserved concurrency (get_function_concurrency): %s" % json.dumps(conc.get("ReservedConcurrentExecutions")))
        except Exception as ex:  # noqa: BLE001
            r.log("concurrency read: %s" % type(ex).__name__)

        r.section("metrics, last 72 h by hour")
        tot = {}
        for metric, stat in (("Invocations", "Sum"), ("Errors", "Sum"), ("Throttles", "Sum"), ("Duration", "Maximum")):
            pts = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": FN}],
                                           StartTime=start, EndTime=now, Period=3600, Statistics=[stat])["Datapoints"]
            vals = sorted((p["Timestamp"].strftime("%m-%d %H:00"), round(p[stat])) for p in pts if p[stat] > 0)
            tot[metric] = sum(v for _, v in vals) if stat == "Sum" else (max((v for _, v in vals), default=0))
            r.log("%s (%s): total/max %s  %s" % (metric, stat, tot[metric], json.dumps(vals)))

        r.section("log group, last 72 h (errors, timeouts, tracebacks)")
        seen = 0
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/%s" % FN, startTime=int(start.timestamp() * 1000),
                                        filterPattern="?Traceback ?ERROR ?\"Task timed out\" ?Error", limit=60)["events"]
            for e in ev[-15:]:
                r.log("  %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S"),
                                   e["message"].strip().replace("\n", " | ")[:900]))
                seen += 1
            r.log("matching lines: %d (showing last %d)" % (len(ev), seen))
            rep = logs.filter_log_events(logGroupName="/aws/lambda/%s" % FN, startTime=int(start.timestamp() * 1000),
                                         filterPattern="REPORT", limit=20)["events"]
            for e in rep[-6:]:
                r.log("  %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S"),
                                   e["message"].strip().replace("\n", " ")[:300]))
        except Exception as ex:  # noqa: BLE001
            r.log("log read skipped: %s" % type(ex).__name__)

        r.section("declared output")
        try:
            h = s3.head_object(Bucket=LIVE, Key=KEY)
            r.log("%s last modified %s, %s bytes" % (KEY, h["LastModified"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), h["ContentLength"]))
            body = json.loads(s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read())
            top = {k: (v if isinstance(v, (str, int, float, bool)) or v is None else "%s[%d]" % (type(v).__name__, len(v)))
                   for k, v in body.items()} if isinstance(body, dict) else {"type": type(body).__name__}
            r.log("top-level: %s" % json.dumps(top, default=str)[:1500])
            for k in ("degraded", "notes", "missing", "errors", "status", "source"):
                if isinstance(body, dict) and k in body:
                    r.log("  %s: %s" % (k, json.dumps(body[k], default=str)[:600]))
        except Exception as ex:  # noqa: BLE001
            r.log("output read: %s" % type(ex).__name__)

        r.kv(invocations_72h=int(tot.get("Invocations", 0)), errors_72h=int(tot.get("Errors", 0)),
             throttles_72h=int(tot.get("Throttles", 0)), max_duration_ms=int(tot.get("Duration", 0)),
             timeout_s=cfg.get("Timeout"), state=cfg.get("State"), log_lines=seen)
        r.ok("read-only; nothing invoked, nothing written")
    sys.exit(0)


if __name__ == "__main__":
    main()
