"""ops 4573 — v2 liveness diagnosis (READ-ONLY). 4572's window closed
before v2's single end-of-run state write could land (780s budget +
stale v1 lease). Settle the question: state NOW, ledger existence, CW
invocations+errors 45m, and the newest log tail."""
import json
import sys
import time
from datetime import datetime, timezone, timedelta

import boto3

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"

s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

R = {"ops": 4573, "at": datetime.now(timezone.utc).isoformat()}

try:
    with report("4573_v2_liveness_diag") as r:
        r.heading("ops 4573 — v2 liveness diagnosis")
        now = datetime.now(timezone.utc)

        st = json.loads(s3.get_object(
            Bucket=B, Key="data/_state/fred-scoped-import.json"
        )["Body"].read())
        R["state"] = {k: st.get(k) for k in
                      ("phase2", "queue_total", "queue_cursor",
                       "rate_rpm", "series_imported",
                       "last_pop_drained", "next_popularity",
                       "throttled_429", "status", "blocked_at",
                       "updated_at", "lease_until")}
        R["state"]["cats_done_n"] = len(st.get("cats_done") or [])
        R["state"]["lease_in_s"] = round(
            (st.get("lease_until") or 0) - time.time(), 1)
        r.kv(**{k: str(v) for k, v in R["state"].items()})

        try:
            h = s3.head_object(Bucket=B,
                               Key="data/_state/fred-queue.json.gz")
            R["queue_obj"] = {"bytes": h["ContentLength"],
                              "age_min": round(
                                  (now - h["LastModified"])
                                  .total_seconds() / 60, 1)}
            r.ok(f"queue ledger exists: {R['queue_obj']}")
        except Exception:
            R["queue_obj"] = None
            r.warn("queue ledger object absent")

        def msum(metric):
            pts = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=now - timedelta(minutes=45), EndTime=now,
                Period=300, Statistics=["Sum"]).get("Datapoints", [])
            return int(sum(p["Sum"] for p in pts))
        R["cw_45m"] = {"invocations": msum("Invocations"),
                       "errors": msum("Errors"),
                       "throttles": msum("Throttles")}
        r.kv(**R["cw_45m"])

        tail = []
        try:
            sts = logs.describe_log_streams(
                logGroupName=f"/aws/lambda/{FN}",
                orderBy="LastEventTime", descending=True, limit=2
            ).get("logStreams", [])
            for stm in sts:
                ev = logs.get_log_events(
                    logGroupName=f"/aws/lambda/{FN}",
                    logStreamName=stm["logStreamName"],
                    limit=40, startFromHead=False).get("events", [])
                for e2 in ev:
                    m = (e2.get("message") or "").strip()
                    if any(x in m for x in
                           ("Error", "error", "Traceback", "REPORT",
                            "Task timed out", "categories_done",
                            "skipped")):
                        tail.append(m[:220])
        except Exception as e:
            tail.append(f"log read: {type(e).__name__}")
        R["log_tail"] = tail[-25:]
        r.section("log tail (filtered)")
        for m in R["log_tail"]:
            r.log(m)

        verdict = "UNKNOWN"
        if R["state"].get("phase2"):
            verdict = "V2_WRITING"
        elif R["cw_45m"]["errors"] > 0 or any(
                "Traceback" in m or "Task timed out" in m
                for m in tail):
            verdict = "V2_CRASHING"
        elif R["state"]["lease_in_s"] > 0:
            verdict = "RUN_IN_FLIGHT_LEASE_HELD"
        R["verdict"] = verdict
        r.ok(f"verdict: {verdict}")
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1200:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4573.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4573.md", "w").write(
        "# 4573 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4573.json", "w"), indent=1,
          default=str)
open("aws/ops/reports/4573.md", "w").write(
    "# 4573 — " + R["verdict"] + "\n- state: " +
    json.dumps(R["state"], default=str) + "\n- cw: " +
    json.dumps(R["cw_45m"]) + "\n- queue: " +
    json.dumps(R.get("queue_obj")) + "\n- tail: " +
    json.dumps(R["log_tail"][-8:], default=str)[:900] + "\n")
print(R["verdict"])
sys.exit(0)
