"""ops 6617 - READ-ONLY: every futures-curves request record of 2026-10-22 after 16:30 UTC, with a PAGINATED listing
(ops 6616 listed one page of 1,000 keys under requests/ - the per-run /datasets/ checkpoints push the prefix past one
page, so it saw one record while the public head shows a later, weaker publication at 16:48:39: 25 complete datasets /
15 contracts against the 16:45:02 run's 42 / 21).  Also: the function's reserved concurrency, its invocations and
errors since 16:30 UTC (per 10 min), and the public head as it stands.  Nothing is changed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-polygon-futures-curves"
REQ_PREFIX = "audit-private/20260909-originals/futures-research/requests/"
SINCE = datetime(2026, 10, 23, 12, 30, tzinfo=timezone.utc)
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def main():
    s3, lam, cw = c("s3"), c("lambda"), c("cloudwatch")
    with report("ops_6617_futures_records_paginated") as r:
        r.heading("ops 6617 - futures-curves request records since 16:30 UTC, paginated (read-only)")
        token, top, n_all = {}, [], 0
        while True:
            res = s3.list_objects_v2(Bucket=LIVE, Prefix=REQ_PREFIX, MaxKeys=1000, **token)
            for o in res.get("Contents", []):
                n_all += 1
                if o["Key"].count("/") == REQ_PREFIX.count("/") and o["LastModified"] >= SINCE:
                    top.append(o)
            if not res.get("IsTruncated"):
                break
            token = {"ContinuationToken": res["NextContinuationToken"]}
        r.log("objects under requests/: %d; top-level records modified since 16:30 UTC: %d" % (n_all, len(top)))
        recs = []
        for o in sorted(top, key=lambda o: o["LastModified"]):
            d = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
            ds = s3.list_objects_v2(Bucket=LIVE, Prefix=o["Key"][:-5] + "/datasets/", MaxKeys=200).get("Contents", [])
            stops = Counter()
            for x in ds:
                try:
                    stops[json.loads(s3.get_object(Bucket=LIVE, Key=x["Key"])["Body"].read()).get("stop")] += 1
                except Exception:  # noqa: BLE001
                    stops["unreadable"] += 1
            rec = {"written": str(o["LastModified"])[11:19], "request_id": (d.get("request_id") or "")[:30], "status": d.get("status"), "phase": d.get("phase"), "published": d.get("published"),
                   "error": d.get("error"), "failure_class": d.get("failure_class"), "started": (d.get("started_at") or "")[11:19], "completed": (d.get("completed_at") or "")[11:19],
                   "requests": d.get("provider_requests_this_execution"), "quality": d.get("quality"), "generated_at": (d.get("generated_at") or "")[11:19], "datasets": len(ds), "stops": dict(stops)}
            recs.append(rec)
            r.log(json.dumps(rec)[:700])
        head = json.loads(s3.get_object(Bucket=LIVE, Key="data/futures-research.json")["Body"].read())
        r.log("public head now: generated_at %s quality %s" % ((head.get("generated_at") or "")[:19], json.dumps(head.get("quality"))))
        try:
            conc = lam.get_function_concurrency(FunctionName=FN).get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            conc = "err:%s" % type(e).__name__
        now = datetime.now(timezone.utc)
        series = {}
        for metric in ("Invocations", "Errors", "Throttles"):
            res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": FN}], StartTime=SINCE, EndTime=now, Period=600, Statistics=["Sum"])
            series[metric] = sorted((p["Timestamp"].strftime("%H:%M"), int(p["Sum"])) for p in res.get("Datapoints", []) if p["Sum"])
        r.log("reserved concurrency %s; since 16:30 per 10 min: %s" % (conc, json.dumps(series)))
        published = [x for x in recs if x.get("published")]
        r.kv(records=len(recs), published=len(published), head_generated=(head.get("generated_at") or "")[:19], head_quality=json.dumps(head.get("quality"))[:160], reserved_concurrency=conc)
        r.ok("read-only; nothing changed")


if __name__ == "__main__":
    main()
