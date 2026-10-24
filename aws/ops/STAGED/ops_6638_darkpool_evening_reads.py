"""ops 6638 - READ-ONLY evening reads for 2026-10-23 (pass 3 day 2, post-close):
 a. justhodl-dark-pool at its new 23:31 UTC minute: today's and yesterday's request journals under
    audit-private/20260909-originals/offexchange-research/requests/ (paginated), with status / error_type / the planned
    sources that failed (`errors` {name: exception}) / the daily CNMS candidates tried and their HTTP statuses, so the
    failure is named (FINRA daily file timing vs partition vs code); the public head data/dark-pool.json (generated_at,
    daily date); the CloudWatch error lines since 23:00 UTC today;
 b. the four alarms and today's sev1 count.
Nothing is changed; nothing is invoked.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
REQ_PREFIX = "audit-private/20260909-originals/offexchange-research/requests/"
ALARMS = ["justhodl-llm-providers-down", "justhodl-contract-sev1", "justhodl-integrity-new-defects", "justhodl-schedule-drift"]
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def insights(logs, group, query, hours, limit=20):
    now = datetime.now(timezone.utc)
    try:
        qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=limit)["queryId"]
    except Exception as e:  # noqa: BLE001
        return "start:%s" % type(e).__name__, []
    for _ in range(45):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def main():
    s3, logs, cw = c("s3"), c("logs"), c("cloudwatch")
    since = datetime(2026, 10, 22, 23, 0, tzinfo=timezone.utc)
    with report("ops_6638_darkpool_evening_reads") as r:
        r.heading("ops 6638 - dark-pool at 23:31 (Oct 22 and Oct 23), alarms, sev1 (read-only)")
        r.section("a. dark-pool request journals since Oct 22 23:00 UTC")
        token, tops = {}, []
        while True:
            res = s3.list_objects_v2(Bucket=LIVE, Prefix=REQ_PREFIX, MaxKeys=1000, **token)
            for o in res.get("Contents", []):
                if o["Key"].count("/") == REQ_PREFIX.count("/") and o["LastModified"] >= since:
                    tops.append(o)
            if not res.get("IsTruncated"):
                break
            token = {"ContinuationToken": res["NextContinuationToken"]}
        tops.sort(key=lambda o: o["LastModified"])
        r.log("journals modified since Oct 22 23:00 UTC: %d" % len(tops))
        for o in tops:
            d = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
            if d.get("source_id") or d.get("partition") is not None:
                # a per-source capture record (fetch / partition journal): one compact line each, failures spelled out
                cap = d.get("capture") or {}
                spec = d.get("specification") or {}
                if d.get("status") == "failed" or (cap and (cap.get("http_status") != 200 or cap.get("status") != "response_retained")):
                    r.log("   SOURCE %s %s: status %s error %s http %s capture %s url %s" % (str(o["LastModified"])[5:19], d.get("source_id") or json.dumps(d.get("partition"))[:80], d.get("status"), d.get("error_type"),
                                                                                              cap.get("http_status"), cap.get("status"), (spec.get("url") or cap.get("url") or "")[:140]))
                continue
            sel = d.get("selection") or {}
            daily = d.get("daily")
            daily_tries = []
            if isinstance(daily, dict):
                for a in daily.get("daily_candidates") or []:
                    daily_tries.append((a.get("name") or a.get("label") or "", a.get("http_status"), a.get("status")))
            rec = {"written": str(o["LastModified"])[5:19], "request_id": (d.get("request_id") or "")[:30], "status": d.get("status"), "error_type": d.get("error_type"),
                   "errors": d.get("errors"), "started": (d.get("started_at") or "")[11:19], "daily_candidates": sel.get("daily_candidate_dates"),
                   "partitions_planned": len(sel.get("partitions") or []), "partitions_captured": len(d.get("partitions") or {}),
                   "daily_got": (daily or {}).get("daily_date") if isinstance(daily, dict) else daily, "result": d.get("result")}
            r.log("JOURNAL " + json.dumps(rec, default=str)[:1400])
            # the per-source capture records of a failed run sit beside the journal: list what the daily fetches answered
            if d.get("status") == "failed":
                sub = s3.list_objects_v2(Bucket=LIVE, Prefix=o["Key"][:-5] + "/", MaxKeys=200).get("Contents", [])
                names = Counter()
                for x in sub:
                    try:
                        v = json.loads(s3.get_object(Bucket=LIVE, Key=x["Key"])["Body"].read())
                        names[("%s %s %s" % (v.get("name") or x["Key"].rsplit("/", 1)[-1][:24], v.get("http_status"), v.get("status")))] += 1
                    except Exception:  # noqa: BLE001
                        names[x["Key"].rsplit("/", 1)[-1][:30] + " unreadable"] += 1
                r.log("   capture records: %s" % json.dumps(names.most_common(20))[:1200])
        head = json.loads(s3.get_object(Bucket=LIVE, Key="data/dark-pool.json")["Body"].read())
        r.log("public head: generated_at %s, daily date %s, counts %s" % ((head.get("generated_at") or "")[:19], (head.get("daily") or {}).get("date"), json.dumps(head.get("counts"))[:200]))
        st, rows = insights(logs, "/aws/lambda/justhodl-dark-pool", 'fields @timestamp, @message | filter @message like /ERROR|Traceback|Task timed out/ | sort @timestamp desc | limit 8', 2, limit=8)
        r.log("CloudWatch error lines since 22:00 UTC today (%s): %s" % (st, json.dumps([(x.get("@timestamp", "")[11:19], x.get("@message", "").replace("\n", " ")[:160]) for x in rows])[:1500]))
        r.section("b. alarms and sev1")
        states = {}
        for a in cw.describe_alarms(AlarmNames=ALARMS).get("MetricAlarms", []):
            states[a["AlarmName"]] = a.get("StateValue")
        cv = json.loads(s3.get_object(Bucket=LIVE, Key="data/contract-violations.json")["Body"].read())
        sev1 = [v for v in (cv.get("violations") or []) if v.get("sev") == 1]
        r.log("alarms %s; contract-violations generated %s: sev1 %d by class %s" % (json.dumps(states), cv.get("generated_at"), len(sev1), json.dumps(Counter(v.get("cls") for v in sev1).most_common())))
        r.kv(journals=len(tops), head_generated=(head.get("generated_at") or "")[:19], head_daily=(head.get("daily") or {}).get("date"), alarms=json.dumps(states), sev1=len(sev1))
        r.ok("read-only; nothing changed")


if __name__ == "__main__":
    main()
