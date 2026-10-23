"""ops 6616 - READ-ONLY evening reads for 2026-10-22 (the pass-3 evening check):
 a. futures-curves after the fix: every request record written after 16:30 UTC (status / phase / published / quality /
    provider_requests_this_execution / error) and the public head data/futures-research.json (generated_at, quality) -
    three paced runs overlapped at 16:35 (lane tick), 16:40 (lane tick) and 16:42 (ops 6614's invoke);
 b. justhodl-research-backtest: the error lines after its 11:02 UTC retime; justhodl-estimate-revisions: whether its
    errors are still the Benzinga 401;
 c. justhodl-llm-providers-down after the policy-A re-point (ops 6615): state and reason;
 d. data/contract-violations.json: sev1 by class right now.
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
REQ_PREFIX = "audit-private/20260909-originals/futures-research/requests/"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def insights(logs, group, query, hours, limit=30):
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
    with report("ops_6616_evening_reads") as r:
        r.heading("ops 6616 - evening reads: futures-curves after the overlap, research-backtest / estimate-revisions errors, the LLM alarm, sev1 (read-only)")
        r.section("a. futures-curves")
        since = datetime(2026, 10, 22, 16, 30, tzinfo=timezone.utc)
        res = s3.list_objects_v2(Bucket=LIVE, Prefix=REQ_PREFIX, MaxKeys=1000)
        recs = []
        for o in res.get("Contents", []):
            if o["Key"].count("/") != REQ_PREFIX.count("/") or o["LastModified"] < since:
                continue
            try:
                d = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
            except Exception as e:  # noqa: BLE001
                recs.append({"key": o["Key"][-20:], "err": type(e).__name__})
                continue
            recs.append({"written": str(o["LastModified"])[11:19], "request_id": (d.get("request_id") or "")[:28], "status": d.get("status"), "phase": d.get("phase"),
                         "published": d.get("published"), "error": d.get("error"), "failure_class": d.get("failure_class"), "started": (d.get("started_at") or "")[11:19],
                         "completed": (d.get("completed_at") or "")[11:19], "requests": d.get("provider_requests_this_execution"), "quality": d.get("quality")})
        recs.sort(key=lambda x: x.get("started") or "")
        for x in recs:
            r.log(json.dumps(x)[:500])
        head = json.loads(s3.get_object(Bucket=LIVE, Key="data/futures-research.json")["Body"].read())
        r.log("public head: contract %s generated_at %s quality %s products %s" % (head.get("contract"), (head.get("generated_at") or "")[:19], json.dumps(head.get("quality")), len(head.get("products") or {})))
        statuses = Counter(x.get("status") for x in recs)
        r.kv(futures_records_since_1630=len(recs), futures_statuses=json.dumps(statuses), futures_head_generated=(head.get("generated_at") or "")[:19])

        r.section("b. research-backtest and estimate-revisions errors")
        st, rows = insights(logs, "/aws/lambda/justhodl-research-backtest", 'fields @timestamp, @message | filter @message like /ERROR|Traceback|Task timed out|Error/ | sort @timestamp desc | limit 8', 14, limit=8)
        msgs = []
        for x in rows:
            m = x.get("@message", "").replace("\n", " ")[:260]
            if m not in msgs:
                msgs.append((x.get("@timestamp", "")[11:19], m))
        r.log("research-backtest errors (%s): %s" % (st, json.dumps(msgs[:4])[:1800]))
        st2, rows2 = insights(logs, "/aws/lambda/justhodl-estimate-revisions", 'fields @timestamp, @message | filter @message like /401|Unauthorized|ERROR|Traceback|Task timed out/ | sort @timestamp desc | limit 6', 14, limit=6)
        msgs2 = []
        for x in rows2:
            m = x.get("@message", "").replace("\n", " ")[:220]
            if m not in msgs2:
                msgs2.append((x.get("@timestamp", "")[11:19], m))
        r.log("estimate-revisions errors (%s): %s" % (st2, json.dumps(msgs2[:3])[:1200]))

        r.section("c. the LLM alarm after policy A")
        al = cw.describe_alarms(AlarmNames=["justhodl-llm-providers-down"]).get("MetricAlarms", [])
        for a in al:
            r.log("%s: %s since %s - %s | period %s eval %s datapoints %s missing %s" % (a["AlarmName"], a.get("StateValue"), str(a.get("StateUpdatedTimestamp"))[:19], (a.get("StateReason") or "")[:160],
                                                                                        a.get("Period"), a.get("EvaluationPeriods"), a.get("DatapointsToAlarm"), a.get("TreatMissingData")))
        r.section("d. sev1 now")
        cv = json.loads(s3.get_object(Bucket=LIVE, Key="data/contract-violations.json")["Body"].read())
        viol = cv.get("violations") or []
        sev1 = [v for v in viol if v.get("sev") == 1]
        r.log("contract-violations.json generated %s: sev1 %d by class %s; sev2 STALE %d" % (cv.get("generated_at"), len(sev1), json.dumps(Counter(v.get("cls") for v in sev1).most_common()), sum(1 for v in viol if v.get("sev") == 2 and v.get("cls") == "STALE")))
        r.kv(llm_alarm=(al[0].get("StateValue") if al else None), sev1=len(sev1), research_backtest_errors=len(msgs), estimate_revisions_errors=len(msgs2))
        r.ok("read-only; nothing changed")


if __name__ == "__main__":
    main()
