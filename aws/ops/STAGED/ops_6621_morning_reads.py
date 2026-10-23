"""ops 6621 - READ-ONLY morning reads for 2026-10-23 (pass 3 day 2):
 a. justhodl-dark-pool: the error lines after its 23:31 UTC retime on Oct 22 (FINRA file timing or code?);
 b. justhodl-research-backtest: RESEARCH_DEADLINE runs today (count + newest line);
 c. alarms: justhodl-llm-providers-down (policy A), justhodl-contract-sev1 (+ sev1 count from data/contract-violations.json),
    justhodl-integrity-new-defects (+ D4 count from data/fleet-integrity.json), justhodl-schedule-drift.
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
ALARMS = ["justhodl-llm-providers-down", "justhodl-contract-sev1", "justhodl-integrity-new-defects", "justhodl-schedule-drift"]
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
    with report("ops_6621_morning_reads") as r:
        r.heading("ops 6621 - morning reads: dark-pool after 23:31, research-backtest, the four alarms (read-only)")
        r.section("a. dark-pool")
        st, rows = insights(logs, "/aws/lambda/justhodl-dark-pool", 'fields @timestamp, @message | filter @message like /ERROR|Traceback|Task timed out|Error|429|403|404|FINRA|finra/ | sort @timestamp desc | limit 12', 16, limit=12)
        seen = []
        for x in rows:
            m = x.get("@message", "").replace("\n", " ").replace(" ", " ")[:420]
            if m not in [s for _, s in seen]:
                seen.append((x.get("@timestamp", "")[5:19], m))
        r.log("dark-pool error lines (%s): %s" % (st, json.dumps(seen[:6])[:3000]))
        st1, rows1 = insights(logs, "/aws/lambda/justhodl-dark-pool", 'fields @message | filter @message like /REPORT/ | stats count() as n by bin(1h) | sort @timestamp desc', 16, limit=20)
        r.log("dark-pool runs per hour (%s): %s" % (st1, json.dumps([(x.get("bin(1h)", "")[5:16], int(float(x.get("n", 0)))) for x in rows1])))
        r.section("b. research-backtest")
        st2, rows2 = insights(logs, "/aws/lambda/justhodl-research-backtest", 'fields @timestamp, @message | filter @message like /RESEARCH_DEADLINE|Task timed out|ERROR/ | sort @timestamp desc | limit 10', 14, limit=10)
        r.log("research-backtest error lines in 14 h (%s): %d; newest: %s" % (st2, len(rows2), json.dumps([(x.get("@timestamp", "")[5:19], x.get("@message", "").replace("\n", " ")[:200]) for x in rows2[:3]])[:1200]))
        r.section("c. alarms")
        states = {}
        for a in cw.describe_alarms(AlarmNames=ALARMS).get("MetricAlarms", []):
            states[a["AlarmName"]] = a.get("StateValue")
            r.log("%s: %s since %s - %s" % (a["AlarmName"], a.get("StateValue"), str(a.get("StateUpdatedTimestamp"))[:19], (a.get("StateReason") or "")[:150]))
        cv = json.loads(s3.get_object(Bucket=LIVE, Key="data/contract-violations.json")["Body"].read())
        viol = cv.get("violations") or []
        sev1 = [v for v in viol if v.get("sev") == 1]
        r.log("contract-violations.json generated %s: sev1 %d by class %s; llm_status rows %d; sev2 STALE %d" % (
            cv.get("generated_at"), len(sev1), json.dumps(Counter(v.get("cls") for v in sev1).most_common()), sum(1 for v in sev1 if "llm_status" in (v.get("detail") or "")),
            sum(1 for v in viol if v.get("sev") == 2 and v.get("cls") == "STALE")))
        fi = json.loads(s3.get_object(Bucket=LIVE, Key="data/fleet-integrity.json")["Body"].read())
        new = fi.get("new") or []
        d4 = [x for x in (fi.get("rows") or []) if isinstance(x, dict) and str(x.get("cls") or x.get("defect") or "").startswith("D4")]
        r.log("fleet-integrity.json generated %s v%s: new %d by class %s; D4 rows %d" % (fi.get("generated_at") or fi.get("at"), fi.get("version"), len(new), json.dumps(Counter(str(x).split("|")[0] for x in new).most_common(8)), len(d4)))
        r.kv(alarms=json.dumps(states), sev1=len(sev1), integrity_new=len(new), d4=len(d4), dark_pool_error_lines=len(seen), research_backtest_errors=len(rows2))
        r.ok("read-only; nothing changed")


if __name__ == "__main__":
    main()
