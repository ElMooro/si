"""ops 6591 - READ-ONLY triage of this morning's alerts (Khalid's screenshots, 2026-10-21 08:17 CT).

1. ANOMALY hold on justhodl-wl-series-api (4,894 invocations in an hour; the chart-pro / why.html watchlist-series
   Function URL): the governor's ledger entry and the anomaly record; invocations per 5 min for the last 8 h; the
   `[diag]` client-telemetry lines in its log (page + user agent, one per browser session - a crawler without
   session storage sends one per page) -> who is hammering it.
2. CloudWatch alarms justhodl-contract-sev1 (46), justhodl-schedule-drift (21), justhodl-integrity-new-defects (263),
   justhodl-llm-providers-down: state + reason, and the detail files behind them (data/contract-violations.json,
   data/schedule-drift.json, data/fleet-integrity.json) summarised - which are freeze aftermath, which are mine.
3. The intraday lane's first session: justhodl-scheduler invocations per 5 min since 13:00 UTC.
Nothing is changed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import time
from collections import Counter

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
API = "justhodl-wl-series-api"
ALARMS = ["justhodl-contract-sev1", "justhodl-schedule-drift", "justhodl-integrity-new-defects", "justhodl-llm-providers-down"]


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for _ in range(6):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            return json.loads(r["Payload"].read() or b"{}")
        except lam.exceptions.TooManyRequestsException:
            time.sleep(15)
    return {"error": "busy"}


def per5(cw, fn, metric, hours):
    now = datetime.now(timezone.utc)
    res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                   StartTime=now - timedelta(hours=hours), EndTime=now, Period=300, Statistics=["Sum"])
    pts = sorted((p["Timestamp"], p["Sum"]) for p in res.get("Datapoints", []))
    return [(t.strftime("%H:%M"), int(v)) for t, v in pts if v]


def insights(logs, group, query, hours):
    now = datetime.now(timezone.utc)
    qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=200)["queryId"]
    for _ in range(40):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def s3_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_err": type(e).__name__}


def main():
    cw, logs, s3 = c("cloudwatch"), c("logs"), c("s3")
    with report("ops_6591_alarm_triage") as r:
        r.heading("ops 6591 - alert triage (read-only): the wl-series-api anomaly hold and the four CloudWatch alarms")
        r.section("1. justhodl-wl-series-api")
        st = gov({"action": "status"})
        entry = (st.get("ledger") or {}).get("lambda:" + API) or {}
        an = st.get("anomalies") or {}
        r.log("governor: stage %s; ledger entry for the API: %s" % (st.get("stage"), json.dumps(entry)[:700]))
        r.log("anomaly record: suspects %s; last found %s" % (json.dumps(an.get("suspects"))[:300], json.dumps((an.get("last") or {}).get("found"))[:600]))
        inv = per5(cw, API, "Invocations", 8)
        r.log("invocations per 5 min (UTC, last 8 h): %s" % json.dumps(inv))
        r.log("throttles per 5 min: %s; errors: %s" % (json.dumps(per5(cw, API, "Throttles", 8)), json.dumps(per5(cw, API, "Errors", 8))))
        status, rows = insights(logs, "/aws/lambda/" + API,
                                'fields @message | filter @message like "[diag]" | parse @message /page=(?<page>[^&]*)&favs=(?<favs>[^&]*)&sw=(?<sw>[^&]*).*ua=(?<ua>.*)$/ | stats count() as n by ua, page | sort n desc | limit 60', 6)
        r.log("diag lines (last 6 h) query %s: %d rows" % (status, len(rows)))
        ua_c, page_c = Counter(), Counter()
        for row in rows:
            n = int(float(row.get("n", 0) or 0))
            ua_c[(row.get("ua") or "?")[:90]] += n
            page_c[(row.get("page") or "?")[:60]] += n
        r.log("by user agent: %s" % json.dumps(ua_c.most_common(8)))
        r.log("by page: %s" % json.dumps(page_c.most_common(12)))
        status2, rows2 = insights(logs, "/aws/lambda/" + API,
                                  'fields @message | filter @message like "REPORT" | stats count() as n, avg(@duration) as ms by bin(30m) | sort @timestamp asc', 8)
        r.log("REPORT lines per 30 min (last 8 h) %s: %s" % (status2, json.dumps([(x.get("bin(30m)", "")[11:16], int(float(x.get("n", 0))), round(float(x.get("ms", 0) or 0))) for x in rows2])))
        status3, rows3 = insights(logs, "/aws/lambda/" + API,
                                  'fields @message | filter @message like "[diag]" | stats count() as n by bin(30m) | sort @timestamp asc', 8)
        r.log("diag lines per 30 min %s: %s" % (status3, json.dumps([(x.get("bin(30m)", "")[11:16], int(float(x.get("n", 0)))) for x in rows3])))
        r.section("2. the four alarms")
        al = cw.describe_alarms(AlarmNames=ALARMS).get("MetricAlarms", [])
        for a in al:
            r.log("%s: %s since %s - %s | metric %s/%s %s %s" % (a["AlarmName"], a["StateValue"], a["StateUpdatedTimestamp"].strftime("%m-%d %H:%M"), (a.get("StateReason") or "")[:140],
                                                                 a.get("Namespace"), a.get("MetricName"), a.get("ComparisonOperator"), a.get("Threshold")))
        cv = s3_json(s3, "data/contract-violations.json")
        viol = cv.get("violations") or []
        sev1 = [v for v in viol if v.get("sev") == 1]
        r.log("contract-violations.json generated %s: %d violations, sev1 %d, by class %s" % (cv.get("generated_at"), cv.get("n_violations"), cv.get("sev1"), json.dumps(cv.get("by_class"))[:300]))
        ages = []
        for v in sev1[:60]:
            m = re.search(r"(\d+)h old, bound is (\d+)h", v.get("detail") or "")
            ages.append((v.get("cls"), v.get("artifact"), v.get("detail")[:80], int(m.group(1)) if m else None))
        r.log("sev1 (first 60): %s" % json.dumps(ages))
        stale_ages = [a[3] for a in ages if a[3] is not None]
        r.log("sev1 STALE ages: %d with an age; >= 96 h (frozen since Oct 17): %d; < 96 h: %d" % (len(stale_ages), sum(1 for x in stale_ages if x >= 96), sum(1 for x in stale_ages if x < 96)))
        sd = s3_json(s3, "data/schedule-drift.json")
        drifts = sd.get("drifts") or sd.get("findings") or []
        r.log("schedule-drift.json generated %s mode %s: %s by class %s" % (sd.get("generated_at") or sd.get("at"), sd.get("mode"), sd.get("drift_count"), json.dumps(sd.get("by_class"))[:300]))
        r.log("drifts (first 40): %s" % json.dumps([(d.get("cls") or d.get("class"), d.get("name") or d.get("rule"), (d.get("detail") or d.get("live") or "")[:80] if isinstance(d.get("detail") or d.get("live") or "", str) else "") for d in drifts[:40]])[:3000])
        fi = s3_json(s3, "data/fleet-integrity.json")
        r.log("fleet-integrity.json keys %s; generated %s; new defects %s; summary %s" % (sorted(fi.keys())[:20], fi.get("generated_at"), fi.get("new_defects") or fi.get("n_new"), json.dumps({k: v for k, v in fi.items() if isinstance(v, (int, float, str))})[:600]))
        defects = fi.get("defects") or fi.get("new") or fi.get("items") or []
        if isinstance(defects, list):
            r.log("defects by class: %s; sample %s" % (json.dumps(Counter((d.get("cls") or d.get("class") or d.get("type") or "?") for d in defects if isinstance(d, dict)).most_common(10)), json.dumps(defects[:8])[:1200]))
        r.section("3. the intraday lane - first session")
        r.log("justhodl-scheduler invocations per 5 min since 12:30 UTC: %s" % json.dumps([x for x in per5(cw, "justhodl-scheduler", "Invocations", 3)]))
        for fn in ("justhodl-market-map", "justhodl-options-gamma", "justhodl-intraday-pulse", "justhodl-trade-ticket-monitor"):
            r.log("%s invocations per 5 min: %s; errors: %s" % (fn, json.dumps(per5(cw, fn, "Invocations", 2)), json.dumps(per5(cw, fn, "Errors", 2))))
        r.kv(api_hold=entry.get("stage"), api_release_after=entry.get("release_after"), api_invocations_last_8h=sum(v for _, v in inv), diag_rows=len(rows),
             contract_sev1=cv.get("sev1"), schedule_drift=sd.get("drift_count"), alarms_in_alarm=sum(1 for a in al if a["StateValue"] == "ALARM"))
        r.ok("triage read")


if __name__ == "__main__":
    main()
