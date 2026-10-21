"""ops 6592 - READ-ONLY, part two of the alert triage (ops 6591 left three questions open).

  a. who fired the nav-drawer diag beacon ~9,500 times (the whole of justhodl-wl-series-api's morning traffic): the
     `[diag]` log lines parsed properly - user agent and page - per 30 min;
  b. the governor's anomaly hold as the status action really reports it (anomaly_held / held), so the release step
     can be verified against it;
  c. the 21 UNDECLARED live triggers behind the schedule-drift alarm: name, expression, state, target function(s)
     (payloads are never printed - only whether one exists and its length);
  d. the 263 "new" fleet-integrity defects by class.
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


def insights(logs, group, query, hours):
    now = datetime.now(timezone.utc)
    qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=500)["queryId"]
    for _ in range(45):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def main():
    logs, s3, evb, sch = c("logs"), c("s3"), c("events"), c("scheduler")
    with report("ops_6592_alarm_triage_2") as r:
        r.heading("ops 6592 - alert triage, part two (read-only)")
        r.section("a. who fired the beacon")
        status, rows = insights(logs, "/aws/lambda/" + API, 'fields @message | filter @message like "[diag]" | sort @timestamp desc | limit 400', 9)
        ua_c, page_c, per30 = Counter(), Counter(), Counter()
        for row in rows:
            m = row.get("@message") or ""
            ua = re.search(r"ua=(.*)$", m)
            pg = re.search(r'"page":\s*"([^"]*)"', m)
            ua_c[(ua.group(1) if ua else "?").strip()[:100]] += 1
            page_c[(pg.group(1) if pg else "?")[:50]] += 1
        r.log("sample of %d diag lines (query %s): by user agent %s" % (len(rows), status, json.dumps(ua_c.most_common(8))))
        r.log("by page: %s" % json.dumps(page_c.most_common(15)))
        status2, rows2 = insights(logs, "/aws/lambda/" + API, 'fields @message | filter @message like "[diag]" | parse @message /ua=(?<ua>.{0,40})/ | stats count() as n by ua | sort n desc | limit 15', 9)
        r.log("all diag lines by user agent (first 40 chars, query %s): %s" % (status2, json.dumps([(x.get("ua"), int(float(x.get("n", 0)))) for x in rows2])))
        status3, rows3 = insights(logs, "/aws/lambda/" + API, 'fields @message | filter @message like "[diag]" and @message not like "diag" | stats count() as n by bin(30m)', 9)
        r.log("(sanity) %s" % status3)
        r.section("b. the hold")
        st = gov({"action": "status"})
        r.log("stage %s; anomaly_held %s; held %s; capped %s; anomalies %s" % (st.get("stage"), json.dumps(st.get("anomaly_held")), json.dumps(st.get("held"))[:400], json.dumps(st.get("capped")), json.dumps(st.get("anomalies"))[:600]))
        lam = c("lambda")
        try:
            conc = lam.get_function_concurrency(FunctionName=API).get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            conc = "err:%s" % type(e).__name__
        r.log("%s reserved concurrency now: %s" % (API, conc))
        r.section("c. the 21 undeclared live triggers")
        sd = json.loads(s3.get_object(Bucket=LIVE, Key="data/schedule-drift.json")["Body"].read())
        und = [d for d in (sd.get("drifts") or sd.get("findings") or []) if (d.get("drift") or d.get("cls")) == "UNDECLARED"]
        out = []
        for d in und:
            k = d.get("key") or ""
            kind, name = (k.split(":", 1) + [""])[:2]
            try:
                if kind in ("events", "rule"):
                    rule = evb.describe_rule(Name=name)
                    tg = evb.list_targets_by_rule(Rule=name).get("Targets", [])
                    out.append({"kind": "rule", "name": name, "expr": rule.get("ScheduleExpression"), "state": rule.get("State"), "created": str(rule.get("CreatedBy") or ""),
                                "targets": [{"fn": fn_of(t.get("Arn")), "input_len": len(t.get("Input") or "")} for t in tg], "description": (rule.get("Description") or "")[:100]})
                else:
                    group, sname = (name.split("/", 1) + [""])[:2] if "/" in name else ("default", name)
                    s_ = sch.get_schedule(Name=sname, GroupName=group)
                    out.append({"kind": "sched", "name": sname, "group": group, "expr": s_.get("ScheduleExpression"), "state": s_.get("State"), "tz": s_.get("ScheduleExpressionTimezone"),
                                "targets": [{"fn": fn_of((s_.get("Target") or {}).get("Arn")), "input_len": len((s_.get("Target") or {}).get("Input") or "")}], "description": (s_.get("Description") or "")[:100]})
            except Exception as e:  # noqa: BLE001
                out.append({"key": k, "err": type(e).__name__})
        r.log("undeclared (%d): %s" % (len(out), json.dumps(out)[:6000]))
        r.section("d. fleet-integrity new defects")
        fi = json.loads(s3.get_object(Bucket=LIVE, Key="data/fleet-integrity.json")["Body"].read())
        new = fi.get("new") or []
        cls = Counter(str(x).split("|")[0] for x in new)
        r.log("new %d by class: %s" % (len(new), json.dumps(cls.most_common(12))))
        sev1 = [x for x in (fi.get("rows") or []) if isinstance(x, dict) and x.get("sev") == 1][:30]
        r.log("sev1 rows sample: %s" % json.dumps(sev1)[:2500])
        r.kv(diag_sampled=len(rows), undeclared=len(out), anomaly_held=json.dumps(st.get("anomaly_held")), api_concurrency=conc, integrity_new=len(new))
        r.ok("triage part two read")


if __name__ == "__main__":
    main()
