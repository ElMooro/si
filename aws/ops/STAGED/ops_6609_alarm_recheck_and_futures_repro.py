"""ops 6609 - READ-ONLY: the four CloudWatch alarms after a full post-thaw day, and the futures-curves failure reproduced.

Self-scheduled follow-up of ops 6591/6592 (Khalid's 2026-10-21 08:17 CT screenshots) for 2026-10-22 13:20 UTC:
 1. describe_alarms for justhodl-contract-sev1, justhodl-schedule-drift, justhodl-integrity-new-defects,
    justhodl-llm-providers-down - state, reason, last transition; the detail files behind them summarised
    (data/contract-violations.json sev1 by class + STALE sev2; data/schedule-drift.json UNDECLARED - expected 0 after
    ops 6594 and the pass-3 declarations of ops 6605; data/fleet-integrity.json new defects by class, the timeout-clipped
    engines and the orphan rules named).
 2. justhodl-wl-series-api: still released (governor status anomaly_held / holds record) and its invocations/errors
    over the last 24 h.
 3. justhodl-polygon-futures-curves (issue #122): the newest full [ERROR] line from CloudWatch Logs, then the failure
    reproduced OUTSIDE the engine - the same 21 initial catalog requests the engine makes (products / contracts /
    schedules x 7 products, built by aws/shared/futures_source_capture.spec/initial, same headers, no redirects), one at
    a time, recording status, body length, content type, Location and the first bytes of the body with the credential
    scrubbed - nothing retained, nothing written, nothing invoked.  The hypothesis from reading the code: a response
    whose BODY IS EMPTY passes capture.request (bounded() only rejects oversize bodies) and then fails in
    futures_research_store.immutable, whose precondition is 0 < len(raw) -> ValueError('Exact bounded futures artifact
    write required'); that ValueError escapes collect_originals and the run records failure_class=ValueError with no
    dataset checkpoint for the offending scope - matching the 5 checkpoints of 21 seen by ops 6607.
Nothing is changed; the engine is not invoked; no key is printed (every string is scrubbed before it is logged).
"""
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
ALARMS = ["justhodl-contract-sev1", "justhodl-schedule-drift", "justhodl-integrity-new-defects", "justhodl-llm-providers-down"]
API = "justhodl-wl-series-api"
FC = "justhodl-polygon-futures-curves"


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def s3_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_err": type(e).__name__}


def gov(payload):
    lam = c("lambda")
    for _ in range(6):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            return json.loads(r["Payload"].read() or b"{}")
        except lam.exceptions.TooManyRequestsException:
            time.sleep(15)
    return {"error": "busy"}


def insights(logs, group, query, hours, limit=50):
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


def metric_sum(cw, fn, metric, hours):
    now = datetime.now(timezone.utc)
    res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                   StartTime=now - timedelta(hours=hours), EndTime=now, Period=3600, Statistics=["Sum"])
    return int(sum(p["Sum"] for p in res.get("Datapoints", [])))


def main():
    s3, cw, logs = c("s3"), c("cloudwatch"), c("logs")
    with report("ops_6609_alarm_recheck_and_futures_repro") as r:
        r.heading("ops 6609 - alarms after a full post-thaw day; wl-series-api; futures-curves failure reproduced (read-only)")
        r.section("1. the four alarms")
        al = cw.describe_alarms(AlarmNames=ALARMS).get("MetricAlarms", [])
        states = {}
        for a in al:
            states[a["AlarmName"]] = a.get("StateValue")
            r.log("%s: %s since %s - %s" % (a["AlarmName"], a.get("StateValue"), str(a.get("StateUpdatedTimestamp"))[:19], (a.get("StateReason") or "")[:220]))
        for name in ALARMS:
            if name not in states:
                r.warn("%s: not found" % name)
        cv = s3_json(s3, "data/contract-violations.json")
        viol = cv.get("violations") or []
        sev1 = [v for v in viol if v.get("sev") == 1]
        sev2_stale = [v for v in viol if v.get("sev") == 2 and (v.get("cls") or "") == "STALE"]
        r.log("contract-violations.json generated %s: %d violations, sev1 %d by class %s; sev2 STALE %d" % (
            cv.get("generated_at"), cv.get("n_violations") or len(viol), len(sev1), json.dumps(Counter(v.get("cls") for v in sev1).most_common()), len(sev2_stale)))
        r.log("sev1 (first 24): %s" % json.dumps([(v.get("cls"), v.get("artifact"), (v.get("detail") or "")[:70]) for v in sev1[:24]])[:3000])
        sd = s3_json(s3, "data/schedule-drift.json")
        drifts = sd.get("drifts") or sd.get("findings") or []
        und = [d for d in drifts if (d.get("drift") or d.get("cls") or d.get("class")) == "UNDECLARED"]
        r.log("schedule-drift.json generated %s mode %s: %s drifts, by class %s; UNDECLARED %d: %s" % (
            sd.get("generated_at") or sd.get("at"), sd.get("mode"), sd.get("drift_count") or len(drifts), json.dumps(sd.get("by_class"))[:300], len(und),
            json.dumps([d.get("key") or d.get("name") for d in und[:30]])[:1500]))
        fi = s3_json(s3, "data/fleet-integrity.json")
        new = fi.get("new") or []
        cls = Counter(str(x).split("|")[0] for x in new)
        r.log("fleet-integrity.json generated %s: new %d by class %s" % (fi.get("generated_at") or fi.get("at"), len(new), json.dumps(cls.most_common(12))))
        rows = [x for x in (fi.get("rows") or []) if isinstance(x, dict)]
        clipped = sorted({x.get("fn") or x.get("function") or x.get("name") for x in rows if "D2" in str(x.get("cls") or x.get("defect") or "")})
        orphan = sorted({x.get("fn") or x.get("function") or x.get("name") or x.get("detail") for x in rows if "D4" in str(x.get("cls") or x.get("defect") or "")})
        r.log("D2 timeout-clipped: %s" % json.dumps([x for x in clipped if x][:12]))
        r.log("D4 orphan rules (%d): %s" % (len(orphan), json.dumps([x for x in orphan if x][:24])[:2000]))
        r.log("new (first 30): %s" % json.dumps([str(x)[:90] for x in new[:30]])[:3000])

        r.section("2. wl-series-api")
        st = gov({"action": "status"})
        r.log("governor %s stage %s; anomaly_held %s; CAP held %s; anomalies record %s" % (
            st.get("version"), st.get("stage"), json.dumps(st.get("anomaly_held")), json.dumps(sorted((st.get("held") or {}).keys()) if isinstance(st.get("held"), dict) else st.get("held"))[:300], json.dumps(st.get("anomalies"))[:500]))
        inv24, err24 = metric_sum(cw, API, "Invocations", 24), metric_sum(cw, API, "Errors", 24)
        r.log("%s last 24 h: %d invocations, %d errors" % (API, inv24, err24))
        status_q, rows_q = insights(logs, "/aws/lambda/" + API, 'fields @message | filter @message like "[diag]" | stats count() as n', 24, limit=5)
        r.log("diag beacon lines in 24 h (%s): %s" % (status_q, json.dumps(rows_q)))
        released = (API not in (st.get("anomaly_held") or [])) and (API not in (st.get("held") or {}))
        r.kv(api_released=released, api_invocations_24h=inv24, api_errors_24h=err24)

        r.section("3. futures-curves: the full error line, then the reproduction")
        inv_fc, err_fc = metric_sum(cw, FC, "Invocations", 24), metric_sum(cw, FC, "Errors", 24)
        r.log("%s last 24 h: %d invocations, %d errors" % (FC, inv_fc, err_fc))
        status_e, rows_e = insights(logs, "/aws/lambda/" + FC, 'fields @timestamp, @message | filter @message like "[ERROR]" | sort @timestamp desc | limit 1', 24, limit=1)
        if rows_e:
            msg = rows_e[0].get("@message", "").replace(" ", " ")
            r.log("newest [ERROR] (%s, %s): %s" % (status_e, rows_e[0].get("@timestamp", "")[:19], msg[:3500]))
        else:
            r.log("no [ERROR] line in 24 h (%s)" % status_e)
        import futures_source_capture as capture  # noqa: E402
        from option_snapshot_capture import NoRedirect  # noqa: E402
        from massive import get_massive_key  # noqa: E402
        secret = get_massive_key()
        if not secret:
            r.fail("no Massive credential readable on the runner - reproduction skipped")
            raise SystemExit(1)
        q = urllib.parse.quote(secret, safe="")

        def scrub(s):
            s = str(s)
            for needle in (secret, q):
                if needle:
                    s = s.replace(needle, "<key>")
            return re.sub(r"apiKey=[^&\s\"']+", "apiKey=<key>", s)

        end = datetime.now(ZoneInfo("America/New_York")).date()
        start = end - timedelta(days=90)
        scopes = [capture.spec(kind, product, start.isoformat(), end.isoformat()) for product in capture.PRODUCTS for kind in ("products", "contracts", "schedules")]
        opener = urllib.request.build_opener(NoRedirect())
        out = []
        for scope in scopes:
            name = "%s:%s" % (scope["product"], scope["kind"])
            try:
                url = capture.initial(scope)
            except ValueError as e:
                out.append({"scope": name, "stage": "initial", "error": scrub(e)})
                continue
            req = urllib.request.Request(url + "&apiKey=" + q, headers={"User-Agent": "JustHodl-Futures-Original-Research/1.0", "Accept": "application/json", "Accept-Encoding": "identity"})
            rec = {"scope": name, "url": scrub(url)[:140]}
            try:
                resp = opener.open(req, timeout=15)
                status, raw, headers = resp.status, resp.read(capture.MAX_PAGE_BYTES + 1), resp.headers
            except urllib.error.HTTPError as exc:
                status, raw, headers = exc.code, exc.read(capture.MAX_PAGE_BYTES + 1), exc.headers
            except Exception as e:  # noqa: BLE001
                out.append(dict(rec, stage="transport", error=type(e).__name__))
                time.sleep(0.4)
                continue
            rec.update(status=status, body_bytes=len(raw), content_type=(headers.get("Content-Type") or "")[:60], location=scrub(headers.get("Location") or "")[:160] or None,
                       body_head=scrub(raw[:160].decode("utf-8", "replace")))
            if len(raw) == 0:
                rec["engine_path"] = "capture.request accepts it (bounded() only rejects oversize) -> protect() -> immutable() precondition 0 < len(raw) fails -> ValueError('Exact bounded futures artifact write required') -> run fails in collect_originals, no checkpoint"
            else:
                try:
                    doc = capture.envelope(raw)
                    rec["envelope"] = {"results": len(doc.get("results") or []), "next_url": bool(doc.get("next_url")), "status": doc.get("status")}
                    if scope["kind"] == "contracts":
                        sel = capture.select_contracts(doc.get("results") or [], scope["product"], end.isoformat(), not doc.get("next_url"))
                        rec["selection"] = {k: sel[k] for k in ("selection_qualified", "qualified_contracts", "returned_rows", "rejected", "duplicate_tickers")}
                        rec["selected"] = [x["ticker"] for x in sel["selected"]]
                except (ValueError, UnicodeDecodeError) as e:
                    rec["envelope"] = "invalid: %s" % scrub(e)
            out.append(rec)
            time.sleep(0.4)
        for rec in out:
            r.log(json.dumps(rec)[:900])
        empties = [x["scope"] for x in out if x.get("body_bytes") == 0]
        non200 = [(x["scope"], x.get("status")) for x in out if x.get("status") not in (200, None)]
        transport = [x["scope"] for x in out if x.get("stage") == "transport"]
        r.kv(alarms=json.dumps(states), sev1=len(sev1), sev2_stale=len(sev2_stale), undeclared=len(und), integrity_new=len(new),
             fc_invocations_24h=inv_fc, fc_errors_24h=err_fc, scopes_probed=len(out), empty_body_scopes=json.dumps(empties), non_200=json.dumps(non200), transport_failures=json.dumps(transport))
        if empties:
            r.warn("reproduced: %d of 21 initial requests come back with an EMPTY body (%s) - that is the ValueError the engine hides" % (len(empties), ", ".join(empties)))
        elif non200:
            r.warn("no empty body, but non-200 answers: %s - the engine would retain those and continue; look at the newest [ERROR] line above" % json.dumps(non200))
        else:
            r.log("all 21 initial requests answered 200 with a body - the failure is not in the initial captures today (or not deterministic)")
        r.ok("read-only; nothing written; the only invocation is the governor's status read; no key printed")


if __name__ == "__main__":
    main()
