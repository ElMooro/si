"""ops 6857 - live proof of justhodl-fleet-error-monitor v1.3.0 (dead letters judged by arrivals and by engine, not by
the queue's depth; ops 6856 forensics, Khalid 2026-11-20 "investigate this and fix it").

  1. wait for the deploy to land - settled BY CONTENT (the deployed zip's lambda_function.py carries VERSION 1.3.0),
     LastUpdateStatus Successful, State Active;
  2. an independent read of what the monitor should find: AsyncEventsDropped per function over the same 25 hours,
     split by reserved concurrency 0;
  3. QUIET invoke ({"mode":"audit_refresh"}: no notification, no history write): the response's dead-letter breakdown
     must match step 2, the held coordinator must be under `held`, never under `failing`;
  4. ONE real invoke: the corrected alert goes out once (SNS e-mail) - a WARNING that names the engines - and the
     alert history starts each engine's weekly window;
  5. a second real invoke: nothing is sent (the standing set is suppressed), the alarm stays visible in the report.
Changes nothing but what the monitor itself writes on a normal run (its report and its alert history).
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
MONITOR = "justhodl-fleet-error-monitor"
HELD_EXPECTED = "justhodl-event-coordinator"
WANT_VERSION = 'VERSION = "1.3.0"'
HOURS = 25
DEPLOY_WAIT_S = 1500


def dropped_by_function(cw, names, hours, now):
    start = (now - timedelta(hours=hours)).replace(minute=0, second=0, microsecond=0)
    out = {}
    for i in range(0, len(names), 500):
        chunk = names[i:i + 500]
        q = [{"Id": "d%d" % j, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "AsyncEventsDropped",
              "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 3600, "Stat": "Sum"}} for j, n in enumerate(chunk)]
        tok = None
        while True:
            kw = {"MetricDataQueries": q, "StartTime": start, "EndTime": now}
            if tok:
                kw["NextToken"] = tok
            res = cw.get_metric_data(**kw)
            for r_ in res["MetricDataResults"]:
                v = sum(r_.get("Values") or [])
                if v > 0:
                    out[chunk[int(r_["Id"][1:])]] = out.get(chunk[int(r_["Id"][1:])], 0) + int(round(v))
            tok = res.get("NextToken")
            if not tok:
                break
    return out


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    once = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=240)   # an invoke is never repeated: one e-mail
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    lam_once = boto3.client("lambda", region_name=REGION, config=once)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    facts, bad = {}, []
    with report("ops_6857_fleet_monitor_dead_letters_proof") as r:
        r.heading("ops 6857 - fleet-error-monitor v1.3.0 live proof (dead letters by arrival and by engine)")

        r.section("1. the deploy has landed (by content)")
        t0, landed, conf = time.time(), False, {}
        while time.time() - t0 < DEPLOY_WAIT_S:
            try:
                fn = lam.get_function(FunctionName=MONITOR)
                conf = fn["Configuration"]
                if conf.get("LastUpdateStatus") == "Successful" and conf.get("State") == "Active":
                    blob = urllib.request.urlopen(fn["Code"]["Location"], timeout=60).read()
                    src = zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode("utf-8", "replace")
                    if WANT_VERSION in src and "def fetch_dead_letters" in src:
                        landed = True
                        names_in_zip = zipfile.ZipFile(io.BytesIO(blob)).namelist()
                        r.log("deployed zip: %d files, public_brain_projection.py bundled: %s" % (len(names_in_zip), "public_brain_projection.py" in names_in_zip))
                        if "public_brain_projection.py" not in names_in_zip:
                            bad.append("the shared module public_brain_projection.py is not in the deployed zip")
                        break
            except Exception as exc:  # noqa: BLE001
                r.log("waiting: %s" % str(exc)[:120])
            time.sleep(20)
        r.log("after %.0f s: landed %s | LastModified %s | LastUpdateStatus %s | State %s | CodeSha256 %s... | timeout %s s mem %s MB" % (
            time.time() - t0, landed, conf.get("LastModified"), conf.get("LastUpdateStatus"), conf.get("State"),
            str(conf.get("CodeSha256"))[:12], conf.get("Timeout"), conf.get("MemorySize")))
        if not landed:
            r.fail("v1.3.0 is not deployed after %d s - nothing invoked" % DEPLOY_WAIT_S)
            sys.exit(1)
        time.sleep(8)   # LastModified is not "deployed" (ops 3019): a short settle after Successful

        r.section("2. independent read: AsyncEventsDropped per function, last %d h" % HOURS)
        names = []
        for page in lam.get_paginator("list_functions").paginate():
            names += [f["FunctionName"] for f in page["Functions"]]
        names = sorted(n for n in names if n != MONITOR)
        now = datetime.now(timezone.utc)
        dropped = dropped_by_function(cw, names, HOURS, now)
        paused = set()
        for n in dropped:
            try:
                if lam.get_function_concurrency(FunctionName=n).get("ReservedConcurrentExecutions") == 0:
                    paused.add(n)
            except Exception:  # noqa: BLE001
                pass
        want_failing = {n: v for n, v in dropped.items() if n not in paused}
        want_held = {n: v for n, v in dropped.items() if n in paused}
        r.log("%d functions scanned; %d dropped async events: %d from %d running engines, %d refused by %d held engines" % (
            len(names), len(dropped), sum(want_failing.values()), len(want_failing), sum(want_held.values()), len(want_held)))
        r.log("held: %s" % json.dumps(want_held, sort_keys=True))
        r.log("failing: %s" % ", ".join("%s %d" % (n.replace("justhodl-", ""), v) for n, v in sorted(want_failing.items(), key=lambda kv: (-kv[1], kv[0]))))

        def invoke(payload, label):
            t = time.time()
            resp = lam_once.invoke(FunctionName=MONITOR, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            raw = json.loads(resp["Payload"].read() or b"{}")
            if resp.get("FunctionError"):
                r.fail("%s: FunctionError %s: %s" % (label, resp.get("FunctionError"), str(raw)[:300]))
                sys.exit(1)
            body = json.loads(raw.get("body") or "{}")
            r.log("%s: status %s in %.0f s | scanned %s | n_alerts %s suppressed %s | dlq_depth %s" % (
                label, raw.get("statusCode"), time.time() - t, body.get("n_scanned"), body.get("n_alerts"), body.get("suppressed"), body.get("dlq_depth")))
            return body

        def read(key):
            obj = s3.get_object(Bucket=LIVE, Key=key)
            return json.loads(obj["Body"].read() or b"{}"), obj["LastModified"]

        r.section("3. quiet invoke (no notification, no history write)")
        hist_before, hist_mod_before = read("data/_fleet-monitor-alert-history.json")
        body = invoke({"mode": "audit_refresh"}, "quiet")
        dl = body.get("dead_letters")
        if dl is None:
            r.fail("the monitor could not read AsyncEventsDropped (dead_letters is null: GetMetricData denied to its role?) - it fell back to the depth rule")
            sys.exit(1)
        got_failing, got_held = dl.get("failing") or {}, dl.get("held") or {}
        r.log("monitor: window %s h | failing %d from %d engines | held %s" % (dl.get("window_hours"), sum(got_failing.values()), len(got_failing), json.dumps(got_held, sort_keys=True)))
        # the two reads are minutes apart at an hour boundary at worst: compare engine sets, tolerate a count moving by a late datapoint
        if set(got_failing) != set(want_failing):
            bad.append("failing engines differ: only monitor %s | only independent %s" % (sorted(set(got_failing) - set(want_failing)), sorted(set(want_failing) - set(got_failing))))
        if set(got_held) != set(want_held):
            bad.append("held engines differ: monitor %s vs independent %s" % (sorted(got_held), sorted(want_held)))
        drift = {n: (got_failing.get(n), want_failing.get(n)) for n in set(got_failing) & set(want_failing) if got_failing[n] != want_failing[n]}
        r.log("per-engine counts that differ between the two reads: %s" % (drift or "none"))
        if HELD_EXPECTED in got_failing:
            bad.append("%s (reserved concurrency 0) was classified as failing" % HELD_EXPECTED)
        if HELD_EXPECTED not in got_held:
            r.warn("%s is not under `held` in this window (released, or no bus events in %d h?)" % (HELD_EXPECTED, HOURS))
        _, hist_mod_after = read("data/_fleet-monitor-alert-history.json")
        if hist_mod_after != hist_mod_before:
            bad.append("the quiet invoke rewrote the alert history")
        doc, _ = read("data/_fleet-monitor.json")
        r.log("report after quiet: version %s | detected %s raised %s | dlq %s | mode %s | alert rows %s" % (
            doc.get("version"), doc.get("n_alerts_detected"), doc.get("n_alerts_raised"), doc.get("dlq_status"), doc.get("notification_mode"),
            [(a.get("lambda"), a.get("severity"), a.get("error_category")) for a in doc.get("alerts", [])][:8]))
        if doc.get("version") != "1.3.0":
            bad.append("report version %s" % doc.get("version"))
        facts.update(failing_engines=len(got_failing), failing_dead_letters=sum(got_failing.values()), held=json.dumps(got_held, sort_keys=True),
                     queue_depth=body.get("dlq_depth"))

        r.section("4. one real invoke: the corrected alert, once")
        seen_before = hist_before.get("<DLQ>:engines") if isinstance(hist_before.get("<DLQ>:engines"), dict) else {}
        r.log("history before: <DLQ>:CRITICAL %s | <DLQ>:WARNING %s | engines already reported %d" % (
            hist_before.get("<DLQ>:CRITICAL"), hist_before.get("<DLQ>:WARNING"), len(seen_before)))
        body1 = invoke({}, "real #1")
        doc1, _ = read("data/_fleet-monitor.json")
        hist1, _ = read("data/_fleet-monitor-alert-history.json")
        rows = [a for a in doc1.get("alerts", []) if a.get("lambda") == "<DLQ>"]
        seen1 = hist1.get("<DLQ>:engines") if isinstance(hist1.get("<DLQ>:engines"), dict) else {}
        r.log("report: detected %s raised %s suppressed %s | sns_sent %s telegram_sent %s | DLQ row %s" % (
            doc1.get("n_alerts_detected"), doc1.get("n_alerts_raised"), doc1.get("n_alerts_suppressed"), doc1.get("sns_sent"), doc1.get("telegram_sent"),
            [(a.get("severity"), a.get("error_category"), (a.get("dlq_depth") or {}).get("total")) for a in rows]))
        r.log("history after: <DLQ>:WARNING %s | <DLQ>:CRITICAL %s | engines stamped %d (held engine stamped: %s)" % (
            hist1.get("<DLQ>:WARNING"), hist1.get("<DLQ>:CRITICAL"), len(seen1), HELD_EXPECTED in seen1))
        failing1 = (body1.get("dead_letters") or {}).get("failing") or {}
        if failing1:
            want_sev = "CRITICAL" if sum(failing1.values()) >= 100 else "WARNING"
            if len(rows) != 1 or rows[0].get("severity") != want_sev:
                bad.append("expected one <DLQ> row of severity %s, found %s" % (want_sev, [(a.get("severity")) for a in rows]))
            if not (body1.get("n_alerts") or 0) >= 1 or doc1.get("sns_sent") is not True:
                bad.append("the corrected alert was not sent (n_alerts %s, sns_sent %s)" % (body1.get("n_alerts"), doc1.get("sns_sent")))
            if set(seen1) < set(failing1):
                bad.append("alert history did not stamp every named engine: missing %s" % sorted(set(failing1) - set(seen1)))
            if HELD_EXPECTED in seen1:
                bad.append("the held engine was stamped as a reported failure")
        else:
            r.warn("no running engine dead-lettered in the window - nothing to send")
        facts.update(alert_severity=rows[0].get("severity") if rows else "none", sns_sent=doc1.get("sns_sent"), engines_stamped=len(seen1))

        r.section("5. a second real invoke: nothing is sent, the alarm stays visible")
        body2 = invoke({}, "real #2")
        doc2, _ = read("data/_fleet-monitor.json")
        dlq_rows2 = [a for a in doc2.get("alerts", []) if a.get("lambda") == "<DLQ>"]
        r.log("report: detected %s raised %s suppressed %s | sns_sent %s | DLQ rows %d" % (
            doc2.get("n_alerts_detected"), doc2.get("n_alerts_raised"), doc2.get("n_alerts_suppressed"), doc2.get("sns_sent"), len(dlq_rows2)))
        hist2, _ = read("data/_fleet-monitor-alert-history.json")
        if failing1:
            restamped = [k for k in ("<DLQ>:WARNING", "<DLQ>:CRITICAL") if hist2.get(k) != hist1.get(k)]
            r.log("history keys re-stamped by the second run: %s" % (restamped or "none"))
            if restamped:
                bad.append("the second run notified again for the same dead letters (%s re-stamped)" % restamped)
            if len(dlq_rows2) != 1:
                bad.append("the <DLQ> alarm disappeared from the report while still current")
        facts.update(second_run_raised=body2.get("n_alerts"), second_run_suppressed=body2.get("suppressed"))

        r.kv(**{k: str(v) for k, v in facts.items()})
        if bad:
            for b in bad:
                r.fail(b)
            sys.exit(1)
        r.ok("v1.3.0 live: %s dead letters from %s running engines named in one %s alert (sent once); %s reported as held, not failures; queue depth %s no longer pages by itself" % (
            facts.get("failing_dead_letters"), facts.get("failing_engines"), facts.get("alert_severity"), facts.get("held"), facts.get("queue_depth")))
    sys.exit(0)


if __name__ == "__main__":
    main()
