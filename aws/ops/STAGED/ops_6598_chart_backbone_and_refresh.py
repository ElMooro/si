"""ops 6598 - bring the chart back: (A) un-throttle its backbone, (B) refresh the freeze-stale engines behind it,
(C) read the errors of the three chart feeders that fail every run (read-only), (D) name the Function URL the
chart hits that answers 404.

Why (ops 6596 census + ops 6597 live probe, 2026-10-21 14:15 UTC): every desk on chart.html opens, but the chart
itself is blank - quote HUD empty, every watchlist row "Unavailable", "No matching valid dated chart frame".  Every
bar request goes data-proxy worker -> justhodl-symdir Function URL (/warehouse-ohlc, /series).  ops 6558 (2026-10-20)
gave the 102 heavy public URL functions reserved concurrency 2 as a flood cap; symdir (6 GB) is one of them.  A chart
page fans out dozens of bar requests at once, so beyond two in flight the URL answers 429, the worker turns that into
502 ("warehouse lookup failed": 1,790 of them in the probe's four minutes), and the page shows nothing.  The runbook's
own rule (docs/ops/RUNAWAY_GUARDRAILS.md, layer 3): "if a site feature starts answering 429, raise that one function's
reserved concurrency rather than removing the cap".  So:

  A. justhodl-symdir reserved concurrency 2 -> SYMDIR_CAP (bounded: SYMDIR_CAP x 6 GB x ~1.3 s = cents per minute even
     under a flood, and the governor's fuse / runaway alarms still apply).  Refused if the governor's ledger holds
     symdir (CAP/ANOM/FUSE) - the governor wins.  Previous value backed up to the private bucket.
  B. one on-demand run (async, the trigger's own payload) of each chart writer that has not run since the thaw and
     whose next scheduled fire is tonight: cryptoquant, dark-pool, dix, gf-value, etf-global-desk, squeeze-pretrigger.
     Each is a single extra run of a daily engine (all six together ~$0.05); their outputs are polled for ~8 minutes.
  C. justhodl-estimate-revisions (16 errors / 16 runs in 8 days), justhodl-share-flows (7/7), justhodl-earnings-pead
     (3 errors) - the last error lines from their logs, so the fix can be proposed with the cause in hand.  Read-only.
  D. which function owns the lambda-url id the chart's modules call directly (86 x 404 in the probe).
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)
SYMDIR = "justhodl-symdir"
SYMDIR_CAP = 30
THAW = datetime(2026, 10, 21, 0, 30, tzinfo=timezone.utc)
REFRESH = [  # function, main output(s) the chart reads
    ("justhodl-cryptoquant", ["data/cryptoquant-onchain.json", "data/cryptoquant-series.json"]),
    ("justhodl-dark-pool", ["data/dark-pool.json"]),
    ("justhodl-dix", ["data/dix.json"]),
    ("justhodl-gf-value", ["data/gf-value.json"]),
    ("justhodl-etf-global-desk", ["data/etf-derived.json", "data/etf-desk.json"]),
    ("justhodl-squeeze-pretrigger", ["data/squeeze-pretrigger.json"]),
]
BROKEN = ["justhodl-estimate-revisions", "justhodl-share-flows", "justhodl-earnings-pead"]
URL_ID = "ru3djltl3oucvsocjrih37sowu0fxgkm"
URL_SUSPECTS = ["justhodl-wl-series-api", "justhodl-symdir", "justhodl-khalid", "justhodl-tv-notes-ingest", "justhodl-equity-research", "justhodl-katlin",
                "justhodl-watchlist-api", "justhodl-wl-api", "justhodl-tv-vault", "justhodl-chart-api", "justhodl-ai-chat", "justhodl-ask"]


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for _ in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            return json.loads(r["Payload"].read() or b"{}")
        except lam.exceptions.TooManyRequestsException:
            time.sleep(15)
    return {"error": "busy"}


def trigger_input(fn):
    """The payload the function's own enabled trigger sends (committed manifest; rule/schedule inputs are not secrets)."""
    m = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    for sec in ("rules", "schedules"):
        for e in m.get(sec, []):
            if e.get("state") != "ENABLED":
                continue
            for t in e.get("targets") or []:
                m_fn = re.search(r":function:([^/:]+)", t.get("arn") or "")
                if m_fn and m_fn.group(1) == fn:
                    raw = (t.get("input") or "").strip()
                    if raw:
                        try:
                            return json.loads(raw)
                        except ValueError:
                            return {}
                    return {}
    return {}


def insights(logs, group, query, hours, limit=20):
    now = datetime.now(timezone.utc)
    try:
        qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=limit)["queryId"]
    except Exception as e:  # noqa: BLE001
        return "start:%s" % type(e).__name__, []
    for _ in range(40):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def main():
    lam, s3, logs = c("lambda"), c("s3"), c("logs")
    now = datetime.now(timezone.utc)
    with report("ops_6598_chart_backbone_and_refresh") as r:
        r.heading("ops 6598 - chart backbone (symdir concurrency) + refresh of the freeze-stale chart engines")
        r.section("A. justhodl-symdir reserved concurrency")
        st = gov({"action": "status"})
        ledger = st.get("ledger") or {}
        held_keys = [k for k in ledger if SYMDIR in k]
        anomaly_held = [x[0] for x in (st.get("anomaly_held") or []) if isinstance(x, (list, tuple))]
        r.log("governor stage %s; ledger entries naming symdir: %s; anomaly_held: %s" % (st.get("stage"), held_keys, anomaly_held))
        if held_keys or SYMDIR in anomaly_held:
            r.fail("the governor holds %s - not raising its concurrency (the governor wins); release through the governor first" % SYMDIR)
            raise SystemExit(1)
        before = lam.get_function_concurrency(FunctionName=SYMDIR).get("ReservedConcurrentExecutions")
        cf = lam.get_function_configuration(FunctionName=SYMDIR)
        r.log("before: reserved %s; memory %s MB; timeout %s s; worst-case flood at %d concurrent ~ $%.2f/hour" % (
            before, cf.get("MemorySize"), cf.get("Timeout"), SYMDIR_CAP, SYMDIR_CAP * (3600 / 1.3) * (cf.get("MemorySize", 6144) / 1024.0) * 1.3 * 0.0000166667))
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/url-caps/ops6598-%s.json" % stamp, Body=json.dumps({"function": SYMDIR, "reserved_before": before, "set_to": SYMDIR_CAP, "at": now.isoformat(), "why": "chart.html bar requests 429 -> worker 502 (ops 6596/6597)"}).encode(), ServerSideEncryption="AES256")
        if before == SYMDIR_CAP:
            r.ok("already %d" % SYMDIR_CAP)
        else:
            lam.put_function_concurrency(FunctionName=SYMDIR, ReservedConcurrentExecutions=SYMDIR_CAP)
            time.sleep(2)
        after = lam.get_function_concurrency(FunctionName=SYMDIR).get("ReservedConcurrentExecutions")
        r.kv(step="A symdir reserved concurrency", before=before, after=after, note="backup backups/url-caps/ops6598-%s.json" % stamp)
        if after != SYMDIR_CAP:
            r.fail("read-back %s != %d" % (after, SYMDIR_CAP))
            raise SystemExit(1)
        r.ok("%s reserved concurrency %s -> %s" % (SYMDIR, before, after))

        r.section("B. one on-demand run of each freeze-stale chart writer")
        started = {}
        for fn, outs in REFRESH:
            try:
                rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
                continue
            if rc == 0 or any(fn in k for k in ledger):
                r.warn("%s: held (rc %s / ledger) - not invoked" % (fn, rc))
                continue
            before_lm = {}
            for k in outs:
                try:
                    before_lm[k] = s3.head_object(Bucket=LIVE, Key=k)["LastModified"]
                except Exception:  # noqa: BLE001
                    before_lm[k] = None
            payload = trigger_input(fn)
            resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=json.dumps(payload).encode())
            started[fn] = (outs, before_lm, resp.get("StatusCode"))
            r.log("%s: async invoke status %s (payload keys %s); outputs before: %s" % (fn, resp.get("StatusCode"), sorted(payload.keys()), {k: (v.strftime("%m-%d %H:%M") if v else None) for k, v in before_lm.items()}))
        deadline = time.time() + 8 * 60
        done = {}
        while time.time() < deadline and len(done) < len(started):
            time.sleep(20)
            for fn, (outs, before_lm, _) in started.items():
                if fn in done:
                    continue
                fresh = []
                for k in outs:
                    try:
                        lm = s3.head_object(Bucket=LIVE, Key=k)["LastModified"]
                    except Exception:  # noqa: BLE001
                        lm = None
                    if lm and (before_lm.get(k) is None or lm > before_lm[k]):
                        fresh.append((k, lm.strftime("%H:%M:%S")))
                if len(fresh) == len(outs):
                    done[fn] = fresh
        for fn, (outs, _, _) in started.items():
            if fn in done:
                r.ok("%s refreshed: %s" % (fn, done[fn]))
                r.kv(step="B refresh", before="stale", after="refreshed " + ", ".join(k for k, _ in done[fn]), note=fn)
            else:
                r.warn("%s: outputs not rewritten within 8 min (still running, or it failed - its log will say; the scheduled run tonight stands)" % fn)
                r.kv(step="B refresh", before="stale", after="not yet", note=fn)
        errs = {}
        for fn in started:
            status, rows = insights(logs, "/aws/lambda/" + fn, 'fields @timestamp, @message | filter @message like /Traceback|Task timed out|\\[ERROR\\]|Error/ | sort @timestamp desc | limit 3', 1)
            errs[fn] = [x.get("@message", "")[:200].replace("\n", " ") for x in rows]
        r.log("errors logged in the last hour by the re-run engines: %s" % json.dumps({k: v for k, v in errs.items() if v})[:2500])

        r.section("C. the three chart feeders that fail every run (read-only)")
        for fn in BROKEN:
            status, rows = insights(logs, "/aws/lambda/" + fn, 'fields @timestamp, @message | filter @message like /Traceback|Task timed out|\\[ERROR\\]|Error|Exception/ | sort @timestamp desc | limit 12', 72, limit=12)
            msgs = []
            for x in rows:
                m = x.get("@message", "").replace("\n", " ")[:360]
                if m and m not in msgs:
                    msgs.append(m)
            r.log("%s (%s): %s" % (fn, status, json.dumps(msgs[:6])[:3200]))
            status2, rows2 = insights(logs, "/aws/lambda/" + fn, 'fields @message | filter @message like /REPORT/ | stats count() as n, avg(@duration) as ms, max(@maxMemoryUsed) as mem by bin(1d)', 72, limit=10)
            r.log("  runs/day (72 h, %s): %s" % (status2, json.dumps([(x.get("bin(1d)", "")[:10], int(float(x.get("n", 0))), round(float(x.get("ms", 0) or 0)), x.get("mem")) for x in rows2])))
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("  config: timeout %s s, memory %s MB, last modified %s" % (cf.get("Timeout"), cf.get("MemorySize"), (cf.get("LastModified") or "")[:19]))
            except Exception as e:  # noqa: BLE001
                r.warn("  config: %s" % type(e).__name__)

        r.section("D. the lambda-url the chart calls directly")
        owner = None
        for fn in URL_SUSPECTS:
            try:
                u = lam.get_function_url_config(FunctionName=fn).get("FunctionUrl") or ""
                if URL_ID in u:
                    owner = fn
                    break
            except Exception:  # noqa: BLE001
                continue
        if not owner:
            # fall back to a scan of every function's URL (slow but definitive)
            pag = lam.get_paginator("list_functions")
            for page in pag.paginate():
                for f in page.get("Functions", []):
                    try:
                        u = lam.get_function_url_config(FunctionName=f["FunctionName"]).get("FunctionUrl") or ""
                    except Exception:  # noqa: BLE001
                        continue
                    if URL_ID in u:
                        owner = f["FunctionName"]
                        break
                if owner:
                    break
        r.log("lambda-url %s... belongs to %s" % (URL_ID[:8], owner or "not found"))
        if owner:
            try:
                rc = lam.get_function_concurrency(FunctionName=owner).get("ReservedConcurrentExecutions")
                status, rows = insights(logs, "/aws/lambda/" + owner, 'fields @message | filter @message like /404|not found|unknown/ | sort @timestamp desc | limit 5', 2, limit=5)
                r.log("  %s: reserved concurrency %s; recent 404-ish log lines (%s): %s" % (owner, rc, status, json.dumps([x.get("@message", "")[:160] for x in rows])))
            except Exception as e:  # noqa: BLE001
                r.warn("  %s" % type(e).__name__)
        r.kv(step="D url owner", before="-", after=owner or "?", note=URL_ID[:8])
        r.ok("symdir at %d; %d/%d stale writers refreshed within the wait; feeder errors read; re-probe with ops 6597 next" % (SYMDIR_CAP, len(done), len(started)))


if __name__ == "__main__":
    main()
