"""ops 6676 - first light of the five fleet-health fixes (Khalid, 2026-10-26: "fix these").

After the deploy lane has shipped analytics-snapshot v2.0.0, earnings-whisper v1.1.0, event-flow-monitor v1.1.0 and
signal-genealogy (validator admits the collector's research_context receipt), and ops 6675 has taken
treasury-auto-updater off the daily-morn tick: invoke each of the four fixed functions ONCE, synchronously and in
parallel (each failed every scheduled run today, so this is the day's run, not an extra one), then read the REPORT line
of each invocation (duration / memory / status) and the published outputs: analytics/manifest.json (complete, pending,
read_stats), data/earnings-whisper.json (engine_version, n_upcoming, input sizes), data/event-flow-health.json
(engine_version, window_days_read, audit_days_present, pulse), the genealogy publication head
(data/signal-genealogy-research/current.json).  Also confirms the live fan-out manifest no longer lists
treasury-auto-updater.  Writes nothing except what the engines write themselves.  No key involved.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import gzip
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=660, max_pool_connections=8)
FUNCTIONS = (os.environ.get("FUNCTIONS") or "justhodl-analytics-snapshot justhodl-earnings-whisper justhodl-event-flow-monitor justhodl-signal-genealogy").split()


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def invoke(lam, fn):
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read()
        return {"fn": fn, "status": resp.get("StatusCode"), "error": resp.get("FunctionError"), "seconds": round(time.time() - t0, 1),
                "request_id": resp.get("ResponseMetadata", {}).get("RequestId"), "body": body[:1500].decode("utf-8", "replace")}
    except Exception as e:  # noqa: BLE001
        return {"fn": fn, "status": None, "error": "%s: %s" % (type(e).__name__, str(e)[:200]), "seconds": round(time.time() - t0, 1), "request_id": None, "body": ""}


def report_line(logs, fn, rid, since_ms):
    for _ in range(6):
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern='"REPORT RequestId: %s"' % rid, limit=5).get("events", [])
        except Exception as e:  # noqa: BLE001
            return "logs: %s" % type(e).__name__
        if ev:
            m = ev[-1].get("message", "")
            return " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
        time.sleep(10)
    return "(REPORT line not yet in CloudWatch)"


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"}))
    with report("ops_6676_fleet_health_first_light") as r:
        r.heading("ops 6676 - fleet-health fixes: first light (one synchronous run each)")
        r.section("0. deployed versions")
        for fn in FUNCTIONS:
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: %s MB / %s s, modified %s" % (fn, c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
        fan = jget(s3, "config/fanout-manifest.json")
        ticks = [t for t, fs in (fan.get("ticks") or {}).items() if "treasury-auto-updater" in fs] if isinstance(fan, dict) else "?"
        r.log("live fan-out manifest: treasury-auto-updater in ticks %s (expected [])" % ticks)
        r.section("1. invocations (parallel, synchronous)")
        since_ms = int(time.time() * 1000) - 10_000
        with ThreadPoolExecutor(max_workers=len(FUNCTIONS)) as ex:
            results = list(ex.map(lambda fn: invoke(lam, fn), FUNCTIONS))
        for res in results:
            r.log("%s: status %s error %s in %ss | %s" % (res["fn"], res["status"], res["error"], res["seconds"], res["body"][:700].replace("\n", " ")))
        r.section("2. REPORT lines")
        for res in results:
            if res["request_id"]:
                r.log("%s: %s" % (res["fn"], report_line(logs, res["fn"], res["request_id"], since_ms)))
        r.section("3. published outputs")
        man = jget(s3, "analytics/manifest.json")
        r.log("analytics/manifest.json: generated %s complete %s pending %s read_stats %s" % (man.get("snapshot_generated_at"), man.get("complete"), man.get("pending"), json.dumps(man.get("read_stats"))[:600]))
        for key in ("analytics/equity_research_flat.json", "analytics/edgar_insiders_flat.json", "analytics/research_critique_flat.json"):
            d = jget(s3, key)
            r.log("%s: n_rows %s complete %s pending %s generated %s" % (key, d.get("n_rows"), d.get("complete"), d.get("pending"), d.get("generated_at")))
        w = jget(s3, "data/earnings-whisper.json")
        r.log("data/earnings-whisper.json: as_of %s engine_version %s n_upcoming %s tiers %s duration %s input_bytes %s" % (
            w.get("as_of"), w.get("engine_version"), w.get("n_upcoming"), w.get("tier_counts"), w.get("duration_s"), json.dumps(w.get("input_bytes"))[:400]))
        e = jget(s3, "data/event-flow-health.json")
        r.log("data/event-flow-health.json: generated %s engine_version %s days_read %s/%s present %s pulse %s anomalies %s alerts %s" % (
            e.get("generated_at"), e.get("engine_version"), e.get("window_days_read"), e.get("window_days"), e.get("audit_days_present"),
            json.dumps(e.get("pulse"))[:200], len(e.get("anomalies") or []), json.dumps(e.get("alerting"))[:200]))
        g = jget(s3, "data/signal-genealogy-research/current.json")
        r.log("signal-genealogy current.json: %s" % json.dumps({k: g.get(k) for k in ("generated_at", "version", "contract", "authority", "replay", "_error") if k in g})[:600])
        bad = [x for x in results if x["error"] or x["status"] != 200]
        if bad:
            r.warn("%d of %d invocations did not return clean: %s" % (len(bad), len(results), ", ".join(x["fn"] for x in bad)))
        else:
            r.ok("all %d invocations returned 200 without FunctionError" % len(results))
        r.ok("ops 6676 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
