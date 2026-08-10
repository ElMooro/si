"""ops 4576 (rev 2) — import sentinel live + wo-4559/4575 leftovers closed.

Khalid: "data import on data.html has been hitting hiccups — monitor it
continuously and fix bugs; FRED most-popular-first then the rest; the
others simultaneously if it won't break."

This op:
  1. bootstraps SSM /justhodl/fred/expand-all (String "0") — the one-knob
     transition from the scoped 7 roots to the full catalog on the SAME
     popularity-desc, AIMD-paced, single-flight machinery (v2.2)
  2. creates justhodl-import-sentinel from repo source + a 10-minute
     EventBridge Scheduler heartbeat, then runs one sweep and asserts
     the health payload with a FRED pipeline entry
  3. re-verifies port-cargo v1.0.2 (ISO-date fix — the last 4574 miss)
  4. observes FRED honestly: state snapshot to the report; if STALLED
     with a free lease the sentinel's sweep already queued an Event
     kick (async queues cleanly at reserved-concurrency 1 — the 4575
     TooManyRequests was the single-flight system WORKING)
  5. simultaneity note enacted: providers have independent rate limits,
     so SDMX walkers and FRED run in parallel by design; the only
     serialization that matters is per-provider, which each lease
     already enforces
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
ACCT = "857687956942"
SENT = "justhodl-import-sentinel"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=150, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
REPO = Path(__file__).resolve().parents[2] / "lambdas"


def zip_src(fn):
    buf = io.BytesIO()
    src = REPO / fn / "source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src))
    return buf.getvalue()


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def settle(r, fn, deadline_s=300):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if (c.get("LastUpdateStatus") == "Successful"
                    and c.get("State") == "Active"):
                return True
        except lam.exceptions.ResourceNotFoundException:
            return False
        time.sleep(6)
    r.warn("  %s did not settle in %ss" % (fn, deadline_s))
    return False


def main():
    fails = 0
    with report("4576_import_sentinel") as r:
        r.heading("ops 4576 — import sentinel + leftovers")

        r.section("1. Expansion knob bootstrap")
        try:
            v = ssm.get_parameter(Name="/justhodl/fred/expand-all"
                                  )["Parameter"]["Value"]
            r.kv(expand_all=v)
        except Exception:
            ssm.put_parameter(Name="/justhodl/fred/expand-all",
                              Value="0", Type="String", Overwrite=False)
            r.ok("created /justhodl/fred/expand-all = 0 (scoped first; "
                 "the sentinel flips it when scoped COMPLETEs)")

        r.section("2. Sentinel: create + 10-min heartbeat + settle")
        cfg = json.loads((REPO / SENT / "config.json").read_text())
        try:
            lam.get_function(FunctionName=SENT)
            lam.update_function_code(FunctionName=SENT, ZipFile=zip_src(SENT))
            r.log("  %s exists — code updated from repo" % SENT)
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=SENT, Runtime=cfg["runtime"], Role=cfg["role"],
                Handler=cfg["handler"], Timeout=cfg["timeout"],
                MemorySize=cfg["memory"], Code={"ZipFile": zip_src(SENT)},
                Description=cfg.get("description", "")[:250],
                Environment={"Variables": cfg.get("environment", {})})
            r.ok("  %s created" % SENT)
        settle(r, SENT)
        scfg = cfg["schedule"]
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, SENT)
        try:
            sch.get_schedule(Name=scfg["name"])
            r.log("  schedule exists: %s" % scfg["name"])
        except Exception:
            sch.create_schedule(
                Name=scfg["name"], ScheduleExpression=scfg["expression"],
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": SCHED_ROLE},
                Description=scfg.get("description", "")[:250])
            r.ok("  heartbeat created: %s (%s)"
                 % (scfg["name"], scfg["expression"]))

        r.section("3. First sweep + payload contract")
        resp = lam.invoke(FunctionName=SENT, InvocationType="RequestResponse",
                          LogType="Tail")
        body = resp["Payload"].read().decode("utf-8", "replace")
        if resp.get("FunctionError"):
            fails += 1
            r.fail("sentinel FunctionError: %s" % body[:400])
        h = get_json("data/import-health.json") or {}
        pipes = {p.get("name"): p for p in h.get("pipelines") or []}
        if h.get("overall") and "fred" in pipes:
            r.ok("health payload live — overall=%s, worst=%s, %d pipelines"
                 % (h.get("overall"), h.get("worst"), len(pipes)))
        else:
            fails += 1
            r.fail("health payload missing/incomplete: %s"
                   % json.dumps(h)[:200])
        f = pipes.get("fred") or {}
        r.kv(fred_status=f.get("status"), fred_detail=f.get("detail"),
             scope=f.get("scope"), imported=f.get("imported"),
             cursor=f.get("queue_cursor"), qtotal=f.get("queue_total"),
             rpm=f.get("rate_rpm"), throttles_15m=f.get("throttles_15m"),
             actions=h.get("actions_this_sweep"))
        if f.get("status") in ("ACTION_REQUIRED", "BLOCKED_403"):
            fails += 1
            r.fail("FRED needs a human: %s" % f.get("detail"))
        for nm, p in pipes.items():
            if nm == "fred":
                continue
            (r.ok if p.get("status") in ("OK", "COMPLETE", "RUNNING")
             else r.warn)("  %s → %s (%s)" % (nm, p.get("status"),
                                              str(p.get("detail"))[:90]))

        r.section("4. port-cargo v1.0.2 gate (the last 4574 miss)")
        before = (get_json("data/port-cargo.json") or {}).get("generated_at")
        resp = lam.invoke(FunctionName="justhodl-port-cargo",
                          InvocationType="RequestResponse")
        pj_body = resp["Payload"].read().decode("utf-8", "replace")
        if resp.get("FunctionError"):
            fails += 1
            r.fail("port-cargo FunctionError: %s" % pj_body[:400])
        t0 = time.time()
        pc = None
        while time.time() - t0 < 60:
            pc = get_json("data/port-cargo.json") or {}
            if pc.get("generated_at") != before:
                break
            time.sleep(5)
        n = (pc or {}).get("n_ports_with_data") or 0
        if (pc or {}).get("version") == "1.0.2" and n > 0:
            r.ok("port-cargo v1.0.2: %d ports parsed, date types %s, "
                 "global pulse %s%%"
                 % (n, pc.get("date_field_type"),
                    (pc.get("global_pulse") or {}).get("total_chg_pct")))
            if n < 1500:
                r.warn("  below the ~2065-port universe — gaps: %s"
                       % (pc.get("gaps") or [])[:2])
        else:
            fails += 1
            r.fail("port-cargo still empty (v=%s n=%s gaps=%s)"
                   % ((pc or {}).get("version"), n,
                      ((pc or {}).get("gaps") or [])[:3]))

        r.section("5. Simultaneity (Khalid's question, answered in design)")
        r.log("providers have independent rate limits — FRED and every "
              "SDMX walker already run in parallel; each provider's own "
              "lease enforces the only serialization that matters "
              "(per-provider single-flight). Nothing to gate.")

        r.section("VERDICT")
        if fails:
            r.fail("%d gate(s) failed" % fails)
        else:
            r.ok("sentinel live on a 10-min heartbeat; port-cargo parsing; "
                 "FRED observed with safe-heal + expansion armed")
        r.kv(gates_failed=fails)
        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
