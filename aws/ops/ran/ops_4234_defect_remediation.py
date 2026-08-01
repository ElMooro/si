"""
ops_4234 — DEFECT REMEDIATION from the ops-4233 audit.

Order of operations matters here. Config can be fixed blind; code cannot.
So this op first PULLS THE REAL EXCEPTION out of CloudWatch Logs for every
badly-failing engine (no theorising about causes), then applies only the
two fix classes that are provably safe from config alone.

A. DIAGNOSE — for each engine with >=20% error rate, fetch the most
   recent real ERROR/Traceback line. Grouped by exception signature so a
   single upstream break affecting twelve engines shows up as one cause,
   not twelve tickets.

B. THROTTLE FIX — the audit found ~41 functions pinned at
   ReservedConcurrentExecutions=1. Reserved concurrency is a CEILING, not
   a guarantee: at 1, a schedule that fires while the previous run is
   still going gets THROTTLED AND LOST. dollar-strength-agent took 22
   throttles, manufacturing-global-agent 27, justhodl-outcome-checker
   118. Those are dropped runs, not slow runs. Raised to 10 where
   throttling is observed.

C. TIMEOUT-CLIP FIX — 42 engines pin their timeout ceiling. Where the
   AVERAGE run is already >=60% of the ceiling, the ceiling is the real
   constraint and the tail of every run is being cut off (the same
   failure shape as the census bug). Raised to 3x, capped at 900s. Where
   the average is far below the ceiling, only the tail clips, which is a
   code/retry problem and is left for section A to explain rather than
   papered over with a bigger timeout.

Nothing is deleted. Every config change is logged with its prior value.
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=90)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4234, "ts": NOW.isoformat(), "diagnoses": {}, "changes": []}

lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)

AUDIT = json.loads(
    (ROOT / "aws" / "ops" / "reports" /
     "4233_fleet_integrity_audit.json").read_text())


def last_error(fn, days=7):
    start = int((NOW - timedelta(days=days)).timestamp() * 1000)
    try:
        r = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % fn,
            startTime=start, endTime=int(NOW.timestamp() * 1000),
            filterPattern='?ERROR ?Exception ?Traceback ?"Task timed out"',
            limit=12)
    except Exception as e:
        return None, "logs unreadable: %s" % str(e)[:70]
    ev = r.get("events", [])
    if not ev:
        return None, "no ERROR lines in %dd (failure may be a timeout " \
                     "with no traceback)" % days
    msgs = [e["message"].strip() for e in ev]
    # prefer a line that actually names an exception
    best = None
    for m in msgs:
        if re.search(r"(Error|Exception|Traceback|Task timed out)", m):
            best = m
            break
    best = (best or msgs[-1])[:400].replace("\n", " | ")
    return best, None


def signature(msg):
    if not msg:
        return "unknown"
    m = re.search(r"([A-Za-z_]*(?:Error|Exception))\b[: ]*(.{0,70})", msg)
    if m:
        return (m.group(1) + ": " + m.group(2)).strip()[:90]
    if "Task timed out" in msg:
        return "Task timed out (no traceback)"
    return msg[:70]


with report("4234_defect_remediation") as rep:
    rep.heading("ops 4234 — defect remediation")

    # ================================================================ A
    rep.section("A. Real exceptions behind the failing engines")
    errs = sorted(AUDIT["defects"].get("D8_errors", []),
                  key=lambda x: -x.get("err_pct", 0))
    rep.log("engines with >=20%% error rate: %d" % len(errs))
    bysig = {}
    for e in errs[:34]:
        fn = e["fn"]
        msg, note = last_error(fn)
        sig = signature(msg) if msg else (note or "unknown")
        bysig.setdefault(sig, []).append(fn)
        OUT["diagnoses"][fn] = {"err_pct": e.get("err_pct"),
                                "errors": e.get("errors"),
                                "signature": sig, "sample": msg}
        rep.fail("  %-38s %5.1f%%  %s" % (fn[:38], e.get("err_pct", 0), sig))
        if msg:
            rep.log("       %s" % msg[:200])
        rep.kv(section="diagnosis", function=fn,
               error_pct=e.get("err_pct"), signature=sig)
        time.sleep(0.15)

    rep.log("")
    rep.log("GROUPED BY ROOT CAUSE — shared causes fix many engines at once")
    for sig, fl in sorted(bysig.items(), key=lambda x: -len(x[1])):
        rep.log("  [%2d engines] %s" % (len(fl), sig))
        rep.log("               %s" % ", ".join(fl)[:180])
        rep.kv(section="root_cause", signature=sig, engines=len(fl),
               functions=", ".join(fl)[:150])
    OUT["root_causes"] = {k: v for k, v in bysig.items()}

    # ================================================================ B
    rep.section("B. Throttle fix — reserved concurrency ceilings")
    thr = {e["fn"]: e for e in AUDIT["defects"].get("D8_errors", [])}
    n_b = 0
    for fn, e in sorted(thr.items()):
        try:
            cur = lam.get_function_concurrency(
                FunctionName=fn).get("ReservedConcurrentExecutions")
        except Exception:
            continue
        if cur is None or cur >= 10:
            continue
        # only where throttling is actually observed
        try:
            cwc = boto3.client("cloudwatch", region_name=REGION, config=CFG)
            r = cwc.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Throttles",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=14), EndTime=NOW,
                Period=1209600, Statistics=["Sum"])
            t = sum(p["Sum"] for p in r.get("Datapoints", []))
        except Exception:
            t = 0
        if t <= 0:
            continue
        try:
            lam.put_function_concurrency(FunctionName=fn,
                                         ReservedConcurrentExecutions=10)
            rep.ok("  %-40s reserved %s -> 10  (%d throttled runs were "
                   "being DROPPED)" % (fn[:40], cur, int(t)))
            OUT["changes"].append({"a": "concurrency", "fn": fn,
                                   "from": cur, "to": 10,
                                   "throttles_14d": int(t)})
            rep.kv(section="concurrency", function=fn, old=cur, new=10,
                   throttles_14d=int(t))
            n_b += 1
        except Exception as ex:
            rep.fail("  %s: %s" % (fn, str(ex)[:110]))
    rep.log("concurrency ceilings raised: %d" % n_b)

    # also sweep every reserved==1 function that throttled, not just the
    # ones that also had a high error rate
    try:
        cwc = boto3.client("cloudwatch", region_name=REGION, config=CFG)
        allfn = []
        for page in lam.get_paginator("list_functions").paginate():
            allfn += [f["FunctionName"] for f in page["Functions"]]
        extra = 0
        for fn in allfn:
            if fn in [c["fn"] for c in OUT["changes"]]:
                continue
            try:
                cur = lam.get_function_concurrency(
                    FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception:
                continue
            if cur is None or cur > 2:
                continue
            r = cwc.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Throttles",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=14), EndTime=NOW,
                Period=1209600, Statistics=["Sum"])
            t = sum(p["Sum"] for p in r.get("Datapoints", []))
            if t <= 0:
                continue
            lam.put_function_concurrency(FunctionName=fn,
                                         ReservedConcurrentExecutions=10)
            rep.ok("  %-40s reserved %s -> 10 (%d dropped)"
                   % (fn[:40], cur, int(t)))
            OUT["changes"].append({"a": "concurrency", "fn": fn,
                                   "from": cur, "to": 10,
                                   "throttles_14d": int(t)})
            extra += 1
        rep.log("additional throttled functions repaired: %d" % extra)
    except Exception as e:
        rep.warn("concurrency sweep: %s" % str(e)[:120])

    # ================================================================ C
    rep.section("C. Timeout-clip fix (only where the AVERAGE is pinned)")
    n_c = 0
    for d in AUDIT["defects"].get("D2_timeout_clipped", []):
        fn, to_s = d["fn"], d["timeout_s"]
        try:
            cfgn = lam.get_function_configuration(FunctionName=fn)
        except Exception:
            continue
        cwc = boto3.client("cloudwatch", region_name=REGION, config=CFG)
        try:
            r = cwc.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Duration",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=14), EndTime=NOW,
                Period=1209600, Statistics=["Average"])
            avg = max((p["Average"] for p in r.get("Datapoints", [])),
                      default=0) / 1000.0
        except Exception:
            continue
        ratio = avg / max(to_s, 1)
        if ratio < 0.60:
            rep.log("  %-38s avg %.0fs of %ds (%.0f%%) — TAIL clip only, "
                    "left alone (code issue, see A)"
                    % (fn[:38], avg, to_s, ratio * 100))
            continue
        new = min(900, to_s * 3)
        if new <= to_s:
            rep.warn("  %-38s already at 900s ceiling — needs work "
                     "SPLITTING, not a bigger timeout" % fn[:38])
            OUT["changes"].append({"a": "needs_split", "fn": fn})
            continue
        try:
            lam.update_function_configuration(FunctionName=fn, Timeout=new)
            rep.ok("  %-38s timeout %ds -> %ds (avg %.0fs = %.0f%% of old "
                   "ceiling)" % (fn[:38], to_s, new, avg, ratio * 100))
            OUT["changes"].append({"a": "timeout", "fn": fn, "from": to_s,
                                   "to": new, "avg_s": round(avg, 1)})
            rep.kv(section="timeout", function=fn, old_s=to_s, new_s=new,
                   avg_s=round(avg, 1))
            n_c += 1
            time.sleep(0.4)
        except Exception as ex:
            rep.fail("  %s: %s" % (fn, str(ex)[:110]))
    rep.log("timeouts raised: %d" % n_c)

    # ================================================================ D
    rep.section("D. Result")
    rep.log("diagnoses captured: %d" % len(OUT["diagnoses"]))
    rep.log("config changes applied: %d" % len(OUT["changes"]))
    (ROOT / "aws" / "ops" / "reports" / "4234_defect_remediation.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote 4234_defect_remediation.json")
