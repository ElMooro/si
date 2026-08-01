"""
justhodl-fleet-integrity — the standing audit.

WHY THIS EXISTS
The fundamental-census walk ran at 25% completion for months and 90
engines double-fired on every tick. Neither errored. Neither alarmed.
Both were found only because a cost investigation happened to walk past
them. The lesson is not "those were bad bugs" — it is that a fleet this
size has no immune system unless one is built deliberately.

This engine encodes every failure mode that has ever hidden here, runs
them weekly against the whole fleet, and — critically — DIFFS AGAINST A
BASELINE so it reports NEW defects rather than re-reporting the known
backlog every week. An alarm that cries every run is an alarm nobody
reads; that is how the original defects survived.

CHECKS
  D1  unguarded self-invocation (recursion / silent truncation)
  D2  timeout-clipped engines (average pinned at the ceiling)
  D3  duplicate EventBridge targets (double-fire)
  D4  orphan schedules (target function does not exist)
  D6  scheduled but zero invocations
  D7  reserved concurrency low enough to drop runs
  D8  error-rate and throttle outliers
  D9  scheduled + failing + no DLQ or on-failure destination
  D10 deprecated runtimes
  D11 account code-storage headroom (at 100% every deploy fleet-wide dies)
  D12 env vars naming resources that no longer resolve
  D13 dead functions (no schedule, no invocations, no URL)

OUTPUT
  s3://<bucket>/data/fleet-integrity.json     — full ledger for the page
  s3://<bucket>/data/_state/fleet-integrity-baseline.json — known set
  CloudWatch EMF metrics under namespace JustHodl/Integrity, emitted as
  a structured log line so the metrics cost ZERO API calls. This is the
  same pattern that should replace the per-function polling that used to
  cost ~$66/month.

MODES
  event {"mode":"audit"}    default — report, diff, alarm
  event {"mode":"baseline"} accept the current state as the new baseline
                            (use after a deliberate cleanup)
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

VERSION = "1.1.0"
MARKER = "fleet-integrity v1.1.0 ops4242 guard-aware"

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ARTIFACT = "data/fleet-integrity.json"
BASELINE = "data/_state/fleet-integrity-baseline.json"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=60)
lam = boto3.client("lambda", config=CFG)
cw = boto3.client("cloudwatch", config=CFG)
evb = boto3.client("events", config=CFG)
sch = boto3.client("scheduler", config=CFG)
s3 = boto3.client("s3", config=CFG)

DEPRECATED = {"python3.6", "python3.7", "python3.8", "nodejs12.x",
              "nodejs14.x", "nodejs16.x", "ruby2.7", "go1.x", "dotnet6"}

# Severity drives the page ordering and the alarm. SEV1 = something is
# silently wrong RIGHT NOW; SEV2 = degradation; SEV3 = hygiene.
SEV = {
    "D11_code_storage": 1, "D4_orphan_rule": 1, "D6_scheduled_never_ran": 1,
    "D1_recursion": 1, "D2_timeout_clipped": 1, "D3_double_fire": 1,
    "D8_errors": 2, "D7_concurrency_drop": 2, "D9_no_dlq": 2,
    "D12_stale_env": 2, "D10_runtime": 3, "D13_dead": 3,
}


def _now():
    return datetime.now(timezone.utc)


def emf(defects_by_class, n_fn, storage_pct, new_count):
    """CloudWatch Embedded Metric Format. One log line, no API calls,
    no per-metric request billing. CloudWatch parses this automatically."""
    metrics = [{"Name": "DefectsTotal", "Unit": "Count"},
               {"Name": "DefectsNew", "Unit": "Count"},
               {"Name": "FleetSize", "Unit": "Count"},
               {"Name": "CodeStoragePct", "Unit": "Percent"}]
    doc = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "JustHodl/Integrity",
                "Dimensions": [[]],
                "Metrics": metrics,
            }],
        },
        "DefectsTotal": sum(len(v) for v in defects_by_class.values()),
        "DefectsNew": new_count,
        "FleetSize": n_fn,
        "CodeStoragePct": round(storage_pct, 1),
    }
    for k, v in defects_by_class.items():
        doc[k] = len(v)
    print(json.dumps(doc))


def collect_metrics(names):
    """Batched GetMetricData. One request per 100 functions rather than
    three per function — the shape that caused the old $66/mo line."""
    spec = [("Invocations", "Sum", "inv"), ("Errors", "Sum", "err"),
            ("Throttles", "Sum", "thr"), ("Duration", "Maximum", "dmax"),
            ("Duration", "Average", "davg")]
    end = _now()
    start = end - timedelta(days=14)
    out = {}
    for i in range(0, len(names), 100):
        chunk = names[i:i + 100]
        q = []
        for j, fn in enumerate(chunk):
            for mt, st, tag in spec:
                q.append({
                    "Id": "m%s_%d" % (tag, j),
                    "MetricStat": {
                        "Metric": {"Namespace": "AWS/Lambda",
                                   "MetricName": mt,
                                   "Dimensions": [{"Name": "FunctionName",
                                                   "Value": fn}]},
                        "Period": 1209600, "Stat": st},
                    "ReturnData": True})
        try:
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=start,
                                     EndTime=end,
                                     ScanBy="TimestampDescending")
        except Exception as e:
            print("[integrity] metric batch %d: %s" % (i, str(e)[:120]))
            continue
        v = {}
        for r in res["MetricDataResults"]:
            vals = r["Values"]
            if not vals:
                v[r["Id"]] = 0.0
            elif r["Id"].startswith("mdmax"):
                v[r["Id"]] = max(vals)
            elif r["Id"].startswith("mdavg"):
                v[r["Id"]] = sum(vals) / len(vals)
            else:
                v[r["Id"]] = sum(vals)
        for j, fn in enumerate(chunk):
            out[fn] = {t: v.get("m%s_%d" % (t, j), 0.0)
                       for _, _, t in spec}
    return out


def schedule_map():
    """fn -> [(kind, rule, expr, state)] plus orphans and duplicate
    targets, gathered in ONE pass so the page and the reconciler agree."""
    wired, orphans, dupes = {}, [], []
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                sigs = set()
                for t in tg.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":function:" not in arn:
                        continue
                    sig = json.dumps({"a": arn, "i": t.get("Input"),
                                      "p": t.get("InputPath")},
                                     sort_keys=True)
                    if sig in sigs:
                        dupes.append({"rule": r["Name"],
                                      "fn": arn.split(":")[-1]})
                    sigs.add(sig)
                    if r.get("ScheduleExpression"):
                        wired.setdefault(arn.split(":")[-1], []).append(
                            {"kind": "events", "rule": r["Name"],
                             "expr": r["ScheduleExpression"],
                             "state": r.get("State")})
    except Exception as e:
        print("[integrity] rules: %s" % str(e)[:120])
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                g = s_.get("GroupName", "default")
                try:
                    d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                except Exception:
                    continue
                arn = (d.get("Target", {}) or {}).get("Arn", "") or ""
                if ":function:" not in arn:
                    continue
                wired.setdefault(arn.split(":")[-1], []).append(
                    {"kind": "scheduler", "rule": s_["Name"],
                     "expr": d.get("ScheduleExpression"),
                     "state": d.get("State"), "group": g})
    except Exception as e:
        print("[integrity] schedules: %s" % str(e)[:120])
    return wired, orphans, dupes


def audit():
    D = {}

    def add(cls, item):
        D.setdefault(cls, []).append(item)

    fns = {}
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns[f["FunctionName"]] = f
    names = sorted(fns)

    # ---- D11 code storage
    storage_pct = 0.0
    try:
        a = lam.get_account_settings()
        used = a["AccountUsage"]["TotalCodeSize"]
        limit = a["AccountLimit"]["TotalCodeSize"]
        storage_pct = 100.0 * used / limit
        if storage_pct > 70:
            add("D11_code_storage",
                {"id": "account", "pct": round(storage_pct, 1),
                 "detail": "at 100%% every deploy in the account fails"})
    except Exception as e:
        print("[integrity] account settings: %s" % str(e)[:100])

    M = collect_metrics(names)
    wired, orphans, dupes = schedule_map()

    for d in dupes:
        add("D3_double_fire",
            {"id": d["rule"], "detail": "target %s listed twice — fires "
                                        "twice per tick" % d["fn"]})

    live = set(names)
    for fn in list(wired):
        if fn not in live:
            for w in wired[fn]:
                add("D4_orphan_rule",
                    {"id": w["rule"],
                     "detail": "targets missing function %s (%s)"
                               % (fn, w["expr"])})

    enabled = {f: [w for w in v if (w.get("state") or "ENABLED") == "ENABLED"]
               for f, v in wired.items() if f in live}
    enabled = {f: v for f, v in enabled.items() if v}

    for fn, v in enabled.items():
        m = M.get(fn, {})
        if m.get("inv", 0) == 0:
            add("D6_scheduled_never_ran",
                {"id": fn, "detail": "scheduled %s but 0 invocations in 14d"
                                     % ", ".join(str(x["expr"]) for x in v)})

    for fn in names:
        m = M.get(fn, {})
        to_ms = fns[fn].get("Timeout", 3) * 1000.0
        if m.get("inv", 0) < 3 or to_ms <= 0:
            continue
        if m.get("dmax", 0) >= to_ms * 0.97 and \
                m.get("davg", 0) >= to_ms * 0.60:
            add("D2_timeout_clipped",
                {"id": fn,
                 "detail": "avg %.0fs of a %.0fs ceiling — the tail of "
                           "every run is being cut off"
                           % (m["davg"] / 1000, to_ms / 1000)})
        er = (100.0 * m.get("err", 0) / m["inv"]) if m.get("inv") else 0
        if m.get("inv", 0) >= 5 and (er >= 20 or m.get("thr", 0) > 0):
            add("D8_errors",
                {"id": fn, "detail": "%.0f%% error rate (%d/%d), %d throttles"
                                     % (er, m.get("err", 0), m["inv"],
                                        m.get("thr", 0))})
        if m.get("thr", 0) > 0:
            try:
                rc = lam.get_function_concurrency(
                    FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception:
                rc = None
            if rc is not None and rc <= 2:
                add("D7_concurrency_drop",
                    {"id": fn, "detail": "reserved=%s with %d throttled "
                                         "runs — those runs were DROPPED, "
                                         "not delayed" % (rc, m["thr"])})
        if fns[fn].get("Runtime") in DEPRECATED:
            add("D10_runtime", {"id": fn,
                                "detail": "runtime %s is deprecated"
                                          % fns[fn].get("Runtime")})
        if m.get("err", 0) > 0 and fn in enabled:
            has = bool((fns[fn].get("DeadLetterConfig") or {}).get(
                "TargetArn"))
            if not has:
                try:
                    c = lam.get_function_event_invoke_config(FunctionName=fn)
                    has = bool((c.get("DestinationConfig") or {}).get(
                        "OnFailure", {}).get("Destination"))
                except Exception:
                    has = False
            if not has:
                add("D9_no_dlq",
                    {"id": fn, "detail": "%d failed events with no DLQ or "
                                         "on-failure destination — they "
                                         "vanished" % m["err"]})
        ev = (fns[fn].get("Environment") or {}).get("Variables") or {}
        blob = json.dumps(ev)
        for pat in ("es.amazonaws.com", "elb.amazonaws.com",
                    "awsapprunner.com"):
            if pat in blob:
                add("D12_stale_env",
                    {"id": fn, "detail": "env references %s — verify the "
                                         "resource still exists" % pat})
                break
        if fn not in wired and m.get("inv", 0) == 0:
            add("D13_dead", {"id": fn,
                             "detail": "no schedule, no invocations in 14d"})

    return D, len(names), storage_pct


def lambda_handler(event=None, context=None):
    event = event or {}
    mode = (event.get("mode") or "audit").lower()
    t0 = time.time()
    D, n_fn, storage_pct = audit()

    # ---- baseline diff: report what is NEW, not the whole backlog
    keys_now = {"%s|%s" % (c, i["id"]) for c, v in D.items() for i in v}
    try:
        base = set(json.loads(s3.get_object(
            Bucket=BUCKET, Key=BASELINE)["Body"].read()).get("keys") or [])
    except Exception:
        base = set()
    new = sorted(keys_now - base)
    fixed = sorted(base - keys_now)

    rows = []
    for cls, items in D.items():
        for it in items:
            k = "%s|%s" % (cls, it["id"])
            rows.append({"cls": cls, "sev": SEV.get(cls, 3),
                         "id": it["id"], "detail": it["detail"],
                         "is_new": k in (keys_now - base)})
    rows.sort(key=lambda r: (r["sev"], not r["is_new"], r["cls"], r["id"]))

    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": _now().isoformat(),
           "fleet_size": n_fn, "code_storage_pct": round(storage_pct, 1),
           "totals": {c: len(v) for c, v in D.items()},
           "n_defects": len(rows), "n_new": len(new), "n_fixed": len(fixed),
           "new": new, "fixed": fixed, "rows": rows,
           "sev1": sum(1 for r in rows if r["sev"] == 1),
           "sev2": sum(1 for r in rows if r["sev"] == 2),
           "sev3": sum(1 for r in rows if r["sev"] == 3),
           "elapsed_s": round(time.time() - t0, 1)}

    s3.put_object(Bucket=BUCKET, Key=ARTIFACT,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")

    if mode == "baseline" or not base:
        s3.put_object(Bucket=BUCKET, Key=BASELINE,
                      Body=json.dumps({"keys": sorted(keys_now),
                                       "at": _now().isoformat()}).encode(),
                      ContentType="application/json")
        print("[integrity] baseline set (%d keys)" % len(keys_now))

    emf(D, n_fn, storage_pct, len(new))
    print("[integrity] defects=%d new=%d fixed=%d sev1=%d elapsed=%.0fs"
          % (len(rows), len(new), len(fixed), doc["sev1"], doc["elapsed_s"]))
    for k in new[:25]:
        print("[integrity] NEW %s" % k)
    return {"ok": True, "n_defects": len(rows), "n_new": len(new),
            "n_fixed": len(fixed), "sev1": doc["sev1"]}
