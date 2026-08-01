"""
ops_4233 — FULL-FLEET INTEGRITY AUDIT. READ-ONLY.

The census bug and the 90 double-fire rules shared one property: both
were INVISIBLE. Nothing errored, nothing alarmed, dashboards kept
publishing. They were found only because a cost investigation happened
to walk past them. This op generalises that walk into a systematic sweep
for every failure mode that can run silently.

DEFECT CLASSES HUNTED
  D1  self-invocation / recursion chains (the census bug's family)
  D2  timeout-clipped engines — Duration max pinned at the configured
      ceiling means the run is being CUT OFF, and whatever the function
      does after that point has never executed
  D3  duplicate EventBridge targets (double-fire) — re-verified
  D4  orphan rules — schedules pointing at functions that no longer
      exist; they fail on every tick, forever, silently
  D5  BROKEN WIRES — a schedule exists but the target Lambda's resource
      policy does not permit events.amazonaws.com to invoke it. The rule
      fires, the invoke is refused, and NO Lambda error metric is ever
      emitted. The engine looks scheduled and is simply dead.
  D6  scheduled-but-never-invoked — schedule present, zero invocations
      in 14d (a superset symptom of D4/D5 and of disabled targets)
  D7  reserved concurrency = 0 — a function hard-disabled, often by an
      old kill-switch nobody remembered to lift
  D8  error-rate and throttle outliers
  D9  async failure with nowhere to land — no DLQ and no on-failure
      destination, so failed async events vanish
  D10 deprecated runtimes (forced-migration risk)
  D11 code storage against the 75 GB account quota — hitting it blocks
      EVERY future deploy fleet-wide
  D12 stale env vars naming resources that no longer exist
  D13 dead functions — no schedule, no invocations, no callers

Read-only. Produces a ranked, evidence-backed defect ledger.
"""

import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=90)
NOW = datetime.now(timezone.utc)
D14 = NOW - timedelta(days=14)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4233, "ts": NOW.isoformat(), "defects": {}}

DEPRECATED = ("python3.6", "python3.7", "python3.8", "nodejs12.x",
              "nodejs14.x", "nodejs16.x", "ruby2.7", "go1.x", "dotnet6")


def C(s):
    return boto3.client(s, region_name=REGION, config=CFG)


lam, cw, evb, sch, s3 = C("lambda"), C("cloudwatch"), C("events"), \
    C("scheduler"), C("s3")


def add(cls, item):
    OUT["defects"].setdefault(cls, []).append(item)


with report("4233_fleet_integrity_audit") as rep:
    rep.heading("ops 4233 — full-fleet integrity audit")

    # ---------------------------------------------------------- inventory
    rep.section("0. Inventory")
    fns = {}
    total_code = 0
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns[f["FunctionName"]] = f
            total_code += f.get("CodeSize", 0)
    rep.log("functions: %d   $LATEST code total: %.2f GB"
            % (len(fns), total_code / 1e9))

    try:
        acct = lam.get_account_settings()
        used = acct["AccountUsage"]["TotalCodeSize"]
        limit = acct["AccountLimit"]["TotalCodeSize"]
        pct = 100.0 * used / limit
        rep.log("ACCOUNT CODE STORAGE: %.2f GB / %.2f GB  (%.1f%%)"
                % (used / 1e9, limit / 1e9, pct))
        rep.kv(section="storage", used_gb=round(used / 1e9, 2),
               limit_gb=round(limit / 1e9, 2), pct=round(pct, 1))
        if pct > 70:
            rep.fail("D11 CODE STORAGE AT %.1f%% — at 100%% every deploy in "
                     "the account fails. Prune old published versions." % pct)
            add("D11_code_storage", {"pct": round(pct, 1),
                                     "used_gb": round(used / 1e9, 2)})
        elif pct > 40:
            rep.warn("D11 code storage at %.1f%% — watch it" % pct)
            add("D11_code_storage", {"pct": round(pct, 1), "level": "watch"})
        else:
            rep.ok("D11 code storage healthy (%.1f%%)" % pct)
    except Exception as e:
        rep.warn("account settings: %s" % str(e)[:110])

    # ---------------------------------------------------------- metrics
    rep.section("1. Fleet metrics (14d)")
    names = sorted(fns)
    M = {}
    SPEC = [("Invocations", "Sum", "inv"), ("Errors", "Sum", "err"),
            ("Throttles", "Sum", "thr"), ("Duration", "Maximum", "dmax"),
            ("Duration", "Average", "davg")]
    for i in range(0, len(names), 100):
        chunk = names[i:i + 100]
        q = []
        for j, fn in enumerate(chunk):
            for mt, st, tag in SPEC:
                q.append({"Id": "m%s_%d" % (tag, j),
                          "MetricStat": {
                              "Metric": {"Namespace": "AWS/Lambda",
                                         "MetricName": mt,
                                         "Dimensions": [
                                             {"Name": "FunctionName",
                                              "Value": fn}]},
                              "Period": 1209600, "Stat": st},
                          "ReturnData": True})
        try:
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=D14,
                                     EndTime=NOW,
                                     ScanBy="TimestampDescending")
        except Exception as e:
            rep.warn("metric batch %d: %s" % (i, str(e)[:90]))
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
            M[fn] = {t: v.get("m%s_%d" % (t, j), 0.0)
                     for _, _, t in SPEC}
    rep.ok("metrics collected for %d functions" % len(M))

    # ---------------------------------------------------------- D1
    rep.section("D1. Self-invocation / recursion chains")
    try:
        r = subprocess.run(
            ["grep", "-rln", "--include=lambda_function.py",
             "-e", "FunctionName=context.function_name",
             "-e", "FunctionName=os.environ\\[.AWS_LAMBDA_FUNCTION_NAME",
             "aws/lambdas"], cwd=str(ROOT),
            capture_output=True, text=True, timeout=120)
        hits = [x.split("/")[2] for x in r.stdout.strip().split("\n") if x]
    except Exception as e:
        hits = []
        rep.warn("grep: %s" % str(e)[:90])
    rep.log("functions that invoke THEMSELVES in source: %d" % len(hits))
    for h in sorted(set(hits)):
        src = ROOT / "aws" / "lambdas" / h / "source" / "lambda_function.py"
        body = src.read_text(errors="ignore") if src.exists() else ""
        guarded = ("CHAIN_MAX" in body or "depth" in body.lower()[:99999]
                   and re.search(r"depth\s*[+<>=]", body) is not None)
        st = "GUARDED" if guarded else "UNGUARDED — recursion risk"
        (rep.ok if guarded else rep.fail)("   %-44s %s" % (h[:44], st))
        rep.kv(section="D1_self_invoke", function=h, guarded=guarded)
        if not guarded:
            add("D1_recursion", {"fn": h})
    # live counter across the fleet
    try:
        seen = set()
        for page in cw.get_paginator("list_metrics").paginate(
                Namespace="AWS/Lambda",
                MetricName="RecursiveInvocationsDropped"):
            for m in page["Metrics"]:
                for d in m["Dimensions"]:
                    if d["Name"] == "FunctionName":
                        seen.add(d["Value"])
        rep.log("functions AWS has ever loop-broken: %s"
                % (", ".join(sorted(seen)) or "none"))
    except Exception:
        pass

    # ---------------------------------------------------------- D2
    rep.section("D2. Timeout-clipped engines (silent truncation)")
    clipped = []
    for fn, m in M.items():
        to_ms = fns[fn].get("Timeout", 3) * 1000.0
        if m["inv"] < 1 or to_ms <= 0:
            continue
        if m["dmax"] >= to_ms * 0.97:
            clipped.append((fn, to_ms, m))
    clipped.sort(key=lambda x: -x[2]["inv"])
    rep.log("engines whose max duration pins the timeout ceiling: %d"
            % len(clipped))
    for fn, to_ms, m in clipped[:30]:
        rep.fail("   %-42s timeout=%4.0fs max=%6.1fs avg=%6.1fs inv=%d"
                 % (fn[:42], to_ms / 1000, m["dmax"] / 1000,
                    m["davg"] / 1000, int(m["inv"])))
        rep.kv(section="D2_timeout_clipped", function=fn,
               timeout_s=int(to_ms / 1000), max_s=round(m["dmax"] / 1000, 1),
               avg_s=round(m["davg"] / 1000, 1), invocations=int(m["inv"]))
        add("D2_timeout_clipped", {"fn": fn, "timeout_s": int(to_ms / 1000),
                                   "max_s": round(m["dmax"] / 1000, 1),
                                   "inv": int(m["inv"])})

    # ---------------------------------------------------------- schedules
    rep.section("D3/D4/D5/D6. Schedule wiring integrity")
    wired = {}   # fn -> list of (kind, rule, expr, state)
    orphan_rules, dup_targets = [], []
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                sigs = {}
                for t in tg.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":function:" not in arn:
                        continue
                    fn = arn.split(":")[-1]
                    sig = json.dumps({"a": arn, "i": t.get("Input"),
                                      "p": t.get("InputPath")},
                                     sort_keys=True)
                    if sig in sigs:
                        dup_targets.append((r["Name"], fn))
                    sigs[sig] = 1
                    if fn not in fns:
                        orphan_rules.append((r["Name"], fn,
                                             r.get("ScheduleExpression")))
                    elif r.get("ScheduleExpression"):
                        wired.setdefault(fn, []).append(
                            ("events", r["Name"], r["ScheduleExpression"],
                             r.get("State")))
    except Exception as e:
        rep.warn("rules: %s" % str(e)[:110])
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                g = s_.get("GroupName", "default")
                try:
                    d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                except Exception:
                    continue
                arn = (d.get("Target", {}).get("Arn", "") or "")
                if ":function:" not in arn:
                    continue
                fn = arn.split(":")[-1]
                if fn not in fns:
                    orphan_rules.append(("scheduler:" + s_["Name"], fn,
                                         d.get("ScheduleExpression")))
                else:
                    wired.setdefault(fn, []).append(
                        ("scheduler", s_["Name"],
                         d.get("ScheduleExpression"), d.get("State")))
    except Exception as e:
        rep.warn("schedules: %s" % str(e)[:110])

    rep.log("D3 duplicate targets remaining: %d" % len(dup_targets))
    for rn, fn in dup_targets[:10]:
        rep.fail("   %s -> %s" % (rn, fn))
        add("D3_double_fire", {"rule": rn, "fn": fn})

    rep.log("D4 orphan schedules (target function does not exist): %d"
            % len(orphan_rules))
    for rn, fn, ex in orphan_rules[:25]:
        rep.fail("   %-44s -> MISSING %s  (%s)" % (rn[:44], fn, ex))
        rep.kv(section="D4_orphan_rule", rule=rn, missing_function=fn,
               expr=ex)
        add("D4_orphan_rule", {"rule": rn, "fn": fn, "expr": ex})

    rep.log("")
    rep.log("D5 broken wires — schedule exists, invoke permission missing")
    broken = []
    enabled_wired = {f: [w for w in v if (w[3] or "ENABLED") == "ENABLED"]
                     for f, v in wired.items()}
    enabled_wired = {f: v for f, v in enabled_wired.items() if v}
    for fn in sorted(enabled_wired):
        try:
            pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
            ok = any("events.amazonaws.com" in json.dumps(s_) or
                     "scheduler.amazonaws.com" in json.dumps(s_)
                     for s_ in pol.get("Statement", []))
        except lam.exceptions.ResourceNotFoundException:
            ok = False
        except Exception:
            continue
        if not ok:
            inv = M.get(fn, {}).get("inv", 0)
            broken.append((fn, inv, enabled_wired[fn][0][2]))
    rep.log("   functions scheduled WITHOUT an events/scheduler invoke "
            "permission: %d" % len(broken))
    for fn, inv, ex in sorted(broken, key=lambda x: x[1])[:30]:
        (rep.fail if inv == 0 else rep.warn)(
            "   %-42s inv14d=%-7d %s" % (fn[:42], int(inv), ex))
        rep.kv(section="D5_broken_wire", function=fn,
               invocations_14d=int(inv), expr=ex)
        add("D5_broken_wire", {"fn": fn, "inv": int(inv), "expr": ex})

    rep.log("")
    rep.log("D6 scheduled but ZERO invocations in 14d")
    dead_sched = [(f, v) for f, v in enabled_wired.items()
                  if M.get(f, {}).get("inv", 0) == 0]
    rep.log("   count: %d" % len(dead_sched))
    for f, v in sorted(dead_sched)[:30]:
        rep.fail("   %-42s %s" % (f[:42], ", ".join(x[2] or "?"
                                                    for x in v)[:50]))
        rep.kv(section="D6_scheduled_never_ran", function=f,
               exprs=", ".join(str(x[2]) for x in v)[:70])
        add("D6_scheduled_never_ran", {"fn": f,
                                       "exprs": [x[2] for x in v]})

    # ---------------------------------------------------------- D7
    rep.section("D7. Reserved concurrency = 0 (hard-disabled functions)")
    n0 = 0
    for fn in names:
        try:
            rc = lam.get_function_concurrency(FunctionName=fn)
            v = rc.get("ReservedConcurrentExecutions")
            if v == 0:
                n0 += 1
                rep.fail("   %-44s RESERVED CONCURRENCY 0 — cannot run"
                         % fn[:44])
                rep.kv(section="D7_disabled", function=fn, reserved=0)
                add("D7_reserved_zero", {"fn": fn})
            elif v is not None and v < 5:
                rep.warn("   %-44s reserved=%s (throttle risk)"
                         % (fn[:44], v))
        except Exception:
            continue
    if n0 == 0:
        rep.ok("   none hard-disabled")

    # ---------------------------------------------------------- D8
    rep.section("D8. Error-rate and throttle outliers")
    bad = []
    for fn, m in M.items():
        if m["inv"] >= 5:
            er = 100.0 * m["err"] / m["inv"]
            if er >= 20 or m["thr"] > 0:
                bad.append((fn, er, m))
    bad.sort(key=lambda x: -x[1])
    rep.log("functions with >=20%% error rate or any throttling: %d"
            % len(bad))
    for fn, er, m in bad[:30]:
        rep.fail("   %-42s err=%5.1f%% (%d/%d) throttles=%d"
                 % (fn[:42], er, int(m["err"]), int(m["inv"]),
                    int(m["thr"])))
        rep.kv(section="D8_errors", function=fn, error_pct=round(er, 1),
               errors=int(m["err"]), invocations=int(m["inv"]),
               throttles=int(m["thr"]))
        add("D8_errors", {"fn": fn, "err_pct": round(er, 1),
                          "errors": int(m["err"]), "inv": int(m["inv"])})

    # ---------------------------------------------------------- D9
    rep.section("D9. Async failures with nowhere to land")
    nodlq = []
    for fn in names:
        f = fns[fn]
        has_dlq = bool((f.get("DeadLetterConfig") or {}).get("TargetArn"))
        if has_dlq:
            continue
        try:
            cfgn = lam.get_function_event_invoke_config(FunctionName=fn)
            dest = (cfgn.get("DestinationConfig") or {}).get("OnFailure")
            if dest and dest.get("Destination"):
                continue
        except Exception:
            pass
        if M.get(fn, {}).get("err", 0) > 0 and fn in enabled_wired:
            nodlq.append((fn, M[fn]["err"]))
    rep.log("scheduled functions WITH errors and NO DLQ/on-failure "
            "destination: %d" % len(nodlq))
    for fn, e in sorted(nodlq, key=lambda x: -x[1])[:20]:
        rep.warn("   %-44s %d failed events dropped silently"
                 % (fn[:44], int(e)))
        add("D9_no_dlq", {"fn": fn, "errors": int(e)})

    # ---------------------------------------------------------- D10
    rep.section("D10. Deprecated runtimes")
    dep = [(f, fns[f].get("Runtime")) for f in names
           if fns[f].get("Runtime") in DEPRECATED]
    rep.log("functions on deprecated runtimes: %d" % len(dep))
    byrt = {}
    for f, rt in dep:
        byrt.setdefault(rt, []).append(f)
    for rt, fl in sorted(byrt.items()):
        rep.fail("   %-14s %d functions" % (rt, len(fl)))
        rep.kv(section="D10_runtime", runtime=rt, count=len(fl),
               sample=", ".join(fl[:4]))
        add("D10_runtime", {"runtime": rt, "count": len(fl),
                            "functions": fl})

    # ---------------------------------------------------------- D12
    rep.section("D12. Env vars naming resources that no longer exist")
    GONE = ["openbb-financial-search", "openbb-simple-working",
            "openbb-prod-alb", "openbb-basic-alb", "es.amazonaws.com",
            "elb.amazonaws.com", "awsapprunner.com"]
    n12 = 0
    for fn in names:
        ev = (fns[fn].get("Environment") or {}).get("Variables") or {}
        blob = json.dumps(ev)
        for g in GONE:
            if g in blob:
                keys = [k for k, v in ev.items() if g in str(v)]
                rep.fail("   %-40s %s -> %s" % (fn[:40], ",".join(keys), g))
                rep.kv(section="D12_stale_env", function=fn,
                       keys=",".join(keys), points_to=g)
                add("D12_stale_env", {"fn": fn, "keys": keys, "target": g})
                n12 += 1
                break
    if n12 == 0:
        rep.ok("   none — env surface clean")

    # ---------------------------------------------------------- D13
    rep.section("D13. Dead functions (no schedule, no invocations)")
    dead = [f for f in names
            if f not in wired and M.get(f, {}).get("inv", 0) == 0]
    rep.log("functions with no schedule AND zero invocations in 14d: %d "
            "of %d (%.0f%% of the fleet)"
            % (len(dead), len(names), 100.0 * len(dead) / max(len(names), 1)))
    for f in sorted(dead)[:40]:
        rep.log("   %s" % f)
    add("D13_dead", {"count": len(dead), "functions": sorted(dead)})

    # ---------------------------------------------------------- summary
    rep.section("SUMMARY — defect ledger")
    order = ["D11_code_storage", "D5_broken_wire", "D6_scheduled_never_ran",
             "D4_orphan_rule", "D2_timeout_clipped", "D1_recursion",
             "D8_errors", "D7_reserved_zero", "D3_double_fire",
             "D10_runtime", "D12_stale_env", "D9_no_dlq", "D13_dead"]
    for k in order:
        v = OUT["defects"].get(k)
        if not v:
            continue
        n = (v[0].get("count") if k == "D13_dead" else len(v))
        rep.log("   %-26s %s" % (k, n))
    (ROOT / "aws" / "ops" / "reports" / "4233_fleet_integrity_audit.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote 4233_fleet_integrity_audit.json")
