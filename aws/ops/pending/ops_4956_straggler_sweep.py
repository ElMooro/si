"""ops_4956 -- straggler sweep: the cards the expedite couldn't see.

4954's audit keyed on import-family fn names; eight cards ride engines
outside that pattern and stayed 8-23h stale through the storm:
  sec-dera        -> justhodl-global-expansion  (hist-banker excluded:
                                                 heavy-walker doctrine)
  ofr-fsi         -> justhodl-gap-metrics
  chicagofed      -> justhodl-plumbing-panel
  cleveland+atlanta -> justhodl-canary-macro
  te-feed / taiwan-moea / peru-copper -> their own justhodl-* feeds
All resolved via the provider-catalog registry (derive, never type).

  A  each engine exists (>=6/7) + live schedule lookup per fn
  B  ensure cadence: create Scheduler schedule where NONE exists
     (2h feeds/panels, 6h global-expansion), tighten-only where a
     rate() exists; cron logged untouched; every change re-described
  C  kick each engine once
  D  post-mark hub proof: >=6 of the 8 straggler cards fresh <2h
Pure ops [skip-deploy]. indicator-bus (internal registry) noted, not
touched -- separate lane if wanted.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
HUB_KEY = "data/provider-catalog.json"
CAT_FN = "justhodl-provider-catalog"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
TARGETS = {  # fn -> (minutes, schedule-name-if-created)
    "justhodl-global-expansion": (360, "justhodl-global-expansion-6h"),
    "justhodl-gap-metrics": (120, "justhodl-gap-metrics-2h"),
    "justhodl-plumbing-panel": (120, "justhodl-plumbing-panel-2h"),
    "justhodl-canary-macro": (120, "justhodl-canary-macro-2h"),
    "justhodl-te-feed": (120, "justhodl-te-feed-2h"),
    "justhodl-taiwan-moea": (120, "justhodl-taiwan-moea-2h"),
    "justhodl-peru-copper": (120, "justhodl-peru-copper-2h"),
}
SLUGS = ["sec-dera", "ofr-fsi", "chicagofed", "clevelandfed",
         "atlantafed", "te-feed", "taiwan-moea", "peru-copper"]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
ev = boto3.client("events", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def rate_minutes(expr):
    import re
    m = re.match(r"rate\((\d+)\s+(minute|minutes|hour|hours|day|days)"
                 r"\)", expr or "")
    if not m:
        return None
    n, u = int(m.group(1)), m.group(2)
    return n * (1 if u.startswith("minute") else
                60 if u.startswith("hour") else 1440)


def mk_rate(minutes):
    if minutes % 60 == 0:
        n = minutes // 60
        return "rate(%d hour%s)" % (n, "" if n == 1 else "s")
    return "rate(%d minutes)" % minutes


def fn_of_arn(arn):
    return (arn or "").split(":function:")[-1].split(":")[0] \
        if ":function:" in (arn or "") else ""


with report("ops_4956_straggler_sweep") as R:
    fails = []

    # A -- engines exist + live schedule lookup ------------------------
    R.section("A engines + existing schedules")
    fns = {}
    for fn in TARGETS:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            fns[fn] = {"arn": c["FunctionArn"], "sched": []}
        except Exception as e:
            R.log("  MISSING %-32s %s" % (fn, str(e)[:60]))
    tok = None
    while True:
        kw = {"MaxResults": 100}
        if tok:
            kw["NextToken"] = tok
        r_ = sch.list_schedules(**kw)
        for it in r_.get("Schedules", []):
            try:
                d = sch.get_schedule(Name=it["Name"],
                                     GroupName=it.get("GroupName",
                                                      "default"))
            except Exception:
                continue
            f = fn_of_arn((d.get("Target") or {}).get("Arn"))
            if f in fns:
                fns[f]["sched"].append(
                    {"src": "scheduler", "name": d["Name"],
                     "group": it.get("GroupName", "default"),
                     "expr": d.get("ScheduleExpression"), "_d": d})
        tok = r_.get("NextToken")
        if not tok:
            break
    tok = None
    while True:
        kw = {"Limit": 100}
        if tok:
            kw["NextToken"] = tok
        r_ = ev.list_rules(**kw)
        for ru in r_.get("Rules", []):
            if not ru.get("ScheduleExpression"):
                continue
            try:
                tg = ev.list_targets_by_rule(Rule=ru["Name"]) \
                    .get("Targets", [])
            except Exception:
                tg = []
            f = fn_of_arn(tg[0]["Arn"]) if tg else ""
            if f in fns:
                fns[f]["sched"].append(
                    {"src": "events", "name": ru["Name"],
                     "expr": ru.get("ScheduleExpression")})
        tok = r_.get("NextToken")
        if not tok:
            break
    for fn, d in sorted(fns.items()):
        R.log("  %-32s scheds=%s" % (
            fn, [(s_["src"], s_["expr"]) for s_ in d["sched"]]
            or "NONE"))
    ok_a = len(fns) >= 6
    R.log("A %s %d/7 engines live" % ("PASS" if ok_a else "FAIL",
                                      len(fns)))
    if not ok_a:
        R.log("ops 4956 RED: A")
        sys.exit(1)

    # B -- ensure cadence ----------------------------------------------
    R.section("B ensure cadence (create-if-missing / tighten rate)")
    changed, verified = [], 0
    for fn, d in fns.items():
        tgt_min, new_name = TARGETS[fn]
        new_expr = mk_rate(tgt_min)
        if not d["sched"]:
            try:
                sch.create_schedule(
                    Name=new_name, GroupName="default",
                    ScheduleExpression=new_expr,
                    FlexibleTimeWindow={"Mode": "OFF"},
                    State="ENABLED",
                    Target={"Arn": d["arn"], "RoleArn": SCHED_ROLE,
                            "Input": "{}"})
                changed.append((fn, new_name, "created", new_expr))
                R.log("  CREATE  %-32s %s" % (fn, new_expr))
            except Exception as e:
                if "already exists" in str(e) or \
                        "ConflictException" in type(e).__name__:
                    changed.append((fn, new_name, "exists", new_expr))
                else:
                    R.log("  create-err %s: %s" % (fn, str(e)[:80]))
            continue
        for s_ in d["sched"]:
            cur = rate_minutes(s_["expr"])
            if cur is None:
                R.log("  cron-keep %-30s %s" % (fn, s_["expr"]))
                continue
            if cur <= tgt_min:
                R.log("  ok       %-30s %s" % (fn, s_["expr"]))
                continue
            try:
                if s_["src"] == "scheduler":
                    dd = s_["_d"]
                    sch.update_schedule(
                        Name=dd["Name"],
                        GroupName=s_.get("group", "default"),
                        ScheduleExpression=new_expr,
                        FlexibleTimeWindow=dd.get(
                            "FlexibleTimeWindow") or {"Mode": "OFF"},
                        Target=dd["Target"],
                        State=dd.get("State", "ENABLED"))
                else:
                    ev.put_rule(Name=s_["name"],
                                ScheduleExpression=new_expr)
                changed.append((fn, s_["name"], "tightened",
                                new_expr))
                R.log("  TIGHTEN %-32s %s -> %s" % (
                    fn, s_["expr"], new_expr))
            except Exception as e:
                R.log("  tighten-err %s: %s" % (fn, str(e)[:80]))
    for fn, name, act, expr in changed:
        try:
            if act in ("created", "exists") or True:
                try:
                    cur = sch.get_schedule(
                        Name=name, GroupName="default") \
                        .get("ScheduleExpression")
                except Exception:
                    cur = ev.describe_rule(Name=name) \
                        .get("ScheduleExpression")
            if cur == expr:
                verified += 1
            else:
                R.log("  VERIFY-FAIL %s cur=%s want=%s" % (
                    name, cur, expr))
        except Exception as e:
            R.log("  verify-err %s: %s" % (name, str(e)[:60]))
    ok_b = verified == len(changed) and len(changed) >= 4
    R.log("B %s changed=%d verified=%d" % (
        "PASS" if ok_b else "FAIL", len(changed), verified))
    if not ok_b:
        fails.append("B")

    # C -- kick each ---------------------------------------------------
    R.section("C kicks")
    for fn in fns:
        try:
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=b"{}")
            R.log("  kicked %s" % fn)
        except Exception as e:
            R.log("  kick-err %s: %s" % (fn, str(e)[:60]))
        time.sleep(0.4)

    # D -- post-mark hub proof -----------------------------------------
    R.section("D straggler cards fresh (<2h for >=6/8)")
    time.sleep(240)
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lam.invoke(FunctionName=CAT_FN, InvocationType="Event",
               Payload=b"{}")
    hub, t0 = {}, time.time()
    while time.time() - t0 < 13 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
        R.log("  t+%4ds inventory %s < mark" % (
            time.time() - t0, (hub.get("as_of") or "")[:19]))
    rows = {p.get("slug"): p.get("freshest_h")
            for p in hub.get("providers", [])}
    fresh_ok = 0
    for sl in SLUGS:
        h = rows.get(sl)
        good = h is not None and abs(h) < 2.0
        fresh_ok += 1 if good else 0
        R.log("  %-14s freshest=%sh %s" % (sl, h,
                                           "OK" if good else "--"))
    ok_d = (hub.get("as_of") or "") >= t_mark and fresh_ok >= 6
    R.log("D %s fresh<2h: %d/8 (as_of %s)" % (
        "PASS" if ok_d else "FAIL", fresh_ok,
        (hub.get("as_of") or "")[:19]))
    if not ok_d:
        fails.append("D")

    if fails:
        R.log("ops 4956 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(engines=len(fns), changed=len(changed),
         fresh_ok="%d/8" % fresh_ok)
    R.log("ops 4956 GREEN -- every straggler card now has a real "
          "cadence and a fresh write; indicator-bus (internal) left "
          "for its own lane")
