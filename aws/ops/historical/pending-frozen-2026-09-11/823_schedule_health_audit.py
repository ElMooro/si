"""ops/823 - system-wide schedule-health audit (READ-ONLY).

WHY NOW:
  In the last hours the EventBridge surface was mutated heavily by parallel
  work: ops 820 band-aided a shared rule, ops 821 reclaimed 18 orphan rules
  AND migrated an engine to EventBridge Scheduler, ops 822 triaged 14 empty
  rules and re-wired edge-engine. ops 822 already proved that a confluence of
  rule deletions can leave a CORE engine silently unscheduled. After that
  much churn the auto-update guarantee - the platform's hard requirement -
  must be re-verified end to end.

WHAT IT DOES (no mutations - pure audit):
  1. Inventories every justhodl-* Lambda.
  2. Inventories every classic EventBridge rule and its Lambda targets.
  3. Inventories every EventBridge Scheduler schedule and its Lambda target.
  4. Reads every local aws/lambdas/*/config.json and extracts what schedule
     it DECLARES (.schedule = classic rule, or .eventbridge_scheduler).
  5. Cross-checks declaration against live state. The critical finding is
     SILENTLY BROKEN: a Lambda whose config.json declares a schedule that
     does not actually exist / does not target it live -> its data is going
     stale with nobody watching.
  6. Reports declared-vs-live coverage, the silently-broken list, the set of
     live Lambdas with no schedule at all (mostly Function-URL engines -
     informational), and bus headroom.

VERDICT passes only if there are zero silently-broken engines.
"""
import glob
import json
import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 4})
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
scheduler = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {
    "ops": 823,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "System-wide schedule-health audit after the EventBridge churn",
}


def fn_from_arn(arn):
    if not arn or ":function:" not in arn:
        return None
    return arn.split(":function:")[-1].split(":")[0]


# --- 1. live Lambda inventory -----------------------------------------
live_fns = set()
paginator = lam.get_paginator("list_functions")
for page in paginator.paginate():
    for f in page.get("Functions", []):
        n = f["FunctionName"]
        if n.startswith("justhodl-"):
            live_fns.add(n)

# --- 2. classic EventBridge rules -> targets ---------------------------
classic_rules = []
tok = None
while True:
    kw = {"Limit": 100}
    if tok:
        kw["NextToken"] = tok
    resp = events.list_rules(**kw)
    classic_rules += resp.get("Rules", [])
    tok = resp.get("NextToken")
    if not tok:
        break

# fn -> set(rule_name) that target it ; rule -> set(fn)
fn_classic = {}
rule_targets = {}
for r in classic_rules:
    rn = r["Name"]
    try:
        tgts = events.list_targets_by_rule(Rule=rn).get("Targets", [])
    except Exception:
        tgts = []
    fns = {fn_from_arn(t.get("Arn")) for t in tgts}
    fns.discard(None)
    rule_targets[rn] = {"state": r.get("State"), "fns": sorted(fns),
                        "expr": r.get("ScheduleExpression")}
    for fn in fns:
        fn_classic.setdefault(fn, set()).add(rn)

# --- 3. EventBridge Scheduler schedules -> target ----------------------
scheduler_schedules = []
stok = None
while True:
    kw = {"MaxResults": 100}
    if stok:
        kw["NextToken"] = stok
    resp = scheduler.list_schedules(**kw)
    scheduler_schedules += resp.get("Schedules", [])
    stok = resp.get("NextToken")
    if not stok:
        break

fn_scheduler = {}
sched_detail = {}
for s in scheduler_schedules:
    name = s["Name"]
    grp = s.get("GroupName", "default")
    try:
        gs = scheduler.get_schedule(Name=name, GroupName=grp)
        tgt_fn = fn_from_arn((gs.get("Target") or {}).get("Arn"))
        sched_detail[name] = {"state": gs.get("State"),
                              "fn": tgt_fn,
                              "expr": gs.get("ScheduleExpression")}
        if tgt_fn:
            fn_scheduler.setdefault(tgt_fn, set()).add(name)
    except Exception as e:
        sched_detail[name] = {"error": str(e)[:120]}

# --- 4 + 5. declared (config.json) vs live -----------------------------
silently_broken = []
declared_classic = 0
declared_scheduler = 0
declared_ok = 0
for cpath in sorted(glob.glob("aws/lambdas/*/config.json")):
    try:
        conf = json.load(open(cpath))
    except Exception:
        continue
    fn = conf.get("function_name")
    if not fn:
        continue
    if "schedule" in conf:
        declared_classic += 1
        rn = (conf["schedule"] or {}).get("rule_name")
        live = rn in rule_targets and fn in rule_targets.get(rn, {}).get(
            "fns", [])
        # accept ANY rule covering it too (manual re-wires)
        live = live or (fn in fn_classic)
        if live:
            declared_ok += 1
        else:
            silently_broken.append({
                "fn": fn, "declares": "classic-rule", "rule_name": rn,
                "issue": "config declares a classic rule that does not "
                         "exist or does not target this Lambda live"})
    elif "eventbridge_scheduler" in conf:
        declared_scheduler += 1
        sn = (conf["eventbridge_scheduler"] or {}).get("schedule_name")
        live = sn in sched_detail and sched_detail[sn].get("fn") == fn
        live = live or (fn in fn_scheduler)
        if live:
            declared_ok += 1
        else:
            silently_broken.append({
                "fn": fn, "declares": "eventbridge-scheduler",
                "schedule_name": sn,
                "issue": "config declares an EventBridge Scheduler schedule "
                         "that does not exist or does not target this "
                         "Lambda live"})

# --- 6. live Lambdas with no schedule at all (informational) ----------
scheduled_fns = set(fn_classic) | set(fn_scheduler)
unscheduled = sorted(f for f in live_fns if f not in scheduled_fns)

report["inventory"] = {
    "live_justhodl_lambdas": len(live_fns),
    "classic_rules": len(classic_rules),
    "classic_rule_headroom": 300 - len(classic_rules),
    "scheduler_schedules": len(scheduler_schedules),
    "lambdas_scheduled_by_classic_rule": len(fn_classic),
    "lambdas_scheduled_by_eventbridge_scheduler": len(fn_scheduler),
    "lambdas_with_no_schedule": len(unscheduled),
}
report["declared_vs_live"] = {
    "config_declares_classic": declared_classic,
    "config_declares_scheduler": declared_scheduler,
    "declared_and_live_ok": declared_ok,
    "silently_broken_count": len(silently_broken),
}
report["silently_broken"] = silently_broken
report["unscheduled_lambdas"] = unscheduled
report["scheduler_schedules_detail"] = sched_detail

checks = {
    "no_silently_broken_engines": len(silently_broken) == 0,
    "scheduler_path_in_use": len(fn_scheduler) >= 1,
    "classic_rules_under_cap": len(classic_rules) < 300,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"SCHEDULE HEALTH GOOD - {len(live_fns)} justhodl Lambdas; "
    f"{len(fn_classic)} on classic rules, {len(fn_scheduler)} on EventBridge "
    f"Scheduler; every config-declared schedule is live. "
    f"{300 - len(classic_rules)} classic-rule slots free."
    if report["all_pass"] else
    f"ACTION NEEDED - {len(silently_broken)} engine(s) declare a schedule "
    f"that is not live (data going stale silently). See silently_broken[].")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/823_schedule_health_audit.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/823_schedule_health_audit.json")
