"""ops/822 - edge-engine schedule repair + empty-rule triage.

ops/821 reclaimed 18 orphan rules (cron firing into deleted Lambdas),
leaving the bus at 282/300. That census also surfaced 14 EMPTY rules -
cron rules with zero targets, firing on schedule into nothing. ops/821
deliberately did not touch them (a zero-target rule could be a sibling
pipeline mid-wire); this script triages them now.

Two jobs:
  1. EDGE ENGINE - 'justhodl-edge-engine-6h' is one of the empty rules and
     the edge engine is a core signal Lambda. This checks whether
     justhodl-edge-engine is scheduled by ANY rule. If it is NOT, the empty
     rule is re-wired to invoke it (rate(6 hours) is already the correct
     cadence) - repairing a silently unscheduled engine. If the engine IS
     scheduled elsewhere, the empty rule is a redundant leftover and is
     deleted.
  2. EMPTY-RULE TRIAGE - every other empty rule is classified by matching
     its identity tokens against the live Lambda inventory. DISABLED empty
     rules, and ENABLED empty rules with no plausible live Lambda to point
     at, are pure dead weight and are deleted. ENABLED empty rules that DO
     have a plausible matching live Lambda are reported untouched - they
     may be a real unscheduled engine that needs a deliberate re-wire.
"""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 5})
events = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ACCT = "857687956942"
REGION = "us-east-1"

report = {"ops": 822, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "edge-engine schedule repair + empty-rule triage"}

# ---- inventory: live Lambdas --------------------------------------------
live_fns = set()
marker = None
while True:
    kw = {"MaxItems": 50}
    if marker:
        kw["Marker"] = marker
    resp = lam.list_functions(**kw)
    for f in resp.get("Functions", []):
        live_fns.add(f["FunctionName"])
    marker = resp.get("NextMarker")
    if not marker:
        break

# ---- inventory: all rules + which Lambda each rule schedules ------------
all_rules = []
tok = None
while True:
    kw = {"Limit": 100}
    if tok:
        kw["NextToken"] = tok
    resp = events.list_rules(**kw)
    all_rules.extend(resp.get("Rules", []))
    tok = resp.get("NextToken")
    if not tok:
        break


def fn_from_arn(arn):
    if ":function:" not in arn:
        return None
    return arn.split(":function:", 1)[1].split(":")[0]


fn_scheduled_by = {}
empty_rules = []
for r in all_rules:
    nm = r["Name"]
    try:
        tg = events.list_targets_by_rule(Rule=nm).get("Targets", [])
    except Exception:
        tg = []
    if not tg:
        empty_rules.append(r)
        continue
    for t in tg:
        fn = fn_from_arn(t.get("Arn", ""))
        if fn:
            fn_scheduled_by.setdefault(fn, []).append(nm)

# ---- JOB 1: edge engine schedule repair ---------------------------------
EDGE_FN = "justhodl-edge-engine"
EDGE_RULE = "justhodl-edge-engine-6h"
edge = {"engine_live": EDGE_FN in live_fns,
        "scheduled_by": sorted(fn_scheduled_by.get(EDGE_FN, [])),
        "empty_rule_present": any(r["Name"] == EDGE_RULE for r in all_rules)}

if not edge["engine_live"]:
    edge["action"] = "justhodl-edge-engine not found - nothing to schedule"
elif edge["scheduled_by"]:
    if edge["empty_rule_present"]:
        try:
            events.delete_rule(Name=EDGE_RULE)
            edge["rule_deleted"] = True
            edge["action"] = ("edge engine already scheduled by %s - deleted "
                              "the redundant empty rule %s"
                              % (edge["scheduled_by"], EDGE_RULE))
        except Exception as e:
            edge["action"] = "delete redundant rule failed: " + str(e)[:160]
    else:
        edge["action"] = "edge engine scheduled; no empty rule to clean"
else:
    # engine exists but is scheduled NOWHERE -> repair
    if edge["empty_rule_present"]:
        try:
            arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, EDGE_FN)
            events.put_targets(Rule=EDGE_RULE,
                               Targets=[{"Id": "edge-engine", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=EDGE_FN,
                    StatementId=EDGE_RULE + "-invoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:%s:%s:rule/%s"
                              % (REGION, ACCT, EDGE_RULE))
            except lam.exceptions.ResourceConflictException:
                pass
            after = [fn_from_arn(t["Arn"]) for t in
                     events.list_targets_by_rule(Rule=EDGE_RULE).get("Targets", [])]
            edge["repaired"] = EDGE_FN in after
            edge["action"] = ("edge engine was UNSCHEDULED - re-wired %s to "
                              "invoke %s (rate 6h)" % (EDGE_RULE, EDGE_FN))
        except Exception as e:
            edge["action"] = "re-wire failed: " + str(e)[:160]
    else:
        edge["action"] = ("edge engine unscheduled AND no rule exists - "
                           "needs a fresh schedule (manual)")
report["edge_engine"] = edge

# ---- JOB 2: empty-rule triage -------------------------------------------
GENERIC = {"update", "schedule", "scheduled", "daily", "weekly", "hourly",
           "monthly", "monitor", "check", "auto", "critical", "collection",
           "report", "reports", "test", "cron", "rate", "engine", "agent",
           "intelligence", "training", "predictions", "signals", "analysis",
           "data", "combined", "realtime"}


def content_tokens(name):
    out = set()
    for t in name.lower().replace("_", "-").split("-"):
        if not t or t.isdigit() or t in GENERIC or t == "justhodl":
            continue
        out.add(t)
    return out


live_tok = {fn: content_tokens(fn) for fn in live_fns}

deleted_empty, flagged_empty, delete_failed = [], [], []
for r in empty_rules:
    nm = r["Name"]
    if nm == EDGE_RULE:
        continue
    state = r.get("State", "")
    sched = r.get("ScheduleExpression", "")
    rtoks = content_tokens(nm)
    cands = sorted(fn for fn, ft in live_tok.items() if rtoks & ft)
    safe_delete = (state == "DISABLED") or (not rtoks) or (not cands)
    if safe_delete:
        try:
            tg = events.list_targets_by_rule(Rule=nm).get("Targets", [])
            if tg:                       # raced - no longer empty, leave it
                flagged_empty.append({"name": nm, "state": state,
                                       "note": "no longer empty - skipped"})
                continue
            events.delete_rule(Name=nm)
            deleted_empty.append({
                "name": nm, "state": state, "schedule": sched,
                "reason": ("disabled empty rule" if state == "DISABLED"
                           else "enabled empty rule, no live Lambda matches "
                                "its identity")})
            time.sleep(0.2)
        except Exception as e:
            delete_failed.append({"name": nm, "error": str(e)[:160]})
    else:
        flagged_empty.append({
            "name": nm, "state": state, "schedule": sched,
            "candidate_lambdas": cands[:6],
            "note": "enabled empty rule with a plausible target - review / "
                    "re-wire manually rather than delete"})

# ---- authoritative final count ------------------------------------------
final_rules = []
tok = None
while True:
    kw = {"Limit": 100}
    if tok:
        kw["NextToken"] = tok
    resp = events.list_rules(**kw)
    final_rules.extend(resp.get("Rules", []))
    tok = resp.get("NextToken")
    if not tok:
        break
total_after = len(final_rules)

report["empty_rules_seen"] = len(empty_rules)
report["deleted_empty"] = deleted_empty
report["flagged_empty"] = flagged_empty
report["delete_failed"] = delete_failed
report["rules_before_822"] = len(all_rules)
report["rules_after_822"] = total_after
report["headroom_after"] = max(0, 300 - total_after)
report["slots_freed_822"] = len(all_rules) - total_after
report["checks"] = {
    "inventory_ran": len(live_fns) > 0 and len(all_rules) > 0,
    "edge_engine_resolved": edge.get("repaired", False)
        or edge.get("rule_deleted", False)
        or "scheduled" in edge.get("action", "")
        or "not found" in edge.get("action", ""),
    "no_delete_errors": len(delete_failed) == 0,
}
report["all_pass"] = all(report["checks"].values())
report["verdict"] = (
    "Edge engine: %s | Empty-rule triage: deleted %d dead rule(s), flagged "
    "%d for review. EventBridge bus now at %d/300 (%d slots free)."
    % (edge.get("action", "n/a"), len(deleted_empty), len(flagged_empty),
       total_after, report["headroom_after"]))

with open("aws/ops/reports/822_empty_rule_triage.json", "w") as fh:
    json.dump(report, fh, indent=2)
print(json.dumps(report, indent=2))
