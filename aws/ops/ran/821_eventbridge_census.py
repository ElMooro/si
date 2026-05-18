"""ops/821 - EventBridge rule census + safe orphan reclamation.

ops/820 confirmed the AWS account sits at 300/300 EventBridge rules - the
default per-bus cap. New scheduled Lambdas can no longer get a dedicated
rule (this is why ops/819 had to co-host dividend-growth on a sibling rule).

Before recommending a structural migration this script takes the cheap,
safe win first: it audits every rule on the default bus and reclaims the
ones that are pure dead weight - SCHEDULED rules whose every target points
at a Lambda that no longer exists. Retired engines (boom-radar,
catch-up-radar, and any other deprecated Lambda) leave these orphan rules
behind; they fire on a cron into a function that 404s, doing nothing, while
still consuming one of the 300 slots.

Reclamation is deliberately conservative. A rule is auto-deleted ONLY when:
  * it has a ScheduleExpression (a cron rule, not an event-pattern rule),
  * it is not AWS-managed (no ManagedBy field),
  * it has at least one target (a zero-target rule could be a sibling
    pipeline mid-wire - left alone to avoid a race),
  * every one of its targets is a Lambda ARN, and
  * every one of those Lambda functions is absent from the live function
    list.
Everything else - disabled rules with live targets, event-pattern rules,
non-Lambda targets, managed rules, empty rules - is reported untouched for
human review.
"""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 5})
events = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

report = {"ops": 821, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "EventBridge rule census + safe orphan reclamation"}

# ---- 1. enumerate every live Lambda function name --------------------------
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

# ---- 2. enumerate every EventBridge rule on the default bus ----------------
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
    """arn:aws:lambda:region:acct:function:NAME[:version|alias] -> NAME."""
    if ":function:" not in arn:
        return None
    return arn.split(":function:", 1)[1].split(":")[0]


healthy, orphan, disabled, non_lambda, empty, managed = [], [], [], [], [], []

for r in all_rules:
    name = r["Name"]
    sched = r.get("ScheduleExpression", "")
    state = r.get("State", "")
    has_event_pattern = bool(r.get("EventPattern"))
    is_managed = bool(r.get("ManagedBy"))
    try:
        tg = events.list_targets_by_rule(Rule=name).get("Targets", [])
    except Exception:
        tg = []
    lambda_targets, other_targets = [], 0
    for t in tg:
        fn = fn_from_arn(t.get("Arn", ""))
        if fn:
            lambda_targets.append(fn)
        else:
            other_targets += 1
    dead = [f for f in lambda_targets if f not in live_fns]
    rec = {"name": name, "schedule": sched, "state": state,
           "event_pattern": has_event_pattern, "managed": is_managed,
           "lambda_targets": lambda_targets, "dead_targets": dead,
           "n_targets": len(tg), "non_lambda_targets": other_targets}
    if is_managed:
        managed.append(rec)
    elif other_targets > 0:
        non_lambda.append(rec)
    elif len(tg) == 0:
        empty.append(rec)
    elif lambda_targets and len(dead) == len(lambda_targets):
        orphan.append(rec)            # every Lambda target dead
    elif state == "DISABLED":
        disabled.append(rec)
    else:
        healthy.append(rec)

# ---- 3. reclaim: delete unambiguous scheduled orphan rules -----------------
reclaimed, reclaim_failed, event_pattern_orphans = [], [], []
for rec in orphan:
    nm = rec["name"]
    if rec["event_pattern"]:
        event_pattern_orphans.append(nm)      # report only, never auto-delete
        continue
    if not rec["schedule"]:
        event_pattern_orphans.append(nm)      # no cron + no pattern -> review
        continue
    try:
        ids = [t["Id"] for t in
               events.list_targets_by_rule(Rule=nm).get("Targets", [])]
        if ids:
            events.remove_targets(Rule=nm, Ids=ids)
        events.delete_rule(Name=nm)
        reclaimed.append(nm)
        time.sleep(0.25)
    except Exception as e:
        reclaim_failed.append({"name": nm, "error": str(e)[:200]})

total_before = len(all_rules)
total_after = total_before - len(reclaimed)

report["census"] = {
    "total_rules_before": total_before,
    "live_lambda_functions": len(live_fns),
    "healthy": len(healthy),
    "orphan_scheduled": len([r for r in orphan if r["schedule"]
                             and not r["event_pattern"]]),
    "orphan_event_pattern_or_other": len(event_pattern_orphans),
    "disabled_with_live_target": len(disabled),
    "empty_no_targets": len(empty),
    "non_lambda_targets": len(non_lambda),
    "aws_managed": len(managed),
}
report["reclaimed"] = sorted(reclaimed)
report["reclaim_failed"] = reclaim_failed
report["event_pattern_orphans_flagged"] = sorted(event_pattern_orphans)
report["orphans_detail"] = [
    {"name": r["name"], "schedule": r["schedule"], "state": r["state"],
     "dead_targets": r["dead_targets"]} for r in orphan]
report["disabled_detail"] = [
    {"name": r["name"], "schedule": r["schedule"],
     "targets": r["lambda_targets"]} for r in disabled][:80]
report["empty_detail"] = [
    {"name": r["name"], "schedule": r["schedule"], "state": r["state"]}
    for r in empty][:40]
report["total_rules_after"] = total_after
report["slots_freed"] = len(reclaimed)
report["headroom_after"] = max(0, 300 - total_after)
report["checks"] = {
    "census_ran": total_before > 0,
    "live_fns_enumerated": len(live_fns) > 0,
    "no_reclaim_errors": len(reclaim_failed) == 0,
}
report["all_pass"] = all(report["checks"].values())

if report["slots_freed"] > 0:
    report["verdict"] = (
        "RECLAIMED %d orphan EventBridge rule(s); bus now at %d/300 "
        "(%d slots free). Orphans were cron rules firing into deleted "
        "Lambdas. If headroom is still tight, migrate scheduled "
        "invocations to EventBridge Scheduler (no 300-rule cap)."
        % (report["slots_freed"], total_after, report["headroom_after"]))
else:
    report["verdict"] = (
        "No orphan rules found - all %d rules have live or managed "
        "targets. The 300-rule cap is real and structural: migrate "
        "scheduled invocations to EventBridge Scheduler (no cap) or "
        "consolidate Lambdas that share a cron onto shared multi-target "
        "rules to free slots." % total_before)

with open("aws/ops/reports/821_eventbridge_census.json", "w") as fh:
    json.dump(report, fh, indent=2)
print(json.dumps(report, indent=2))
