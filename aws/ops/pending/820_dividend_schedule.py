"""ops/820 — schedule the dividend-growth engine + audit the EventBridge
rule-limit saturation.

ops/819 could not create the dividend-growth-daily rule: the account has
hit the EventBridge 300-rule-per-bus limit. Rather than request a limit
bump, this co-hosts dividend-growth on its sibling rule capital-return-daily
(both are shareholder-return screens that read the same screener feed and
should refresh together) - an EventBridge rule supports up to 5 targets, so
this needs ZERO new rules. It also audits the rule population so the
saturation can be addressed strategically (migration to EventBridge
Scheduler, which has no 300-rule cap).
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
events = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

FN = "justhodl-dividend-growth"
HOST_RULE = "capital-return-daily"      # sibling shareholder-return screen
report = {"ops": 820, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Schedule dividend-growth via shared rule + audit "
                     "EventBridge rule saturation"}

# ---- audit: how many rules, how many are Lambda-cron rules ----
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
scheduled = [r for r in all_rules if r.get("ScheduleExpression")]
report["rule_audit"] = {
    "total_rules": len(all_rules),
    "scheduled_rules": len(scheduled),
    "event_pattern_rules": len(all_rules) - len(scheduled),
    "note": ("EventBridge default cap is 300 rules per event bus. The "
             "account is saturated - new per-Lambda rules now fail. "
             "Strategic fix: migrate scheduled invocations to EventBridge "
             "Scheduler (no 300 cap) or consolidate onto shared multi-target "
             "rules."),
}

# ---- locate the host rule + its current targets ----
fn_arn = None
try:
    fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
except Exception as e:
    report["fn_arn_err"] = str(e)[:160]

host_ok = False
try:
    rd = events.describe_rule(Name=HOST_RULE)
    host_arn = rd["Arn"]
    tgts = events.list_targets_by_rule(Rule=HOST_RULE).get("Targets", [])
    report["host_rule"] = {
        "name": HOST_RULE,
        "schedule": rd.get("ScheduleExpression"),
        "state": rd.get("State"),
        "existing_targets": [t.get("Arn", "").split(":")[-1] for t in tgts],
        "n_targets": len(tgts),
    }
    already = any(t.get("Arn") == fn_arn for t in tgts)
    if already:
        host_ok = True
        report["wire"] = "dividend-growth already a target of " + HOST_RULE
    elif len(tgts) >= 5:
        report["wire"] = ("ERROR host rule already has 5 targets - pick "
                           "another rule")
    elif fn_arn:
        # grant the rule permission to invoke the Lambda
        try:
            lam.add_permission(
                FunctionName=FN,
                StatementId=HOST_RULE + "-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=host_arn)
        except lam.exceptions.ResourceConflictException:
            pass
        used_ids = {t.get("Id") for t in tgts}
        new_id = next(str(i) for i in range(1, 99) if str(i) not in used_ids)
        events.put_targets(Rule=HOST_RULE,
                           Targets=[{"Id": new_id, "Arn": fn_arn}])
        host_ok = True
        report["wire"] = ("added %s as target #%s on %s"
                          % (FN, new_id, HOST_RULE))
    else:
        report["wire"] = "ERROR no function ARN resolved"
except events.exceptions.ResourceNotFoundException:
    report["wire"] = "ERROR host rule %s not found" % HOST_RULE
except Exception as e:
    report["wire"] = "ERROR %s: %s" % (type(e).__name__, str(e)[:160])

# ---- verify the target is really on the rule ----
verified = False
try:
    time.sleep(2)
    tgts2 = events.list_targets_by_rule(Rule=HOST_RULE).get("Targets", [])
    verified = any(t.get("Arn") == fn_arn for t in tgts2)
    report["host_targets_after"] = [t.get("Arn", "").split(":")[-1]
                                    for t in tgts2]
except Exception as e:
    report["verify_err"] = str(e)[:160]

checks = {
    "fn_arn_resolved": fn_arn is not None,
    "host_rule_found": "host_rule" in report,
    "wired_ok": host_ok,
    "target_verified": verified,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "DIVIDEND-GROWTH SCHEDULED - co-hosted on %s (%s), auto-refreshes daily. "
    "Account is at %d/300 EventBridge rules; recommend migrating scheduled "
    "invocations to EventBridge Scheduler to remove the cap."
    % (HOST_RULE,
       report.get("host_rule", {}).get("schedule", "?"),
       len(all_rules))
    if report["all_pass"] else "REVIEW - see checks[]/wire")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/820_dividend_schedule.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/820_dividend_schedule.json")
