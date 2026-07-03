"""ops 2788 — create the missing EventBridge daily schedule for earnings-iv-crush (was unscheduled -> stale)."""
import os, json
from datetime import datetime, timezone
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
fn = "justhodl-earnings-iv-crush"
RULE = "justhodl-earnings-iv-crush-daily"
CRON = "cron(50 21 * * ? *)"  # 21:50 UTC daily, post-close, staggered from dealer-gex 20:35 / settlement 21:30 / dark-pool 21:40
R = {"ops": 2788, "ts": datetime.now(timezone.utc).isoformat()}
arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
print("lambda arn:", arn)
rule_arn = ev.put_rule(Name=RULE, ScheduleExpression=CRON, State="ENABLED",
                       Description="Daily post-close earnings IV-crush scan (added ops 2788 — engine was unscheduled, feed 7d stale)")["RuleArn"]
print("put_rule:", rule_arn)
try:
    lam.add_permission(FunctionName=fn, StatementId="eivc-eventbridge-daily",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=rule_arn)
    print("add_permission: granted")
except lam.exceptions.ResourceConflictException:
    print("add_permission: already exists (ok)")
ev.put_targets(Rule=RULE, Targets=[{"Id": "eivc-target", "Arn": arn}])
print("put_targets: done")
# verify
d = ev.describe_rule(Name=RULE); tg = ev.list_targets_by_rule(Rule=RULE)["Targets"]
R["rule"] = {"name": RULE, "state": d["State"], "sched": d["ScheduleExpression"], "targets": [t["Arn"] for t in tg]}
print("VERIFY: rule", d["State"], d["ScheduleExpression"], "-> targets", [t["Arn"].split(":")[-1] for t in tg])
assert d["State"] == "ENABLED" and tg, "schedule not active"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2788_eivc_schedule.json","w"), indent=1, default=str)
print("OPS 2788 COMPLETE — earnings-iv-crush now scheduled daily 21:50 UTC")
