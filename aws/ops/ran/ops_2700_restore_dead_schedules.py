"""ops 2700 — restore the 3 dropped EventBridge schedules found by the fleet audit
(ops 2699) and refresh the rotten feeds NOW.

Broken loops (config.json declares the schedule; live rule vanished — the known
deploy-lambdas schedule-drop mode):
  justhodl-self-improvement    cascade-calibration.json  29d old, 9 consumers (best-setups!)
  justhodl-trade-tickets       trade-tickets.json        28d old, 9 consumers
  justhodl-polygon-options-flow polygon-options-flow.json 12d old, 9 consumers
Also: sync-refresh financial-secretary (chain via liquidity-agent silent 34d) and
report rule states for liquidity-agent / calibration-fleet / snapshotter for the
next decision. Report: aws/ops/reports/2700_restore_schedules.json.
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=420, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
NOW = datetime.now(timezone.utc)
R = {"ops": 2700, "ts": NOW.isoformat(), "fixed": {}, "chain_report": {}}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

def age_h(key):
    try:
        lm = s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
        return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)
    except Exception:
        return None

TARGETS = [
    ("justhodl-self-improvement",     "data/cascade-calibration.json"),
    ("justhodl-trade-tickets",        "data/trade-tickets.json"),
    ("justhodl-polygon-options-flow", "data/polygon-options-flow.json"),
]

sect("1/4 RECREATE RULES FROM config.json")
for fn, feed in TARGETS:
    cfg = json.load(open("aws/lambdas/%s/config.json" % fn))
    sc = cfg["schedule"]
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)
    rule_arn = ev.put_rule(Name=sc["name"], ScheduleExpression=sc["expression"],
                           State="ENABLED", Description=sc.get("description", ""))["RuleArn"]
    try:
        lam.add_permission(FunctionName=fn, StatementId="evt-" + sc["name"],
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=sc["name"], Targets=[{"Id": "1", "Arn": arn}])
    R["fixed"][fn] = {"rule": sc["name"], "expr": sc["expression"], "before_age_h": age_h(feed)}
    print("  %-32s rule=%s expr=%s before=%sh" % (fn, sc["name"], sc["expression"], R["fixed"][fn]["before_age_h"]))

sect("2/4 REFRESH FEEDS NOW (sync invokes)")
for fn, feed in TARGETS:
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    err = r.get("FunctionError")
    body = (r["Payload"].read() or b"")[:180].decode("utf-8", "ignore")
    a = age_h(feed)
    R["fixed"][fn].update({"invoke_error": err, "invoke_head": body, "after_age_h": a})
    print("  %-32s err=%s after=%sh  %s" % (fn, err, a, body[:90]))

sect("3/4 SECRETARY + CHAIN STATES")
r = lam.invoke(FunctionName="justhodl-financial-secretary", InvocationType="RequestResponse")
R["chain_report"]["financial_secretary"] = {
    "invoke_error": r.get("FunctionError"),
    "head": (r["Payload"].read() or b"")[:180].decode("utf-8", "ignore"),
    "fred_cache_after_h": age_h("data/fred-cache-secretary.json")}
print("  financial-secretary:", json.dumps(R["chain_report"]["financial_secretary"])[:200])
for probe in ("liquidity-agent", "calibration-fleet", "history-snapshotter", "ka-metrics"):
    hits = []
    for pg in ev.get_paginator("list_rules").paginate():
        for rl in pg["Rules"]:
            if probe.replace("-", "") in rl["Name"].replace("-", "").lower():
                hits.append({"rule": rl["Name"], "state": rl.get("State"), "expr": rl.get("ScheduleExpression")})
    R["chain_report"][probe] = hits or "NO_RULE"
    print("  %-22s %s" % (probe, json.dumps(hits) if hits else "NO_RULE"))

sect("4/4 ASSERT + REPORT")
for fn, feed in TARGETS:
    fx = R["fixed"][fn]
    assert not fx["invoke_error"], "%s invoke errored" % fn
    assert fx["after_age_h"] is not None and fx["after_age_h"] < 2, "%s feed not refreshed: %s" % (fn, fx["after_age_h"])
print("  HARD ASSERTS PASSED — all three feeds fresh")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2700_restore_schedules.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
for fn in ("justhodl-best-setups", "justhodl-master-ranker"):
    lam.invoke(FunctionName=fn, InvocationType="Event")
    print("  retriggered", fn)
print("\nOPS 2700 COMPLETE")
