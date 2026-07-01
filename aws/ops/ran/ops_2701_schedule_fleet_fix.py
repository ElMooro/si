"""ops 2701 — fleet schedule enforcement (root cause of ops-2700 failure).

Discovery chain: fleet audit (2699) -> 3 dead loops -> restore attempt (2700)
crashed on ValidationException because the config crons were ILLEGAL EventBridge
syntax ('* * MON-FRI'; DOM must be '?') — meaning these schedules were NEVER
created; deploy-lambdas failed silently every time. Fleet scan found 12 configs
with the pattern (now corrected in-repo). Separately, run-ops.yml swallowed the
crash (pipe masked exit code) and reported success — that gate is fixed in this
same push, so THIS script's asserts are the first with real teeth.

This op: for each of the 12 engines, ensure an ENABLED rule exists per the
corrected config (create if missing), refresh the rotten feeds, hard-assert the
3 critical ones. Report: aws/ops/reports/2701_schedule_fleet_fix.json.
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=420, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2701, "ts": datetime.now(timezone.utc).isoformat(), "engines": {}}

FNS = ["justhodl-cascade-recalibrator", "justhodl-cascade-validator", "justhodl-digest-trends-ai",
       "justhodl-page-ai-commentary", "justhodl-pnl-tracker", "justhodl-polygon-futures-curves",
       "justhodl-polygon-fx-regime", "justhodl-polygon-options-flow", "justhodl-prediction-snapshotter",
       "justhodl-self-improvement", "justhodl-trade-ticket-monitor", "justhodl-trade-tickets"]
CRITICAL = {"justhodl-self-improvement": "data/cascade-calibration.json",
            "justhodl-trade-tickets": "data/trade-tickets.json",
            "justhodl-polygon-options-flow": "data/polygon-options-flow.json"}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

def age_h(key):
    try:
        lm = s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
        return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)
    except Exception:
        return None

sect("1/4 MAP LIVE RULES -> FN")
fn_rules = {}
for pg in ev.get_paginator("list_rules").paginate():
    for r in pg["Rules"]:
        if not r.get("ScheduleExpression"):
            continue
        try:
            tg = ev.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
        except Exception:
            continue
        for t in tg:
            if ":function:" in t.get("Arn", ""):
                fn = t["Arn"].split(":function:")[1].split(":")[0]
                fn_rules.setdefault(fn, []).append({"rule": r["Name"], "state": r.get("State"), "expr": r["ScheduleExpression"]})
print("  scheduled fns:", len(fn_rules))

sect("2/4 ENFORCE THE 12")
for fn in FNS:
    cfg = json.load(open("aws/lambdas/%s/config.json" % fn))
    sc = cfg["schedule"]
    live = [x for x in fn_rules.get(fn, []) if x["state"] == "ENABLED"]
    rec = {"config_expr": sc["expression"], "had_enabled_rule": bool(live),
           "live_rules": live[:2], "created": False}
    if not live:
        rule_arn = ev.put_rule(Name=sc["name"], ScheduleExpression=sc["expression"],
                               State="ENABLED", Description=sc.get("description", ""))["RuleArn"]
        try:
            lam.add_permission(FunctionName=fn, StatementId="evt-" + sc["name"],
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=rule_arn)
        except lam.exceptions.ResourceConflictException:
            pass
        ev.put_targets(Rule=sc["name"], Targets=[{"Id": "1", "Arn":
            "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)}])
        rec["created"] = True
        if fn not in CRITICAL:
            lam.invoke(FunctionName=fn, InvocationType="Event")  # refresh rot async
    R["engines"][fn] = rec
    print("  %-34s had_rule=%-5s created=%-5s expr=%s" % (fn.replace("justhodl-", ""),
          rec["had_enabled_rule"], rec["created"], sc["expression"]))

sect("3/4 REFRESH + HARD-ASSERT CRITICAL FEEDS")
for fn, feed in CRITICAL.items():
    before = age_h(feed)
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    err = r.get("FunctionError")
    head = (r["Payload"].read() or b"")[:160].decode("utf-8", "ignore")
    after = age_h(feed)
    R["engines"][fn].update({"before_age_h": before, "after_age_h": after,
                             "invoke_error": err, "invoke_head": head})
    print("  %-30s %sh -> %sh err=%s %s" % (fn.replace("justhodl-", ""), before, after, err, head[:80]))
    assert not err, "%s errored: %s" % (fn, head)
    assert after is not None and after < 2, "%s feed not refreshed (%s)" % (fn, after)
print("  HARD ASSERTS PASSED")

sect("4/4 REPORT + CONSUMERS")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2701_schedule_fleet_fix.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2701_schedule_fleet_fix.json")
for fn in ("justhodl-best-setups", "justhodl-master-ranker"):
    lam.invoke(FunctionName=fn, InvocationType="Event")
    print("  retriggered", fn)
print("\nOPS 2701 COMPLETE")
