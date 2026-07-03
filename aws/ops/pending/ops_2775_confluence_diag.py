"""ops 2775 — diagnose (+ safe-fix) stale justhodl-options-confluence (last upd 06-23).
It feeds master-ranker + best-setups, so staleness propagates to the top rankers.
Config says hourly cron(20 * * * ? *). Check: rule exists/enabled/targeted?
dependency-feed freshness (9 feeds)? then INVOKE (safe — writes only its own feed)
and capture any error. If healthy, ensure schedule enabled so it stays fresh.
If it errors, capture the trace (no blind fix). Report: 2775_confluence_diag.json.
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
FN, RULE = "justhodl-options-confluence", "justhodl-options-confluence-hourly"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
R = {"ops": 2775, "ts": datetime.now(timezone.utc).isoformat()}
print("settling 10s…"); time.sleep(10)

# 1) function config
try:
    c = lam.get_function_configuration(FunctionName=FN)
    R["fn"] = {"state": c.get("State"), "runtime": c.get("Runtime"), "timeout": c.get("Timeout"),
               "mem": c.get("MemorySize"), "last_modified": c.get("LastModified"),
               "env_keys": sorted((c.get("Environment", {}) or {}).get("Variables", {}).keys())}
    print("fn:", json.dumps(R["fn"]))
except ClientError as e:
    R["fn"] = "ERR " + str(e)[:80]

# 2) EventBridge rule
try:
    rd = ev.describe_rule(Name=RULE)
    tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
    R["rule"] = {"exists": True, "state": rd.get("State"), "schedule": rd.get("ScheduleExpression"),
                 "n_targets": len(tg), "targets_fn": [t["Arn"].split(":")[-1] for t in tg]}
    print("rule:", json.dumps(R["rule"]))
except ClientError as e:
    R["rule"] = {"exists": False, "err": str(e)[:80]}
    print("rule: MISSING —", str(e)[:70])

# 3) dependency feed freshness
deps = ["catalyst-skew-premove", "dealer-gex", "earnings-iv-crush", "engine-trust",
        "options-analytics", "options-flow", "polygon-options-flow", "volatility-squeeze"]
now = datetime.now(timezone.utc)
R["deps"] = {}
for d in deps:
    k = "data/%s.json" % d
    try:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        age_h = (now - h["LastModified"]).total_seconds() / 3600.0
        R["deps"][d] = {"exists": True, "age_hours": round(age_h, 1), "bytes": h["ContentLength"]}
    except ClientError:
        R["deps"][d] = {"exists": False}
print("deps freshness:")
for d, v in R["deps"].items():
    print("   %-24s %s" % (d, ("MISSING" if not v.get("exists") else "%.1fh old (%dB)" % (v["age_hours"], v["bytes"]))))
stale_deps = [d for d, v in R["deps"].items() if not v.get("exists") or v.get("age_hours", 0) > 72]
R["stale_or_missing_deps"] = stale_deps

# 4) safe invoke (writes only its own feed) + capture error
print("invoking…")
try:
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    raw = resp["Payload"].read()
    fe = resp.get("FunctionError")
    R["invoke"] = {"function_error": fe, "payload_head": raw[:400].decode("utf-8", "ignore")}
    if fe:
        print("INVOKE ERROR:", raw[:300].decode("utf-8", "ignore"))
    else:
        print("invoke OK:", raw[:200].decode("utf-8", "ignore"))
        # confirm feed refreshed
        h = s3.head_object(Bucket=BUCKET, Key="data/options-confluence.json")
        R["feed_after"] = {"last_modified": h["LastModified"].isoformat(), "age_min": round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60, 1)}
        print("feed now:", R["feed_after"])
except Exception as e:
    R["invoke"] = {"exception": str(e)[:200]}
    print("invoke exception:", str(e)[:150])

# 5) if invoke healthy, ensure schedule enabled/targeted so it stays fresh
healthy = isinstance(R.get("invoke"), dict) and not R["invoke"].get("function_error") and not R["invoke"].get("exception")
if healthy:
    try:
        arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        ev.put_rule(Name=RULE, ScheduleExpression="cron(20 * * * ? *)", State="ENABLED",
                    Description="Options confluence synthesizer hourly")
        try:
            lam.add_permission(FunctionName=FN, StatementId="confluence-eventbridge", Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, RULE))
        except ClientError as e:
            if "ResourceConflict" not in str(e): raise
        ev.put_targets(Rule=RULE, Targets=[{"Id": "confluence", "Arn": arn}])
        rd2 = ev.describe_rule(Name=RULE)
        R["schedule_fix"] = {"state": rd2.get("State"), "schedule": rd2.get("ScheduleExpression"),
                             "n_targets": len(ev.list_targets_by_rule(Rule=RULE).get("Targets", []))}
        print("schedule ensured:", json.dumps(R["schedule_fix"]))
    except ClientError as e:
        R["schedule_fix"] = "ERR " + str(e)[:100]
else:
    # pull last CloudWatch error trace to explain the failure
    try:
        lg = "/aws/lambda/%s" % FN
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1).get("logStreams", [])
        if streams:
            evs = logs.get_log_events(logGroupName=lg, logStreamName=streams[0]["logStreamName"], limit=25, startFromHead=False).get("events", [])
            errln = [e["message"].strip() for e in evs if any(w in e["message"] for w in ("Error", "Traceback", "Task timed out", "Exception", "errorMessage"))]
            R["cloudwatch_tail"] = errln[-8:]
            print("CloudWatch error tail:")
            for l in errln[-8:]: print("   ", l[:160])
    except ClientError as e:
        R["cloudwatch_tail"] = "logs err " + str(e)[:60]

R["diagnosis"] = ("HEALTHY — refreshed + schedule ensured" if healthy else
                  "ERRORS — see invoke/cloudwatch; likely dep/code issue")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2775_confluence_diag.json", "w"), indent=1, default=str)
print("\nDIAGNOSIS:", R["diagnosis"])
print("OPS 2775 COMPLETE")
