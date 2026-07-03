"""ops 2786 — freshness audit of every feed the Options Desk reads + diagnose earnings-iv-crush staleness."""
import os, json
from datetime import datetime, timezone
import boto3
s3 = boto3.client("s3", region_name="us-east-1"); B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
now = datetime.now(timezone.utc)
FEEDS = ["dealer-gex.json","dealer-gex-history.json","options-gamma.json","options-analytics.json",
         "polygon-options-flow.json","options-flow.json","opex-calendar.json","dix.json",
         "catalyst-skew-premove.json","earnings-iv-crush.json","volatility-squeeze.json","options-confluence.json"]
R = {"ops": 2786, "ts": now.isoformat(), "feeds": {}, "engine": {}}
print("── FEED FRESHNESS (Options Desk) ──")
for f in FEEDS:
    try:
        h = s3.head_object(Bucket=B, Key="data/"+f)
        age = (now - h["LastModified"].astimezone(timezone.utc)).total_seconds()/3600
        flag = "STALE" if age > 48 else ("aging" if age > 30 else "fresh")
        R["feeds"][f] = {"age_h": round(age,1), "flag": flag, "bytes": h["ContentLength"]}
        print("  %-28s %6.1fh  %s" % (f, age, flag))
    except Exception as e:
        R["feeds"][f] = {"err": str(e)[:60]}; print("  %-28s MISSING/ERR %s" % (f, str(e)[:40]))
# diagnose earnings-iv-crush engine
print("\n── earnings-iv-crush ENGINE DIAGNOSIS ──")
fn = "justhodl-earnings-iv-crush"
try:
    cfg = lam.get_function(FunctionName=fn)["Configuration"]
    arn = cfg["FunctionArn"]
    R["engine"]["exists"] = True; R["engine"]["last_modified"] = cfg["LastModified"]; R["engine"]["timeout"] = cfg["Timeout"]
    print("  lambda EXISTS · last_code_update %s · timeout %ss" % (cfg["LastModified"][:16], cfg["Timeout"]))
    # EventBridge rules targeting it
    rules = ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    R["engine"]["rules"] = []
    if not rules:
        print("  ⚠️ NO EventBridge rule targets this lambda — NOT SCHEDULED (that's why it's stale)")
    for rn in rules:
        d = ev.describe_rule(Name=rn)
        R["engine"]["rules"].append({"name": rn, "state": d.get("State"), "sched": d.get("ScheduleExpression")})
        print("  rule %s · %s · %s" % (rn, d.get("State"), d.get("ScheduleExpression")))
    # last actual invocation from CloudWatch logs
    lg = "/aws/lambda/" + fn
    try:
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1).get("logStreams", [])
        if streams and streams[0].get("lastEventTimestamp"):
            last = datetime.fromtimestamp(streams[0]["lastEventTimestamp"]/1000, tz=timezone.utc)
            age = (now - last).total_seconds()/3600
            R["engine"]["last_invocation_h"] = round(age,1)
            print("  last INVOCATION: %.1fh ago (%s)" % (age, last.isoformat()[:16]))
    except Exception as e:
        print("  logs check err:", str(e)[:60])
except lam.exceptions.ResourceNotFoundException:
    R["engine"]["exists"] = False; print("  ⚠️ lambda %s DOES NOT EXIST" % fn)
except Exception as e:
    R["engine"]["err"] = str(e)[:80]; print("  err:", str(e)[:80])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2786_options_feed_health.json","w"), indent=1, default=str)
print("\nOPS 2786 COMPLETE")
