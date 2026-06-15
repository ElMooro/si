import json, boto3, base64
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
# 1) rule state
for rn in ["ticker-trends-2x-daily"]:
    try:
        r=ev.describe_rule(Name=rn); print(f"rule {rn}: State={r.get('State')} sched={r.get('ScheduleExpression')}")
        t=ev.list_targets_by_rule(Rule=rn).get("Targets",[]); print("  targets:", [x.get("Arn","").split(":")[-1] for x in t])
    except Exception as e: print(f"rule {rn}: {str(e)[:100]}")
# 2) function exists + config
try:
    c=lam.get_function_configuration(FunctionName="justhodl-ticker-trends")
    print(f"lambda: exists, timeout={c.get('Timeout')}s mem={c.get('MemorySize')} last_mod={c.get('LastModified')}")
except Exception as e: print("lambda cfg:",str(e)[:120])
# 3) invoke and see if it succeeds
r=lam.invoke(FunctionName="justhodl-ticker-trends", InvocationType="RequestResponse", LogType="Tail")
print("invoke:", r.get("StatusCode"), "err:", r.get("FunctionError"))
log=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
print("---- log tail ----")
for ln in log.splitlines()[-22:]:
    print(" ",ln[:170])
# 4) re-check file freshness
try:
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/ticker-trends.json")
    age=(datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600
    print(f"ticker-trends.json age now: {age:.2f}h")
except Exception as e: print("file:",str(e)[:100])
