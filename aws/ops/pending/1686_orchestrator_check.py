import boto3
from datetime import datetime, timezone, timedelta
lam=boto3.client("lambda",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1"); cw=boto3.client("cloudwatch",region_name="us-east-1")
orch=["justhodl-health-monitor","justhodl-sector-tilt","justhodl-desk-allocator","justhodl-prepump-alerts-router","justhodl-page-ai-commentary","justhodl-smart-money-cluster"]
now=datetime.now(timezone.utc); start=now-timedelta(days=14)
def daily(name,metric):
    r=cw.get_metric_statistics(Namespace="AWS/Lambda",MetricName=metric,Dimensions=[{"Name":"FunctionName","Value":name}],StartTime=start,EndTime=now,Period=86400,Statistics=["Sum"])
    return [(p["Timestamp"].strftime("%m-%d"),int(p["Sum"])) for p in sorted(r.get("Datapoints",[]),key=lambda x:x["Timestamp"])]
# also enumerate ALL disabled rules in the account (the batch-disable smoking gun)
print("### ALL DISABLED EventBridge rules in account ###")
dis=[]
p=ev.get_paginator("list_rules")
for pg in p.paginate():
    for r in pg.get("Rules",[]):
        if r.get("State")=="DISABLED": dis.append(r["Name"])
print(f"{len(dis)} disabled rules:")
for n in sorted(dis): print("   -",n)
print()
for fn in orch:
    print("="*64); print(fn)
    try:
        arn=lam.get_function_configuration(FunctionName=fn)["FunctionArn"]
    except Exception as e:
        print("  MISSING:",str(e)[:70]); continue
    try:
        rules=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
        for rn in rules:
            r=ev.describe_rule(Name=rn); print(f"  rule {rn}: State={r.get('State')} sched={r.get('ScheduleExpression')}")
        if not rules: print("  rules: NONE (orchestrated by another fn)")
    except Exception as e: print("  rules err:",str(e)[:70])
    print("  invocations(14d):", daily(fn,"Invocations"))
