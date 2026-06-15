import json, boto3
ev=boto3.client("events",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
rn="ticker-trends-2x-daily"
# 1) enable rule
ev.enable_rule(Name=rn)
r=ev.describe_rule(Name=rn); print(f"rule {rn}: State={r.get('State')} sched={r.get('ScheduleExpression')}")
# 2) dedupe targets (had 2 identical)
t=ev.list_targets_by_rule(Rule=rn).get("Targets",[])
print("targets before:", [(x['Id'], x['Arn'].split(':')[-1]) for x in t])
if len(t)>1:
    # keep the first, remove the rest (all point to same function)
    dup_ids=[x["Id"] for x in t[1:]]
    ev.remove_targets(Rule=rn, Ids=dup_ids)
    print("removed dup targets:", dup_ids)
t2=ev.list_targets_by_rule(Rule=rn).get("Targets",[])
print("targets after:", [(x['Id'], x['Arn'].split(':')[-1]) for x in t2])
# ensure lambda permission for the rule exists (idempotent add)
try:
    lam.add_permission(FunctionName="justhodl-ticker-trends", StatementId="evt-ticker-trends-2x",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=r["Arn"])
    print("added invoke permission")
except lam.exceptions.ResourceConflictException:
    print("invoke permission already present")
except Exception as e:
    print("perm:", str(e)[:100])
# 3) trigger a fresh run NOW (async — function runs up to 600s)
lam.invoke(FunctionName="justhodl-ticker-trends", InvocationType="Event")
print("async-invoked justhodl-ticker-trends to refresh feed now")
