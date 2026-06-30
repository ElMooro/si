import boto3
ev=boto3.client("events",region_name="us-east-1")
for nm in ["null","None",""]:
    try:
        r=ev.describe_rule(Name=nm); print(f"STRAY RULE '{nm}':", r.get("ScheduleExpression"),"— deleting")
        for t in ev.list_targets_by_rule(Rule=nm).get("Targets",[]): ev.remove_targets(Rule=nm,Ids=[t["Id"]])
        ev.delete_rule(Name=nm); print(f"  deleted '{nm}'")
    except Exception: print(f"  no stray rule '{nm}' (clean)")
# confirm the two real rules still good
for rule in ["justhodl-search-attention-daily","justhodl-attention-confluence-daily"]:
    r=ev.describe_rule(Name=rule); print(f"  {rule}: {r.get('ScheduleExpression')} {r.get('State')}")
print("DONE 2591")
