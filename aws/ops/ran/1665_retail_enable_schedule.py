import boto3
ev=boto3.client("events",region_name="us-east-1")
R="justhodl-retail-sentiment-30min"
before=ev.describe_rule(Name=R).get("State")
ev.enable_rule(Name=R)
after=ev.describe_rule(Name=R)
print(f"rule {R}: {before} -> {after.get('State')} | sched {after.get('ScheduleExpression')}")
