"""ops 2642 — deal-scanner: daily 22:00 → every 3 hours (freshness + self-healing)."""
import boto3
ev=boto3.client("events",region_name="us-east-1")
r=ev.put_rule(Name="deal-scanner-daily", ScheduleExpression="cron(5 */3 * * ? *)", State="ENABLED",
              Description="Deal scanner — every 3 hours (fresh contract/order wins)")
print("rule updated:",r.get("RuleArn"))
d=ev.describe_rule(Name="deal-scanner-daily")
print("now:",d.get("ScheduleExpression"),"[",d.get("State"),"]")
print("DONE 2642")
