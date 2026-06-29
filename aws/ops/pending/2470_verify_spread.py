import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
print("=== #6 DEMAND-SUPPLY GROWTH SPREAD (per industry) ===")
for g,e in (d.get("industry_pressure") or {}).items():
    if e.get("demand_supply_spread_pp") is not None:
        print("  %-22s spread=%-6s state=%-8s forming=%s (ordersYoY %s, backlogYoY %s)"%(
            g,e.get("demand_supply_spread_pp"),e.get("spread_state"),e.get("bottleneck_forming"),
            e.get("new_orders_yoy_pct"),e.get("backlog_yoy_pct")))
forming=[g for g,e in (d.get("industry_pressure") or {}).items() if e.get("bottleneck_forming")]
print("INDUSTRIES WITH BOTTLENECK FORMING:",forming or "none (spreads not widening — consistent w/ DAMPING/easing)")
print("DONE 2470")
