import json, boto3
from botocore.config import Config
from datetime import datetime, timezone
ev=boto3.client("events",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
# 1) find + enable the short-interest schedule rule
rules=[]
p=ev.get_paginator("list_rules")
for pg in p.paginate():
    for r in pg["Rules"]:
        if "short-interest" in r["Name"].lower():
            rules.append((r["Name"], r.get("State")))
print("short-interest rules:", rules)
for name,state in rules:
    if state!="ENABLED":
        ev.enable_rule(Name=name); print("ENABLED", name)
# 2) invoke to refresh now
print("invoking justhodl-short-interest...")
r=lam.invoke(FunctionName="justhodl-short-interest",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:200])
# 3) verify file fresh + whole-market short float
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/short-interest.json")["Body"].read())
bt=d.get("by_ticker",{})
cov=sum(1 for v in bt.values() if v.get("latest_short_pct") is not None)
fv=sum(1 for v in bt.values() if v.get("short_src")=="finviz")
print(f"\ngenerated_at: {d.get('generated_at')}")
print(f"by_ticker={len(bt)}  latest_short_pct cov={cov}  finviz-sourced={fv}")
for tk in ["GME","MU","CVNA","BYND","SMCI"]:
    v=bt.get(tk,{}); print(f"  {tk:6} short={v.get('latest_short_pct')} dtc={v.get('days_to_cover')} src={v.get('short_src')}")
