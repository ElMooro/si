import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"))
print("BULLWHIP (trend-based):")
for g,e in (d.get("industry_pressure") or {}).items():
    if e.get("bullwhip_ratio") is not None:
        print("  %-22s ratio=%-6s chg=%-7s state=%s"%(g,e.get("bullwhip_ratio"),str(e.get("bullwhip_chg_pct"))+'%',e.get("bullwhip_state")))
print("CHOKEPOINT criticality on boom ranks (tech chokepoints):")
for r in (d.get("ranks") or [])[:12]:
    if r.get("chokepoint_criticality") is not None:
        print("  %-5s crit=%s boom=%s"%(r["ticker"],r.get("chokepoint_criticality"),r.get("boom_score")))
print("MARGIN INFLECTING early calls:",[c["ticker"] for c in (d.get("early_bottleneck_calls") or []) if c.get("margin_inflecting")])
print("DONE 2460")
