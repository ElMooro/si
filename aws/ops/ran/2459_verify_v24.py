import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s")
print("\n=== 1. BULLWHIP per industry (Lee/Forrester) ===")
for g,e in (d.get("industry_pressure") or {}).items():
    if e.get("bullwhip_ratio") is not None:
        print("  %-22s ratio=%-5s prior=%-5s state=%s"%(g,e.get("bullwhip_ratio"),e.get("bullwhip_prior"),e.get("bullwhip_state")))
print("\n=== 2. GOLDRATT chokepoint wiring + 3. BUFFETT margin inflection (early calls) ===")
for c in (d.get("early_bottleneck_calls") or [])[:8]:
    print("  %-5s %-22s crit=%-5s in_scarcity=%-5s | margin now=%s trough=%s INFLECTING=%s | gap=%s"%(
        c["ticker"],c.get("industry"),c.get("chokepoint_criticality"),c.get("chokepoint_in_scarcity"),
        c.get("op_margin_now"),c.get("op_margin_trough"),c.get("margin_inflecting"),c.get("consensus_gap_score")))
print("DONE 2459")
