import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"))
crit=[(r["ticker"],r.get("chokepoint_criticality"),r.get("boom_score")) for r in (d.get("ranks") or []) if r.get("chokepoint_criticality") is not None]
print("boom ranks WITH chokepoint criticality (%d):"%len(crit))
for t,c,b in crit[:12]: print("  %-5s crit=%s boom=%s"%(t,c,b))
ck_scarcity=[c["ticker"] for c in (d.get("early_bottleneck_calls") or []) if c.get("chokepoint_in_scarcity")]
print("chokepoint-in-scarcity early calls:",ck_scarcity or "none (commodities aren't chokepoints — correct)")
print("DONE 2461")
