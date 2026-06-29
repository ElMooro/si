import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
P=d.get("ppi_pricing") or {}
print("=== #5 PPI PRICING POWER ===")
for o in P.get("ppi_inputs",[]):
    print("  %-40s flag=%-16s ind=%-16s names=%s"%(str(o.get("input"))[:40],o.get("flag"),o.get("industry"),o.get("capturing_names")))
print("tightening_inputs:",P.get("tightening_inputs"),"| pricing_power_building:",P.get("pricing_power_building"))
# also confirm #3 backwardation + #4 cot blocks are clean
print("--- #3 backwardation:",json.dumps(d.get("backwardation"))[:300])
print("--- #4 commercial_positioning:",json.dumps(d.get("commercial_positioning"))[:300])
print("DONE 2481")
