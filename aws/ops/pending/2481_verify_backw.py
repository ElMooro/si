import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
B=d.get("backwardation") or {}
print("=== #3 BACKWARDATION (roll-yield) ===")
for k,v in (B.get("commodities") or {}).items():
    print("  %-10s etf3mo=%-7s spot3mo=%-7s roll=%-6s %-14s producers=%s"%(k,v.get("etf_3mo_pct"),v.get("spot_3mo_pct"),v.get("roll_yield_pct"),v.get("curve_state"),",".join(v.get("producers",[])[:5])))
print("backwardated:",B.get("backwardated"),"| physical_tightness:",B.get("physical_tightness"))
print("DONE 2481")
