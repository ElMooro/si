import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
C=d.get("concentration") or {}
print("=== #6 SINGLE-POINT-OF-FAILURE ===")
print("n_flagged:",C.get("n_flagged"),"| systemic_hubs:",C.get("systemic_hubs"))
for tk,v in list((C.get("flagged_names") or {}).items())[:10]:
    print("  %-5s hub=%-5s flags=%s"%(tk,v.get("systemic_hub"),[f.get("type")+":"+str(f.get("country") or f.get("pct")) for f in (v.get("flags") or [])][:3]))
print("PPI(ppi_pricing) tightening_inputs:",(d.get("ppi_pricing") or {}).get("tightening_inputs"),"building:",(d.get("ppi_pricing") or {}).get("pricing_power_building"))
print("DONE 2489")
