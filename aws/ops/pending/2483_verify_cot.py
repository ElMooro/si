import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
C=d.get("commercial_positioning") or {}
print("=== #4 COT COMMERCIAL HEDGERS ===")
for k,v in (C.get("commodities") or {}).items():
    print("  %-3s %-12s net=%-10s chg13wk=%-10s pct_oi=%-6s %-24s prod=%s"%(k,v.get("name"),v.get("commercial_net"),v.get("chg_13wk"),v.get("pct_oi"),v.get("lean"),",".join(v.get("producers",[])[:4])))
print("commercials_leaning_tight:",C.get("commercials_leaning_tight"))
print("DONE 2483")
