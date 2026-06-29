import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
C=d.get("capacity_response") or {}
print("=== #5 CAPACITY RESPONSE ===")
for k,v in (C.get("phrases") or {}).items():
    print("  %-18s 120d=%-4s prior=%-4s %-8s e.g. %s"%(k,v.get("hits_120d"),v.get("hits_prior"),v.get("trend"),",".join(v.get("sample_tickers",[])[:5])))
print("intensity_chg_pct:",C.get("intensity_chg_pct"),"| supply_relief_coming:",C.get("supply_relief_coming"))
print("read:",C.get("read"))
print("expanding_names:",C.get("expanding_names"))
print("DONE 2469")
