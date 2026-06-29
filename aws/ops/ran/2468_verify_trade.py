import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
T=d.get("trade_policy") or {}
print("=== #4 TRADE/POLICY ===")
for k,v in (T.get("phrases") or {}).items():
    print("  %-18s 120d=%-4s prior=%-4s %-8s e.g. %s"%(k,v.get("hits_120d"),v.get("hits_prior"),v.get("trend"),",".join(v.get("sample_tickers",[])[:5])))
print("rising:",T.get("rising_phrases"),"| intensity_chg_pct:",T.get("intensity_chg_pct"),"| building:",T.get("policy_bottleneck_building"))
print("exposed_names:",T.get("exposed_names"))
print("DONE 2468")
