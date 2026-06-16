import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=170,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ciss-ai",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-ai.json")["Body"].read())
print("ok:", d.get("ok"))
it=d.get("interpretation") or {}
for k in ["headline","regime","stress_source","risk_assets","liquidity"]:
    print(f"\n[{k}] {it.get(k)}")
print("\n[watch]", it.get("watch"))
if not d.get("ok"): print("RAW:", d.get("raw","")[:300])
