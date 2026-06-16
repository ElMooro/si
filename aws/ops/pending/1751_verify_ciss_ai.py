import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=170,retries={"max_attempts":0}))
r=lam.invoke(FunctionName="justhodl-ciss-ai",InvocationType="RequestResponse")
print("invoke:", r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-ai.json")["Body"].read())
print("ok:", d.get("ok"), "| regime:", d.get("ea_regime"), "| model:", d.get("model"))
it=d.get("interpretation") or {}
for k in ["headline","regime_read","stress_source","risk_assets","liquidity"]:
    v=it.get(k); print(f"\n[{k}]\n{v}")
print("\n[watch]", it.get("watch"))
if not d.get("ok"): print("\nRAW (parse failed):", d.get("raw","")[:400])
