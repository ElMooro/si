import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=170,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
cur=lam.get_function_configuration(FunctionName="justhodl-ciss-ai").get("Environment",{}).get("Variables",{})
print("ciss-ai env keys BEFORE:", sorted(cur.keys()))
if "ANTHROPIC_API_KEY" not in cur:
    src=lam.get_function_configuration(FunctionName="justhodl-research-critique").get("Environment",{}).get("Variables",{})
    if src.get("ANTHROPIC_API_KEY"):
        merged=dict(cur); merged["ANTHROPIC_API_KEY"]=src["ANTHROPIC_API_KEY"]
        lam.update_function_configuration(FunctionName="justhodl-ciss-ai",Environment={"Variables":merged})
        import time; time.sleep(6)
        print("ANTHROPIC_API_KEY copied from research-critique (value not shown)")
    else: print("source lacks key!")
else: print("key already present")
# re-invoke
print("invoke:", lam.invoke(FunctionName="justhodl-ciss-ai",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-ai.json")["Body"].read())
print("ok:", d.get("ok"))
it=d.get("interpretation") or {}
for k in ["headline","regime","stress_source","sovereign","risk_assets","liquidity"]:
    print(f"\n[{k}] {it.get(k)}")
print("\n[watch]", it.get("watch"))
if not d.get("ok"): print("RAW:", d.get("raw","")[:300])
