import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
for fn in ["justhodl-position-sizer","justhodl-tail-hedge","justhodl-hedge-planner"]:
    try: print(fn,"->",lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read().decode()[:120])
    except Exception as e: print(fn,"ERR",str(e)[:110])
time.sleep(2)
print("\n--- position-sizer: gamma in sizing multiplier? ---")
d=rd("data/position-sizing.json"); print("  regime:",json.dumps(d.get("regime",{})))
sp=(d.get("sized_positions") or [])[:1]
print("  top sized sample:",json.dumps(sp[0]) if sp else "none")

print("\n--- tail-hedge: real options cost read? ---")
d=rd("data/tail-hedge.json")
print("  stance:",d.get("stance"))
print("  vol_cost_context:",json.dumps(d.get("vol_cost_context") or {}))

print("\n--- hedge-planner: options-surface cost context? ---")
d=rd("data/hedge-planner.json")
print("  action:",d.get("action"),"| vol_timing:",json.dumps(d.get("vol_timing"))[:80])
print("  vol_cost_context:",json.dumps(d.get("vol_cost_context") or {}))
