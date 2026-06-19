import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
for fn in ["justhodl-hedge-pnl","justhodl-portfolio-analytics"]:
    try: print(fn,"->",lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read().decode()[:110])
    except Exception as e: print(fn,"ERR",str(e)[:110])
time.sleep(2)
print("\n--- hedge-pnl ---")
d=rd("data/hedge-pnl.json"); print("  verdict:",d.get("verdict"))
print("  massive_risk_context:",json.dumps(d.get("massive_risk_context") or {}))
print("\n--- portfolio-analytics ---")
d=rd("data/portfolio-analytics.json"); print("  n_candidates:",d.get("n_candidates"))
print("  risk_environment:",json.dumps(d.get("risk_environment") or {}))
