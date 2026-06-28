import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-crypto-miners",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-miners.json")["Body"].read())
hr=d.get("hash_ribbons") or {}; es=(d.get("event_study") or {}).get("hash_ribbon") or {}
print("RIBBON:",hr.get("state"),"| in_cap days:",hr.get("days_in_capitulation"))
print("  true buys 3y:",hr.get("n_true_buys_3y"),"| naive crossups 3y:",hr.get("n_naive_crossups_3y"))
print("  last true buy:",hr.get("last_true_buy"))
print("event study verdict:",es.get("verdict"),"standing:",es.get("standing"))
for h in ("fwd30d","fwd90d","fwd180d"):
    v=es.get(h) or {}
    print(f"   {h}: buy mean {v.get('buy_mean')}% vs base {v.get('baseline_mean')}% | edge {v.get('edge_pp')}pp (n {v.get('n_buys')})")
print("DONE 2388")
