import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=620,retries={"max_attempts":0})); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-regime-conditional-trust")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
try: print("invoke:",lam.invoke(FunctionName="justhodl-regime-conditional-trust",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
except Exception as e: print("invoke note:",str(e)[:80])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/regime-conditional-trust.json")["Body"].read())
print("\ncurrent_regime:",d.get("current_regime"),"| engines:",d.get("n_engines"))
print("regime distribution (weekly reconstructed):",d.get("regime_distribution"))
print("\nBEST suited to current regime (",d.get("current_regime"),"):")
for b in (d.get("best_suited_to_current_regime") or [])[:8]:
    print(f"   {b['engine']:<34} excess {b.get('regime_mean_excess_pct')}% n{b.get('n')} factor {b.get('factor')}")
print("WORST suited:")
for w in (d.get("worst_suited_to_current_regime") or [])[:8]:
    print(f"   {w['engine']:<34} excess {w.get('regime_mean_excess_pct')}% n{w.get('n')} factor {w.get('factor')}")
# sample one engine's full by_regime to confirm bucketing
samp=[e for e,v in d.get("engines",{}).items() if len(v.get("by_regime",{}))>=2][:3]
print("\nsample engines with multi-regime track records:")
for e in samp:
    print(f"   {e}: "+", ".join(f"{r}:n{v['n']}/lb{v['wilson_lb']}/exc{v['mean_excess_pct']}" for r,v in d["engines"][e]["by_regime"].items()))
print("DONE 2065")
