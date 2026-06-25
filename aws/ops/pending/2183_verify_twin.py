import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-hot-money")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print("v",d.get("version"))
print("\nINFLOW LEADERS (USD return decomposed):")
for c in (d.get("inflow_leaders") or [])[:10]:
    print(f"  {c['country']:<13} score {c['hot_money_score']:+.2f} {c.get('conviction',''):<18} driver={c.get('return_driver','')}  usd_ret {c.get('usd_return_20d')}% = eq {c.get('local_equity_20d')}% + fx {c.get('fx_strength')}%")
tw=[c for c in d.get("all_countries",[]) if c.get("return_driver")=="TWIN_ENGINE"]
print(f"\nTWIN_ENGINE countries (currency + equity both lifting USD returns): {[c['country'] for c in tw]}")
print("DONE 2183")
