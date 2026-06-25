import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=790,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(40):
    c=lam.get_function(FunctionName="justhodl-hot-money")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print(f"countries={d.get('n_countries')} dur={d.get('duration_s')}s")
print("\nINFLOW LEADERS (now with REAL flow):")
for c in d.get("inflow_leaders",[])[:10]:
    f=c.get("net_flow_5d_usd"); fs=f"${f/1e6:+.0f}M" if f else "n/a"
    print(f"  #{c['rank']:<2} {c['country']:<14} score {c['hot_money_score']:+.2f}  flow5d {fs}  rel_mom {c.get('rel_mom_20d')}%")
print("\nOUTFLOW (real redemptions):")
for c in d.get("outflow_leaders",[])[:6]:
    f=c.get("net_flow_5d_usd"); fs=f"${f/1e6:+.0f}M" if f else "n/a"
    print(f"  {c['country']:<14} score {c['hot_money_score']:+.2f}  flow5d {fs}")
nz=sum(1 for c in d.get("all_countries",[]) if c.get("net_flow_5d_usd"))
print(f"\ncountries with real flow data: {nz}/{d.get('n_countries')}")
print("DONE 2176")
