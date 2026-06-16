import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-earnings-surprise.json")["Body"].read())
print(f"surprise universe (>=1B): {d.get('n')}")
print("TOP BEATS:", [(x['ticker'],x.get('eps_surprise'),x.get('perf_m')) for x in d.get("top_beats",[])[:6]])
print("TOP MISSES:", [(x['ticker'],x.get('eps_surprise')) for x in d.get("top_misses",[])[:6]])
# valuations eps_surprise attach
v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
sp=v.get("sp_table",[])
print("valuations sp_table eps_surprise coverage:", sum(1 for r in sp if r.get("eps_surprise") is not None),"/",len(sp))
# add SLA
K="data/_freshness-manifest.json"
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
m.setdefault("key_overrides",{})["data/finviz-earnings-surprise.json"]=14
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,Body=json.dumps(m,indent=2).encode(),ContentType="application/json")
print("surprise SLA=14h added | total finviz overrides:", len([k for k in m["key_overrides"] if "finviz" in k]))
