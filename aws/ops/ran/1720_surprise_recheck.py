import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-earnings-surprise.json")["Body"].read())
print(f"surprise (sane, >=1B, |eps|<=100): {d.get('n')}")
print("TOP BEATS:", [(x['ticker'],x.get('eps_surprise'),x.get('perf_m')) for x in d.get("top_beats",[])[:6]])
print("TOP MISSES:", [(x['ticker'],x.get('eps_surprise')) for x in d.get("top_misses",[])[:6]])
# re-run valuations to confirm eps_surprise attaches
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-stock-valuations",InvocationType="Event")
for i in range(15):
    time.sleep(20)
    v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
    if v.get("generated_at")!=before: break
sp=v.get("sp_table",[])
print("valuations eps_surprise coverage:", sum(1 for r in sp if r.get("eps_surprise") is not None),"/",len(sp))
