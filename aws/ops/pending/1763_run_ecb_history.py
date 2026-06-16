import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke ecb-history:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
time.sleep(3)
for sid in ["unemployment_ea","indprod_total","indprod_core","eurusd","ilm_usd_claims","ilm_eur_to_nonres"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/ecb-hist/{sid}.json")["Body"].read())
        print(f"  {sid:20} freq={d.get('freq'):8} n={d.get('n_points')} {d.get('first_date')}->{d.get('latest_date')} latest={d.get('latest')} pctile={d.get('percentile')}")
    except Exception as e: print(f"  {sid:20} MISSING ({type(e).__name__})")
# manifest count
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
print("manifest series:", len(m) if isinstance(m,list) else m.get("count"))
