import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
r=lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")
print("invoke:", r["Payload"].read().decode()[:160])
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
ids={x["id"]:x for x in (m if isinstance(m,list) else m.get("series",[]))}
for sid in ["unemployment_ea","indprod_total","indprod_core","eurusd","ilm_usd_claims","ilm_eur_to_nonres","ciss_fx","ciss_mm"]:
    x=ids.get(sid)
    if x: print(f"  {sid:20} {x.get('freq'):8} latest={x.get('latest_date')} val={x.get('latest')} pctile={x.get('percentile')} n={x.get('n_points')} disc={x.get('discontinued')}")
    else: print(f"  {sid:20} MISSING from manifest")
print("manifest total series:", len(ids))
