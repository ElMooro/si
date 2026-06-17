import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
print("invoke plumbing-aggregator:", lam.invoke(FunctionName="justhodl-plumbing-aggregator",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/plumbing-stress.json")["Body"].read())
ri=d.get("raw_indicators",{})
for k in ("OFR_FAILS_DELIVER","OFR_FAILS_RECEIVE"):
    v=ri.get(k,{})
    print(f"  {k}: value={v.get('value')} z={v.get('z_score')} pct={v.get('percentile')} n={v.get('n_obs')} stress={v.get('stress_score_0_100')} date={v.get('date')} err={v.get('err')}")
print("  L1 score:", (d.get("layers",{}).get("L1",{}) or {}).get("score"), "| composite:", d.get("composite_score"), d.get("composite_label"))
