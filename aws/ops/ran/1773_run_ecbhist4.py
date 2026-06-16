import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
for sid in ["m1_growth","gdp_yoy","bank_rate_nfc","eurcny","eurjpy"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/ecb-hist/{sid}.json")["Body"].read())
        print(f"  {sid:14} freq={d.get('freq'):9} n={d.get('n_points'):5} {d.get('first_date')}->{d.get('latest_date')} latest={d.get('latest')} pctile={d.get('percentile')}")
    except Exception as e: print(f"  {sid:14} MISSING")
