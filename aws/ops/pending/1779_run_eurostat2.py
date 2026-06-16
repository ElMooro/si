import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-eurostat-history",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-confidence.json")["Body"].read())
print("written:", d["written"])
for s in d["confidence"]+d["production_yoy"]:
    print(f"  {s['id']:22} {str(s['latest']):>9} ({s['latest_date']}) {s['first_date']}-> n={s.get('n_points')} pctile={s['percentile']} | {s['label'][:42]}")
print("heal manifest:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="Event").get("StatusCode"))
