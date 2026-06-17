import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-switzerland",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/switzerland.json")["Body"].read())
for s in d["series"]:
    print(f"  {s['id']:24} {s.get('start_date')} -> {s.get('latest_date')}  n={len(s.get('points',[]))}")
