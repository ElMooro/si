import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
r=lam.invoke(FunctionName="ecb-auto-updater",InvocationType="RequestResponse")
print("ecb-auto-updater invoke:", json.loads(r["Payload"].read().decode()).get("body","")[:120])
d=json.loads(s3.get_object(Bucket="openbb-lambda-data",Key="ecb_data.json")["Body"].read())
print("\nCISS sub-indices after N-variant fix (date should now be ~2026-06):")
for k,v in d.items():
    if isinstance(v,dict) and "CISS" in k.upper() and "SOV" not in k.upper() and isinstance(v.get("value"),(int,float)):
        print(f"  {k:24} {round(v['value'],4)} @ {v.get('date')}")
