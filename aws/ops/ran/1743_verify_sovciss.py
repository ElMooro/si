import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
print("invoking ecb-auto-updater (sync)...")
r=lam.invoke(FunctionName="ecb-auto-updater",InvocationType="RequestResponse")
payload=r["Payload"].read().decode()[:300]
print("  response:", payload)
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="ecb_data.json")["Body"].read())
# SovCISS
sov=d.get("ECB.SOVCISS",{})
print(f"\nECB.SOVCISS: value={sov.get('value')} date={sov.get('date')} freq={sov.get('frequency')} obs={sov.get('observations')}")
print(f"  name: {sov.get('name')}")
# CISS components (symbols kept stable)
print("\nCISS components (symbol -> latest value/date):")
for k,v in d.items():
    if isinstance(v,dict) and "CISS" in k and "SOV" not in k:
        print(f"  {k:24} {v.get('value')} @ {v.get('date')}")
