import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-stock-valuations",InvocationType="Event")
v=None
for i in range(15):
    time.sleep(20)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
    if d.get("generated_at")!=before: v=d; break
v=v or d
sp=v.get("sp_table",[]); hp=v.get("hp",[])
print(f"sp_table sector_valuation: {sum(1 for r in sp if r.get('sector_valuation'))}/{len(sp)}")
print(f"hp sector_valuation: {sum(1 for r in hp if r.get('sector_valuation'))}/{len(hp)}")
from collections import Counter
c=Counter(r.get('sector_valuation') for r in sp)
print("tilt distribution:", dict(c))
