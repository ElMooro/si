import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=170,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-switzerland",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/switzerland.json")["Body"].read())
print("n_series:", d["n_series"], "| crisis:", d["crisis_signal"]["regime"], d["crisis_signal"]["score_0_100"])
for s in d["series"]:
    print(f"  {s['id']:24} {s.get('latest'):>10} ({s.get('latest_date')}) pctile={s.get('pctile')} 3m={s.get('chg_3m')} | {s['label'][:40]}")
print("drivers:", d["crisis_signal"]["drivers"])
