import json, boto3
from datetime import datetime, timezone, date
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-switzerland",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/switzerland.json")["Body"].read())
today=datetime.now(timezone.utc).date()
print("regime:", d["crisis_signal"]["regime"], d["crisis_signal"]["score_0_100"])
for s in d["series"]:
    ld=s.get("latest_date") or ""
    try:
        ld10=(ld+"-01")[:10] if len(ld)==7 else ld[:10]; old=(today-date.fromisoformat(ld10)).days
    except Exception: old=None
    print(f"  {s['id']:24} {s.get('source'):12} {ld:12} {str(old)+'d':>6} latest={s.get('latest')} ({s.get('start_date')}→)")
