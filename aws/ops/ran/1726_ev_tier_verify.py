import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-stock-valuations",InvocationType="Event")
for i in range(15):
    time.sleep(20)
    v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/stock-valuations.json")["Body"].read())
    if v.get("generated_at")!=before: break
sp=v.get("sp_table",[]); hp=v.get("hp_out",[]) or []
both=sp+hp
tier=sum(1 for r in both if r.get("ev_tier"))
print(f"ev_tier coverage: {tier}/{len(both)}")
from collections import Counter
print("tier dist:", dict(Counter(r.get("ev_tier") for r in both if r.get("ev_tier"))))
for t in ["AAPL","XOM","KO","NVDA"]:
    r=next((x for x in both if x.get("t")==t),None)
    if r: print(f"  {t}: ev_ebitda={r.get('ev_ebitda')} sector_med={r.get('sector_ev_ebitda')} tier={r.get('ev_tier')}")
print("DIAG EV:", [d for d in v.get("diagnostics",v.get("DIAG",[])) if "EV" in str(d) or "EBITDA" in str(d)])
