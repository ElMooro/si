import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
# invoke alpha-research async, poll for refresh
KEY="data/alpha-scoreboard-research.json"
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=KEY)["Body"].read()).get("generated_at")
except: before=None
lam.invoke(FunctionName="justhodl-alpha-research",InvocationType="Event")
print("invoked alpha-research (async); polling...")
d=None
for i in range(13):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=KEY)["Body"].read())
        if d.get("generated_at")!=before: break
    except: pass
bt=d.get("by_ticker",{}) if d else {}
# find financials arrays and report depth
depths=[]
for tk,r in bt.items():
    fin=r.get("financials") or r.get("fins") or []
    if isinstance(fin,list) and fin: depths.append((tk,len(fin)))
depths.sort(key=lambda x:-x[1])
print(f"refreshed={d.get('generated_at')!=before if d else '?'} | tickers={len(bt)}")
print("financials depth (top 6 by length):", depths[:6])
if depths:
    tk,n=depths[0]; yrs=[f.get("year") for f in bt[tk].get("financials",bt[tk].get("fins",[]))]
    print(f"  {tk}: {n} years -> {yrs[0]}..{yrs[-1]}")
