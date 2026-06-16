import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
K="data/stock-valuations.json"
def gen():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read()).get("generated_at")
    except: return None
before=gen(); print("before generated_at:",before)
lam.invoke(FunctionName="justhodl-stock-valuations",InvocationType="Event")
print("async invoked; polling for refresh...")
d=None
for i in range(28):  # up to ~9min
    time.sleep(20)
    g=gen()
    if g and g!=before:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read()); print("refreshed after ~%ds"%((i+1)*20)); break
if not d:
    print("did not refresh in window; reading current feed anyway")
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
sp=d.get("sp_table",[]); hp=d.get("hp",[])
def cov(rows,f): return sum(1 for r in rows if r.get(f) is not None)
print(f"\nsp_table={len(sp)}  recom={cov(sp,'analyst_recom')} target={cov(sp,'target_price')} upside={cov(sp,'analyst_upside')}")
print(f"hp={len(hp)}  recom={cov(hp,'analyst_recom')} target={cov(hp,'target_price')} upside={cov(hp,'analyst_upside')}")
for r in sp[:5]:
    print(f"  {r.get('t'):6} recom={r.get('analyst_recom')} target={r.get('target_price')} upside={r.get('analyst_upside')}")
for ln in d.get("diagnostics",[]):
    if "finviz" in ln.lower(): print("DIAG:",ln)
