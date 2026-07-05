import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2845,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["inv_note"]=str(e)[:100]
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
# find the signals list wherever it is
def find_sigs(o):
    for k in ("signals","canaries","grid","by_signal","results"):
        if isinstance(o.get(k),list): return o[k]
    for v in o.values():
        if isinstance(v,list) and v and isinstance(v[0],dict) and "key" in v[0]: return v
    return []
sigs=find_sigs(d)
want={"semiconductor_ip":None,"chile_exports":None,"korea_exports":None,"copper":None}
for s in sigs:
    if s.get("key") in want:
        want[s["key"]]={"available":s.get("available"),"value":s.get("value"),"band":s.get("band") or s.get("status"),"as_of":s.get("as_of"),"lead":s.get("lead_months")}
R["new_canaries"]=want
R["top_keys"]=[k for k in d.keys()][:15]
R["grid_score"]=d.get("grid_score") or d.get("composite") or d.get("score") or d.get("headline")
R["status"]="LIVE" if (want["semiconductor_ip"] and want["semiconductor_ip"].get("available")) and (want["chile_exports"] and want["chile_exports"].get("available")) else "CHECK"
print(json.dumps(R,indent=1,default=str)[:2500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2845_verify_canary.json","w"),indent=1,default=str)
print("OPS 2845 COMPLETE")
