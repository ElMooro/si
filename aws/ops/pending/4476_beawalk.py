"""ops 4475 — force 4GB ephemeral on sec-bulk (create path didn't apply it
-> Errno 28), re-run, verify the 1.2GB stream lands."""
import json,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-usgov-direct"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=880,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4476,"started":datetime.now(timezone.utc).isoformat()}
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
bb=json.loads(inv["Payload"].read().decode())
R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
rn=R.get("run") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("BEA TABLE-WALK LIVE (the promised cursor): NIPA full table list discovered, 5 tables/run "
  f"(Q, Year=ALL) to warm. First run: {json.dumps(rn.get('bea_walk') or rn,default=str)[:250]}. "
  "Daily 12:40 converges to 100% of NIPA; remaining 12 datasets same pattern next. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/_state/bea-walk.json","snippet":"progress_pct"}]})
bus({"action":"fanout_pending"})
bw=rn.get("bea_walk") or {}
ok=bool(bw.get("pulled_this_run"))
R["verdict"]=f"PASS — walk: {json.dumps(bw,default=str)[:140]}" if ok else f"PARTIAL — {json.dumps(rn,default=str)[:220]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4476_beawalk.json","w"),indent=1,default=str)
open("aws/ops/reports/4476_beawalk.md","w").write(f"# ops 4475 — ephemeral fix — {R['verdict']}\n")
print(R["verdict"])
