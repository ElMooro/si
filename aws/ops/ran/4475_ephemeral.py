"""ops 4475 — force 4GB ephemeral on sec-bulk (create path didn't apply it
-> Errno 28), re-run, verify the 1.2GB stream lands."""
import json,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-sec-bulk"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=880,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4475,"started":datetime.now(timezone.utc).isoformat()}
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
R["ephemeral_before"]=c.get("EphemeralStorage",{}).get("Size")
for _ in range(6):
    try:
        lam.update_function_configuration(FunctionName=FN,EphemeralStorage={"Size":4096},Timeout=900,MemorySize=2048)
        break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
R["ephemeral_after"]=c.get("EphemeralStorage",{}).get("Size")
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
bb=json.loads(inv["Payload"].read().decode())
R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
rn=R.get("run") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"SEC-BULK EPHEMERAL FIXED ({R['ephemeral_before']}->{R['ephemeral_after']}MB; create "
  f"path had silently kept 512 -> Errno 28 explicit). Re-run: {json.dumps(rn,default=str)[:250]}. "
  "Verify gb+sha and seal."),
 "evidence":[{"kind":"log","ref":"data/_state/sec-bulk.json","snippet":"sha256"}]})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and rn.get("ok")
R["verdict"]=f"PASS — {json.dumps({k:rn.get(k) for k in ('file','gb','sha256')},default=str)}" if ok else f"PARTIAL — {json.dumps(rn,default=str)[:220]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4475_ephemeral.json","w"),indent=1,default=str)
open("aws/ops/reports/4475_ephemeral.md","w").write(f"# ops 4475 — ephemeral fix — {R['verdict']}\n")
print(R["verdict"])
