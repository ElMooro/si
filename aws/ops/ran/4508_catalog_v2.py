"""ops 4508 — hub zeros: redeploy catalog w/ candidate prefixes+discovery,
re-inventory, report which zeros closed + the discovered truth."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-provider-catalog"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
logs=boto3.client("logs",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":4508,"started":datetime.now(timezone.utc).isoformat()}
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
bb=json.loads(inv["Payload"].read().decode())
R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
time.sleep(6)
try:
    ev=logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",limit=40)
    disc=[e["message"].strip() for e in ev.get("events",[]) if "USGOV_SUBS" in e.get("message","") or "EC_KEYS" in e.get("message","")]
    R["discovery"]=disc[-2:]
except Exception as e: R["discovery"]=[f"log err {str(e)[:60]}"]
try:
    hub=json.loads(s3.get_object(Bucket=BUCKET,Key="data/provider-catalog.json")["Body"].read())
    zeros=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
    R["still_zero"]=zeros; R["totals"]=hub.get("totals")
except Exception as e: R["hub_err"]=str(e)[:80]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("HUB v2 (Khalid saw the zeros): candidate prefixes for DDP/ECB + discovery. "
  f"Discovered: {json.dumps(R.get('discovery'),default=str)[:260]} · new totals {json.dumps(R.get('totals'))} · "
  f"still_zero={R.get('still_zero')} (occ/midas/chicago are the known blocks; fred-canary fills as its warm lands). Verify hub+seal."),
 "evidence":[{"kind":"log","ref":"data/provider-catalog.json","snippet":"totals"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"totals={json.dumps(R.get('totals'))} still_zero={R.get('still_zero')} disc={json.dumps(R.get('discovery'),default=str)[:200]}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4508_catalog_v2.json","w"),indent=1,default=str)
open("aws/ops/reports/4508_catalog_v2.md","w").write(f"# ops 4508 — hub v2 — {R['verdict']}\n")
print(R["verdict"])
