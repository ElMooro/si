"""ops 4553 — deploy the scoped FRED import, schedule every 5 min, kick
real rounds now, report genuine seen/excluded/imported counts across the
7 priority categories Khalid specified."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4553,"started":datetime.now(timezone.utc).isoformat()}
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
lam.update_function_configuration(FunctionName=FN,Timeout=280,MemorySize=1536)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
ev.put_targets(Rule="justhodl-fred-catalog-5min",
    Targets=[{"Id":"cats","Arn":arn,"Input":json.dumps({"phase":"categories"})},
             {"Id":"scoped","Arn":arn,"Input":json.dumps({"phase":"scoped_import"})}])
# kick three real rounds now (don't wait for cron)
rounds=[]
for i in range(3):
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"phase":"scoped_import"}).encode())
    body=json.loads(inv["Payload"].read().decode())
    rn=json.loads(body["body"]) if isinstance(body,dict) and "body" in body else body
    rounds.append({"fn_err":inv.get("FunctionError"),"result":rn})
    if isinstance(rn,dict) and rn.get("status")=="COMPLETE": break
R["rounds"]=rounds
final=rounds[-1]["result"] if rounds else {}
try:
    m=json.loads(s3.get_object(Bucket=B,Key="data/providers/fred-scoped/manifest.json")["Body"].read())
    R["manifest"]=m
except Exception as e: R["manifest_err"]=str(e)[:80]
try:
    st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    R["sample_excluded"]=st.get("excluded_ids",[])[:5]
    R["sample_imported"]=st.get("imported_ids",[])[:8]
except Exception: pass
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"FRED SCOPED IMPORT (Khalid's 7 exact categories: Interest Rates/Exchange Rates/Monetary Data/"
  f"Financial Indicators/Banking/Business Lending/FX Intervention, freshness<=90d): "
  f"manifest={json.dumps(R.get('manifest'))[:300]}. sample_excluded={json.dumps(R.get('sample_excluded'))[:200]} "
  f"sample_imported={json.dumps(R.get('sample_imported'))[:150]}. Real observations pulled for fresh series "
  "only, stored per-series + paged catalog. Scheduled every 5min, resumable. Verify a few series against "
  "fred.stlouisfed.org directly."),
 "evidence":[{"kind":"log","ref":"data/providers/fred-scoped/manifest.json","snippet":"series_imported"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"manifest={json.dumps(R.get('manifest'))} sample_imported={json.dumps(R.get('sample_imported'))[:150]}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4553.json","w"),indent=1,default=str)
open("aws/ops/reports/4553.md","w").write("# 4553 — "+R["verdict"]+"\n- rounds: "+json.dumps([r.get('result') for r in rounds],default=str)+"\n- excluded_sample: "+json.dumps(R.get('sample_excluded'),default=str)+"\n")
print(R["verdict"][:400])
