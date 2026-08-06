import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-provider-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
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
lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
top=[(p["slug"],p["n_keys"],p.get("hot_feeds")) for p in sorted(hub["providers"],key=lambda x:-x["n_keys"])[:12]]
fred=next((p for p in hub["providers"] if p["slug"]=="fred"),{})
zero=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
try:
    dbg=json.loads(s3.get_object(Bucket=B,Key="data/audit/provider-join-debug.json")["Body"].read())
except Exception as e: dbg={"err":str(e)[:60]}
out="DBG="+json.dumps(dbg,default=str)[:500]+" | "+f"totals={json.dumps(hub.get('totals'))} fred={json.dumps({k:fred.get(k) for k in ('n_keys','hot_feeds','total_mb')})} top={json.dumps(top)[:300]} zero={json.dumps(zero)[:200]}"
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4519_dbgx.md","w").write("# 4518 final join — "+out+"\n")
print(out)
