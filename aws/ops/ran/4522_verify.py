import io,json,os,time,zipfile
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
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
fe=inv.get("FunctionError"); body=inv["Payload"].read().decode()[:200]
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
top=[(p["slug"],p["n_keys"],p.get("hot_feeds")) for p in sorted(hub["providers"],key=lambda x:-x["n_keys"])[:10]]
fred=next((p for p in hub["providers"] if p["slug"]=="fred"),{})
f=json.loads(s3.get_object(Bucket=B,Key="data/providers/fred.json")["Body"].read())
via=sum(1 for k in f.get("keys",[]) if k.get("via")=="rollup")
out=(f"fn_err={fe} body={body[:120]} | totals={json.dumps(hub.get('totals'))} "
     f"fred_row={json.dumps({k:fred.get(k) for k in ('n_keys','hot_feeds')})} via_rollup={via} "
     f"top={json.dumps(top)[:280]}")
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4522_verify.md","w").write("# 4522 — "+out+"\n")
print(out)
