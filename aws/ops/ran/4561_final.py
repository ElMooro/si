"""ops 4561 — set imported_baseline=1115 in state, deploy, one Event+poll
round, expect accounting.reconciles=true."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4561,"started":datetime.now(timezone.utc).isoformat()}
st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
st["imported_baseline"]=1115
s3.put_object(Bucket=B,Key="data/_state/fred-scoped-import.json",Body=json.dumps(st,default=str).encode(),ContentType="application/json")
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
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
lam.invoke(FunctionName=FN,InvocationType="Event",Payload=json.dumps({"phase":"scoped_import"}).encode())
final=None
for _ in range(9):
    time.sleep(30)
    st2=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    if st2.get("updated_at","")>R["started"]:
        final={"cats_done":len(st2.get("cats_done") or []),"of":st2.get("n_categories_expanded"),
               "seen":st2.get("series_seen"),"imported_total":st2.get("series_imported"),
               "stale":st2.get("series_excluded_stale"),"disc":st2.get("series_excluded_discontinued"),
               "accounting":st2.get("accounting"),"status":st2.get("status")}
        break
R["round"]=final
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4561.json","w"),indent=1,default=str)
open("aws/ops/reports/4561.md","w").write("# 4561 — "+json.dumps(final,default=str)+"\n")
print(json.dumps(final,default=str)[:350])
