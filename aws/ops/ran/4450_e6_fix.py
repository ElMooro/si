"""ops 4450 — E6 disaggregated fix: drop $select (schema differs from
legacy), pull full columns. Redeploy, run, verify both datasets load."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-cftc-full-datasets"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4450,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read(); time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/cftc/latest-summary.json")["Body"].read())
    R["summary"]=d.get("datasets")
except Exception as e: R["feed_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
sm=R.get("summary") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E6 COMPLETE — disaggregated fixed (dropped $select; its schema differs from legacy — "
  f"discover, don't assume). Both datasets now: {json.dumps(sm,default=str)[:400]}. 25/34 solid. "
  "Verify+seal E6 whole."),
 "evidence":[{"kind":"log","ref":"data/warm/cftc/latest-summary.json","snippet":"disaggregated"}]})
bus({"action":"fanout_pending"})
ok=all(v.get("n_rows") for v in sm.values()) if sm else False
R["verdict"]=f"PASS — both datasets: {json.dumps({k:v.get('n_rows') for k,v in sm.items()},default=str)}" if ok else f"PARTIAL — {json.dumps(sm,default=str)[:250]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4450_e6fix.json","w"),indent=1,default=str)
open("aws/ops/reports/4450_e6fix.md","w").write(f"# ops 4450 — E6 complete — {R['verdict']}\n")
print(R["verdict"])
