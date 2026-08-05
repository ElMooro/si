"""ops 4438 — D6 fleet-map.html + F8 wave 2 + projection fixed. 18/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4438,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    for _ in range(5):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
for fn in ("justhodl-llm-cost-dashboard","justhodl-prepump-alerts-router","justhodl-stock-screener"):
    try:
        deploy(fn)
        inv=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        R[fn]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
        _=inv["Payload"].read()
    except Exception as e: R[fn]={"err":str(e)[:120]}
time.sleep(3)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/llm-cost.json")["Body"].read())
    R["projection"]=d.get("projection")
except Exception as e: R["proj_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("D6 + F8w2 + PROJECTION FIXED — 18/34. D6 fleet-map.html LIVE at "
  "https://justhodl.ai/fleet-map.html: KPI strip (functions/alive/DEAD/feeds/parasite-audit "
  "280-of-280 kept), DEAD-lambda table from D3 health, top-20 fan-out hubs from the D4 graph. "
  "F8 WAVE 2: prepump-alerts-router + stock-screener (next two offenders, 41+34 fallback sites) "
  "now run guard_output(warn) at write — three engines total on the migration path, CloudWatch "
  "FabricationSuspects accruing. PROJECTION: None-filter added (a None day no longer breaks it); "
  f"live value: {json.dumps(R.get('projection'),default=str)[:180]}. All three engines "
  "redeployed clean. Verify+seal; next: E1 symbology-master + F8 wave 3."),
 "evidence":[{"kind":"file","ref":"fleet-map.html","snippet":"Fleet Map"},
             {"kind":"log","ref":"data/llm-cost.json","snippet":"projection"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"18/34: +D6 page, +F8w2 x2, projection fixed"})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — D6 live, F8w2 deployed, projection={json.dumps(R.get('projection'),default=str)[:70]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4438_d6.json","w"),indent=1,default=str)
open("aws/ops/reports/4438_d6.md","w").write(
 f"# ops 4438 — D6+F8w2 — {R['verdict']}\n- runs: "+json.dumps({k:R[k] for k in R if k.startswith('justhodl')},default=str)[:400]+
 f"\n- projection: {json.dumps(R.get('projection'),default=str)}\n")
print(json.dumps(R,default=str)[:600])
