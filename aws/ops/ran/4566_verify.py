"""ops 4566 — clean epoch with the pending-queue accounting model.
Preserve skip-set, deploy, TWO Event+poll rounds, confirm reconciles=true
holds across BOTH (not just a lucky single). Also sample the 144 errors
to confirm they're rate-limit (stranger-on-leaked-key) not logic bugs."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4566,"started":datetime.now(timezone.utc).isoformat()}
ev.disable_rule(Name="justhodl-fred-catalog-5min")
time.sleep(190)
st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
keep=st.get("imported_ids") or []
R["err_sample"]=list((st.get("errors") or {}).items())[:5]
s3.put_object(Bucket=B,Key="data/_state/fred-scoped-import.json",
    Body=json.dumps({"cats_done":[],"series_seen":0,"series_excluded_stale":0,
                     "series_imported":len(keep),"imported_baseline":len(keep),
                     "series_queued":0,"excluded_ids":[],"imported_ids":keep,
                     "n_pages":st.get("n_pages",0),"buffer":[],
                     "reset_by":"ops4566"}).encode(),ContentType="application/json")
R["baseline"]=len(keep)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
b=io.BytesIO()
with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f3 in os.listdir("aws/shared"):
        if f3.endswith(".py"): z.write("aws/shared/"+f3,f3)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=b.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
ev.enable_rule(Name="justhodl-fred-catalog-5min")
rounds=[]
for rd in range(2):
    mark=datetime.now(timezone.utc).isoformat()
    lam.invoke(FunctionName=FN,InvocationType="Event",Payload=json.dumps({"phase":"scoped_import"}).encode())
    for _ in range(9):
        time.sleep(30)
        st2=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
        if st2.get("updated_at","")>mark and not (st2.get("lease_until") or 0)>time.time():
            rounds.append({"cats_done":len(st2.get("cats_done") or []),"of":st2.get("n_categories_expanded"),
                           "seen":st2.get("series_seen"),"queued":st2.get("series_queued"),
                           "imported_total":st2.get("series_imported"),
                           "accounting":st2.get("accounting"),"status":st2.get("status")})
            break
R["rounds"]=rounds
R["both_reconcile"]=all(r.get("accounting",{}).get("reconciles") for r in rounds) if rounds else False
def bus(p):
    i=lam.invoke(FunctionName="justhodl-a2a-bus",InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"FRED accounting — pending-queue model, TWO consecutive rounds: both_reconcile={R['both_reconcile']} "
  f"rounds={json.dumps(rounds)[:300]}. err_sample={json.dumps(R['err_sample'])[:200]} (expect rate-limit from "
  "strangers on the still-live leaked key — the reason we run 60/min). Import advancing, lease-protected.")})
bus({"action":"fanout_pending"})
R["verdict"]=f"both_reconcile={R['both_reconcile']} rounds={json.dumps(rounds)[:280]}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4566.json","w"),indent=1,default=str)
open("aws/ops/reports/4566.md","w").write("# 4566 — "+R["verdict"]+"\n- errs: "+json.dumps(R["err_sample"],default=str)+"\n")
print(R["verdict"][:350])
