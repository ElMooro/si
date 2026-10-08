"""ops 4502 — batch: redeploy 3 patched engines, targeted runs, verdicts.
(a) nyfed-full: seclending/ambs candidate-walk -> bounded 4/4?
(b) expansion only=occ + only=sec_midas: scrape-then-follow.
(c) canary: BLS startyear 2006 -> hot_series depth check."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=880,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4502,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    for _ in range(24):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(24):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
def run(fn,payload=b"{}"):
    inv=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=payload)
    b=json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
deploy("justhodl-nyfed-markets-full")
rn=run("justhodl-nyfed-markets-full")
R["nyfed_bounded"]=(rn.get("bounded") if isinstance(rn,dict) else rn)
deploy("justhodl-global-expansion")
R["occ"]=(run("justhodl-global-expansion",json.dumps({"only":"occ"}).encode()) or {}).get("live")
occ_s=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/global-expansion-summary.json")["Body"].read())
R["occ_detail"]=occ_s.get("occ")
R["midas_run"]=run("justhodl-global-expansion",json.dumps({"only":"sec_midas"}).encode())
mid_s=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/global-expansion-summary.json")["Body"].read())
R["midas_detail"]=mid_s.get("sec_midas")
deploy("justhodl-canary-macro")
cn=run("justhodl-canary-macro")
R["canary"]={"hot_series":cn.get("hot_series"),"bls":(cn.get("panels") or {}).get("bls_labor")}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("RESIDUALS BATCH (Khalid directive): nyfed seclending/ambs candidate-walk -> "
  f"{json.dumps({k:v for k,v in (R.get('nyfed_bounded') or {}).items() if k in ('seclending_latest','ambs_latest')},default=str)[:220]} · "
  f"OCC scrape-follow -> {json.dumps(R.get('occ_detail'),default=str)[:160]} · "
  f"MIDAS scrape-follow -> {json.dumps(R.get('midas_detail'),default=str)[:160]} · "
  f"BLS widened to 2006 -> {json.dumps(R.get('canary'),default=str)[:120]}. Verify+seal each."),
 "evidence":[{"kind":"log","ref":"data/warm/global-expansion-summary.json","snippet":"occ"}]})
bus({"action":"fanout_pending"})
sec_ok=(R.get("nyfed_bounded") or {}).get("seclending_latest",{}).get("ok")
amb_ok=(R.get("nyfed_bounded") or {}).get("ambs_latest",{}).get("ok")
R["verdict"]=(f"nyfed sec={sec_ok} ambs={amb_ok} · occ={'OK' if (R.get('occ_detail') or {}).get('ok') else 'MISS'} · "
              f"midas={'OK' if (R.get('midas_detail') or {}).get('ok') else 'MISS'} · canary_hot={R['canary'].get('hot_series')}")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4502_batch.json","w"),indent=1,default=str)
open("aws/ops/reports/4502_batch.md","w").write(f"# ops 4502 — residuals batch — {R['verdict']}\n- occ: {json.dumps(R.get('occ_detail'),default=str)[:220]}\n- midas: {json.dumps(R.get('midas_detail'),default=str)[:220]}\n- nyfed: {json.dumps(R.get('nyfed_bounded'),default=str)[:300]}\n")
print(R["verdict"])
