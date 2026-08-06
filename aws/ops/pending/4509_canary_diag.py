"""ops 4509 — canary diag (its Lambda died: zero fred-canary warm keys) +
surface occ/midas/seclending/ambs (reader 5637 wedged at GH)."""
import gzip,json,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-canary-macro"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
logs=boto3.client("logs",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":4509,"started":datetime.now(timezone.utc).isoformat()}
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["fn_err"]=inv.get("FunctionError")
body=inv["Payload"].read().decode()
R["body"]=body[:260]
time.sleep(6)
try:
    ev=logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",limit=80)
    msgs=[e["message"] for e in ev.get("events",[])]
    errs=[m.strip() for m in msgs if "Error" in m or "Traceback" in m or "Task timed out" in m]
    R["log_tail"]=(errs[-3:] if errs else [m.strip()[:120] for m in msgs[-3:]])
except Exception as e: R["log_tail"]=[f"log err {str(e)[:60]}"]
def gj(k):
    b=s3.get_object(Bucket=BUCKET,Key=k)["Body"].read()
    return json.loads(gzip.decompress(b) if k.endswith(".gz") else b)
try:
    g=gj("data/warm/global-expansion-summary.json")
    R["occ"]=g.get("occ"); R["sec_midas"]=g.get("sec_midas")
except Exception as e: R["exp_err"]=str(e)[:70]
try:
    ny=gj("data/warm/nyfed-markets/latest-summary.json").get("bounded",{})
    R["seclending"]=ny.get("seclending_latest"); R["ambs"]=ny.get("ambs_latest")
except Exception as e: R["ny_err"]=str(e)[:70]
try:
    r2=s3.list_objects_v2(Bucket=BUCKET,Prefix="data/warm/fred-canary/",MaxKeys=10)
    R["fred_canary_keys_now"]=[o["Key"] for o in r2.get("Contents",[])]
except Exception: pass
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("CANARY DIAG + CLIPPED SURFACING: fn_err="+str(R.get("fn_err"))
  +" body="+R.get("body","")[:150]+" log="+json.dumps(R.get("log_tail"),default=str)[:280]
  +" · fred-canary keys now="+str(len(R.get("fred_canary_keys_now") or []))
  +" · seclending="+json.dumps(R.get("seclending"),default=str)[:110]
  +" ambs="+json.dumps(R.get("ambs"),default=str)[:110]
  +" occ="+json.dumps(R.get("occ"),default=str)[:130]
  +" midas="+json.dumps(R.get("sec_midas"),default=str)[:130]+". Seal what passed; patch follows the log."),
 "evidence":[{"kind":"log","ref":"data/warm/canary-macro-summary.json","snippet":"panels"}]})
bus({"action":"fanout_pending"})
R["verdict"]=("canary "+("CRASH:"+str(R.get("fn_err")) if R.get("fn_err") else "ran")
 +" keys_now="+str(len(R.get("fred_canary_keys_now") or []))
 +" | sec="+str((R.get("seclending") or {}).get("ok"))+" ambs="+str((R.get("ambs") or {}).get("ok"))
 +" occ="+str((R.get("occ") or {}).get("ok"))+" midas="+str((R.get("sec_midas") or {}).get("ok")))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4509_diag.json","w"),indent=1,default=str)
open("aws/ops/reports/4509_diag.md","w").write("# ops 4509 — "+R["verdict"]+"\n- log: "+json.dumps(R.get("log_tail"),default=str)[:400]+"\n- occ: "+json.dumps(R.get("occ"),default=str)[:250]+"\n- midas: "+json.dumps(R.get("sec_midas"),default=str)[:250]+"\n- seclending: "+json.dumps(R.get("seclending"),default=str)[:200]+"\n- ambs: "+json.dumps(R.get("ambs"),default=str)[:200]+"\n")
print(R["verdict"])
