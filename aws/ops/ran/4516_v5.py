"""ops 4513 — v4 hub (map x graph join) + async canary verify."""
import gzip,io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-provider-catalog"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4516,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    for _ in range(20):
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
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
deploy("justhodl-canary-macro")
lam.invoke(FunctionName="justhodl-canary-macro",InvocationType="Event",Payload=b"{}")
deploy(FN)
time.sleep(170)
try:
    cs=json.loads(s3.get_object(Bucket=B,Key="data/warm/canary-macro-summary.json")["Body"].read())
    R["panels"]={k:(v.get("ok") or str(v.get("reason"))[:34]) for k,v in cs.items() if isinstance(v,dict)}
except Exception as e: R["panels"]={"err":str(e)[:60]}
try:
    h=s3.head_object(Bucket=B,Key="data/canary-macro.json")
    R["canary_hot"]={"exists":True,"age_min":round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/60,1),"kb":round(h["ContentLength"]/1024,1)}
except Exception as e: R["canary_hot"]={"exists":False,"err":str(e)[:60]}
try:
    ls=s3.list_objects_v2(Bucket=B,Prefix="data/warm/fred-canary/",MaxKeys=30)
    R["fred_canary_keys"]=len(ls.get("Contents",[]))
except Exception: R["fred_canary_keys"]=0
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
bb=json.loads(inv["Payload"].read().decode())
R["inv"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
try:
    hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
    R["totals"]=hub.get("totals")
    R["top"]=[(p["slug"],p["n_keys"],p.get("hot_feeds")) for p in sorted(hub["providers"],key=lambda x:-x["n_keys"])[:10]]
    R["fred_row"]=next((p for p in hub["providers"] if p["slug"]=="fred"),None)
    R["still_zero"]=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
except Exception as e: R["hub_err"]=str(e)[:80]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("HUB v5 (map unwrapped, 738 engines) + canary fred-fix: totals="+json.dumps(R.get("totals"))
  +" top="+json.dumps(R.get("top"))[:260]+" fred="+json.dumps(R.get("fred_row"),default=str)[:120]
  +" · canary_hot="+json.dumps(R.get("canary_hot"))+" fred_canary_keys="+str(R.get("fred_canary_keys"))
  +" still_zero="+json.dumps(R.get("still_zero"))[:160]+". Verify /data.html + seal."),
 "evidence":[{"kind":"log","ref":"data/provider-catalog.json","snippet":"totals"}]})
bus({"action":"fanout_pending"})
R["verdict"]=("totals="+json.dumps(R.get("totals"))+" fred="+json.dumps(R.get("fred_row"),default=str)[:100]
 +" canary="+json.dumps(R.get("canary_hot"))+" panels="+json.dumps(R.get("panels"),default=str)[:220]+" fkeys="+str(R.get("fred_canary_keys")))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4516_v5x.json","w"),indent=1,default=str)
open("aws/ops/reports/4516_v5x.md","w").write("# ops 4513 — v4 — "+R["verdict"]+"\n- top: "+json.dumps(R.get("top"),default=str)+"\n- still_zero: "+json.dumps(R.get("still_zero"))+"\n")
print(R["verdict"])
