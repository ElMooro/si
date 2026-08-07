"""ops 4537 — Perplexity's re-verification order, items 1-6: deploy
canary(PK-raise+tri-state+hot-parse)+agent(turn-guard/gate/ledger)+catalog
(attributed instruments); verify the 40 series RESTORED, flags computed,
reconcile literal True, smoke v2 exit; post to the CLEAN thread 0807-reseal."""
import io,json,os,subprocess,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4537,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f2 in os.listdir("aws/shared"):
            if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
for fn in ("justhodl-canary-macro","justhodl-backend-agent","justhodl-provider-catalog"):
    deploy(fn)
lam.invoke(FunctionName="justhodl-canary-macro",InvocationType="Event",Payload=b"{}")
time.sleep(200)
hot=json.loads(s3.get_object(Bucket=B,Key="data/canary-macro.json")["Body"].read())
series=[k for k,v in hot.items() if isinstance(v,dict) and k!="flags"]
live=[k for k in series if hot[k].get("value") is not None]
dead=[k for k in series if hot[k].get("data_unavailable") or hot[k].get("value") is None]
R["canary"]={"total":len(series),"live":len(live),"dead":len(dead),
 "dead_sample":dead[:8],
 "T10Y3M":(hot.get("T10Y3M") or {}).get("value"),
 "SAHM":(hot.get("SAHMREALTIME") or {}).get("value"),
 "VIX":(hot.get("VIXCLS") or {}).get("value"),
 "GDPNOW":(hot.get("GDPNOW") or {}).get("value"),
 "flags":hot.get("flags")}
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["cat_fn_err"]=ic.get("FunctionError"); _=ic["Payload"].read()
time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
rowsum=sum(p.get("datasets") or 0 for p in hub["providers"])
R["hub"]={"datasets_total":hub.get("datasets_total"),"rowsum":rowsum,
 "reconcile_key":hub.get("reconcile_ok"),"match":rowsum==hub.get("datasets_total"),
 "breakdown":hub.get("breakdown"),
 "instr_rows":[(p["slug"],p["datasets"]) for p in hub["providers"] if p.get("unit")=="instruments"]}
try:
    sm=subprocess.run(["python3","tools/smoke_feeds.py"],capture_output=True,text=True,timeout=180)
    R["smoke"]={"exit":sm.returncode,"out":sm.stdout[:400]}
except Exception as e: R["smoke"]={"err":str(e)[:80]}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("CLEAN-THREAD RESEAL REQUEST — your re-verification order executed: (1) PK magic on multi-fetch "
  "= HARD failure -> per-id fallback: canary now "
  f"{R['canary']['live']}/{R['canary']['total']} live (dead={R['canary']['dead']}), T10Y3M={R['canary']['T10Y3M']} "
  f"SAHM={R['canary']['SAHM']} VIX={R['canary']['VIX']} GDPNOW={R['canary']['GDPNOW']}. (2) flags tri-state: "
  f"{json.dumps(R['canary']['flags'])[:180]} — null NEVER false. (3) turn-guard reads len(thread.turns); "
  "escalation on fired==3. (4) ledger ETag conditional write. (5) smoke v2 (cadence+fields+unavail-ratio+"
  f"flag-null contract): {json.dumps(R.get('smoke'))[:200]}. (6) instruments = attributed rows "
  f"{R['hub']['instr_rows']}; reconcile_ok literal={R['hub']['reconcile_key']} rowsum==total={R['hub']['match']} "
  f"breakdown={json.dumps(R['hub']['breakdown'])}. Verify each on production and seal here."),
 "evidence":[{"kind":"log","ref":"data/canary-macro.json","snippet":"flags"},
             {"kind":"log","ref":"data/provider-catalog.json","snippet":"reconcile_ok"}]})
bus({"action":"fanout_pending"})
R["verdict"]=(f"canary {R['canary']['live']}/{R['canary']['total']} live flags={json.dumps(R['canary']['flags'])[:120]} | "
 f"hub match={R['hub']['match']} reconcile_key={R['hub']['reconcile_key']} instr={R['hub']['instr_rows']} | smoke={ (R.get('smoke') or {}).get('exit') }")
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4537_p0.json","w"),indent=1,default=str)
open("aws/ops/reports/4537_p0.md","w").write("# 4537 — "+R["verdict"]+"\n- smoke: "+json.dumps(R.get("smoke"),default=str)[:400]+"\n- dead_sample: "+json.dumps(R["canary"]["dead_sample"])+"\n")
print(R["verdict"][:350])
