"""ops 4552 — create+deploy justhodl-fred-catalog, run Phase 1 (category
discovery, should complete fast — FRED has a few thousand categories,
not 800k) to completion via repeated invokes within this op, then kick
Phase 2 once to get a REAL first series-discovered number. Report the
true FRED catalog scale before any full-history ingestion is considered."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4552,"started":datetime.now(timezone.utc).isoformat()}
cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
exists=True
try: lam.get_function_configuration(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
else:
    lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],
        Handler=cfg["handler"],Code={"ZipFile":buf.getvalue()},Timeout=cfg["timeout"],
        MemorySize=cfg["memory"],Environment={"Variables":cfg.get("env",{})},
        Description=cfg.get("description",""))
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
arn=c["FunctionArn"]
rule="justhodl-fred-catalog-5min"
ev.put_rule(Name=rule,ScheduleExpression="rate(5 minutes)",State="ENABLED")
ev.put_targets(Rule=rule,Targets=[{"Id":"cats","Arn":arn,"Input":json.dumps({"phase":"categories"})},
                                   {"Id":"meta","Arn":arn,"Input":json.dumps({"phase":"series_meta"})}])
try:
    lam.add_permission(FunctionName=FN,StatementId="evb",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule}")
except lam.exceptions.ResourceConflictException: pass
# Phase 1: run to completion NOW (categories are cheap — expect this to finish in 1-3 calls)
cat_rounds=0
for _ in range(6):
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"phase":"categories"}).encode())
    body=json.loads(inv["Payload"].read().decode())
    rn=json.loads(body["body"]) if isinstance(body,dict) and "body" in body else body
    cat_rounds+=1
    if rn.get("status")=="COMPLETE": break
R["phase1"]={"rounds":cat_rounds,"final":rn}
# Phase 2: one real round to get a genuine series_discovered number
inv2=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"phase":"series_meta"}).encode())
body2=json.loads(inv2["Payload"].read().decode())
rn2=json.loads(body2["body"]) if isinstance(body2,dict) and "body" in body2 else body2
R["phase2_round1"]=rn2
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"FRED CATALOG DISCOVERY (Khalid: what about FRED remaining data) — new engine "
  f"justhodl-fred-catalog, additive only. Phase1 (category tree): {json.dumps(R['phase1'])[:200]}. "
  f"Phase2 round1 (series metadata, categories walked so far): {json.dumps(rn2)[:250]}. This reveals FRED's "
  "TRUE series count before any full-observation ingestion is considered (that's a separate, much larger "
  "phase — storage/scope decision, not started). Existing 298 curated canary-macro keys untouched."),
 "evidence":[{"kind":"log","ref":"data/providers/fred/catalog-manifest.json","snippet":"series_discovered"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"phase1={json.dumps(R['phase1'])} phase2={json.dumps(rn2)}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4552.json","w"),indent=1,default=str)
open("aws/ops/reports/4552.md","w").write("# 4552 — "+R["verdict"]+"\n")
print(R["verdict"][:350])
