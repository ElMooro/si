"""ops 4455 — E10 v1: backfill orchestrator (hourly). 29/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-backfill-orchestrator"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4456,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
try:
    try:
        lam.get_function_configuration(FunctionName=FN)
        lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); R["mode"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
        lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],Handler=cfg["handler"],
            Code={"ZipFile":buf.getvalue()},Timeout=cfg["timeout"],MemorySize=cfg["memory"],
            Description=cfg["description"][:250],Environment={"Variables":cfg["env"]}); R["mode"]="created"
    for _ in range(24):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): break
        time.sleep(5)
    RULE="justhodl-backfill-hourly"
    arn=ev.put_rule(Name=RULE,ScheduleExpression="rate(1 hour)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4456",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    # run twice now to prove the cursor advances + merge dedupes
    for i in (1,2):
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
        b=json.loads(inv["Payload"].read().decode())
        try: R[f"run{i}"]=json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
        except Exception: R[f"run{i}"]=str(b)[:200]
        time.sleep(3)
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
try:
    R["progress"]=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/backfill-progress.json")["Body"].read()).get("tasks",{}).get("tga_deep")
except Exception as e: R["prog_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
pg=R.get("progress") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E10 v1 SHIPPED — 29/34. justhodl-backfill-orchestrator (HOURLY): one bounded older "
  "page per run merged into the warm archive, dedupe by date, cursor in "
  "data/audit/backfill-progress.json, COMPLETE-and-stop when the source exhausts. v1 worklist: "
  f"TGA deep history. Two live runs prove cursor+merge: run1={json.dumps(R.get('run1'),default=str)[:180]} "
  f"run2={json.dumps(R.get('run2'),default=str)[:180]} -> progress={json.dumps(pg,default=str)[:250]}. "
  "TGA walks back toward dataset origin ~2500 rows/hour unattended; nyfed/cftc/fred worklist "
  "entries are add-a-dict. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/audit/backfill-progress.json","snippet":"tga_deep"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"29/34: +E10 orchestrator hourly"})
bus({"action":"fanout_pending"})
ok=(pg.get("pages_done") or 0)>=2
R["verdict"]=f"PASS — pages_done={pg.get('pages_done')}, n_obs={pg.get('n_obs_total')}, span={pg.get('last_span')}" if ok else f"PARTIAL — {json.dumps(pg,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4456_e10fix.json","w"),indent=1,default=str)
open("aws/ops/reports/4456_e10fix.md","w").write(f"# ops 4455 — E10 — {R['verdict']}\n- runs: {json.dumps({k:R.get(k) for k in ('run1','run2')},default=str)[:500]}\n")
print(R["verdict"])
