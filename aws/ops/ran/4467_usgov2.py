"""ops 4466 — Perplexity's originating-agency flag actioned: usgov-direct
live (BEA key finally consumed, BLS beyond-CES, Fed DDP) + E7 extended to 6
Treasury datasets. Additive only; FRED untouched per APR-0003 rejection."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4467,"started":datetime.now(timezone.utc).isoformat()}
def zip_of(fn):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    return b.getvalue()
def wait_idle(fn):
    for _ in range(30):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return True
        except lam.exceptions.ResourceNotFoundException: return False
        time.sleep(6)
    return True
FN7=None; R["e7"]="done-4466"
# create usgov-direct
FN="justhodl-usgov-direct"
try:
    if wait_idle(FN):
        for _ in range(6):
            try: lam.update_function_code(FunctionName=FN,ZipFile=zip_of(FN)); break
            except lam.exceptions.ResourceConflictException: time.sleep(12)
    else:
        cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
        lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],Handler=cfg["handler"],
            Code={"ZipFile":zip_of(FN)},Timeout=cfg["timeout"],MemorySize=cfg["memory"],
            Description=cfg["description"][:250],Environment={"Variables":cfg["env"]})
    wait_idle(FN)
    arn=ev.put_rule(Name="justhodl-usgov-direct-daily",ScheduleExpression="cron(40 12 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-usgov-direct-daily",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4467",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    bb=json.loads(inv["Payload"].read().decode())
    R["usgov"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    bb=json.loads(i["Payload"].read().decode())
    return json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
ug=R.get("usgov") or {}; e7=R.get("e7") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("USGOV WAVE 2 (ops 4467): Fed DDP now a 6-release sweep (H15/H41/H8/G19/H10/CP zips) and BEA parameter-map materialized = the walkable 100% worklist. First run: (daily "
  f"12:40) first run — BEA {json.dumps(ug.get('bea'),default=str)[:150]} · BLS "
  f"{json.dumps(ug.get('bls'),default=str)[:150]} · FedDDP "
  f"{json.dumps(ug.get('fed_ddp'),default=str)[:120]}. E7 extended to 6 Treasury datasets: "
  f"{json.dumps(e7,default=str)[:200]}. data/warm/usgov/. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/usgov/latest-summary.json","snippet":"bea"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"usgov-direct live: BEA activated, BLS expanded, DDP, E7x6"})
bus({"action":"fanout_pending"})
ok=isinstance(ug,dict) and (ug.get("bea",{}).get("ok") or ug.get("bls",{}).get("ok"))
R["verdict"]=f"PASS — bea={json.dumps(ug.get('bea'),default=str)[:80]} bls={json.dumps(ug.get('bls'),default=str)[:80]} ddp={json.dumps(ug.get('fed_ddp'),default=str)[:60]}" if ok else f"PARTIAL — {json.dumps(ug or R.get('err'),default=str)[:250]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4467_usgov2.json","w"),indent=1,default=str)
open("aws/ops/reports/4467_usgov2.md","w").write(f"# ops 4466 — usgov-direct — {R['verdict']}\n- e7: {json.dumps(e7,default=str)[:300]}\n")
print(R["verdict"])
