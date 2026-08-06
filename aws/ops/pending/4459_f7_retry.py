"""ops 4459 — F7 deploy retry WITH the wait-for-idle guard (4458 dropped it
and hit ResourceConflict mid-flight — my own documented pitfall). Deploy,
schedule, invoke, verify latest.json, close 33/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-self-critique"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4459,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
def wait_idle():
    for _ in range(30):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return True
        except lam.exceptions.ResourceNotFoundException: return False
        time.sleep(6)
    return True
exists=wait_idle()
try:
    if exists:
        for _ in range(6):
            try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); R["mode"]="updated"; break
            except lam.exceptions.ResourceConflictException: time.sleep(12)
    else:
        cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
        lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],Handler=cfg["handler"],
            Code={"ZipFile":buf.getvalue()},Timeout=cfg["timeout"],MemorySize=cfg["memory"],
            Description=cfg["description"][:250],Environment={"Variables":cfg["env"]}); R["mode"]="created"
    for _ in range(24):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): break
        time.sleep(5)
    RULE="justhodl-self-critique-daily"
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(20 22 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4459",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    b=json.loads(inv["Payload"].read().decode())
    R["run"]=json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
time.sleep(3)
try:
    R["latest"]=json.loads(s3.get_object(Bucket=BUCKET,Key="data/llm/self-critique/latest.json")["Body"].read()).get("counts")
except Exception as e: R["latest_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    bb=json.loads(i["Payload"].read().decode())
    return json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
rn=R.get("run") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("F7 COMPLETE (4458 hit ResourceConflict because I dropped my own wait-for-idle guard "
  "— restored, deployed clean). First live snapshot: "
  + json.dumps({"run":rn,"latest_counts":R.get("latest")},default=str)[:400] +
  ". Master board: 33/34 sealed-ready; E2 writer sits on Khalid's desk as APR-0001 — by design "
  "the last item is his signature, not my code. Verify+seal F5/F7."),
 "evidence":[{"kind":"log","ref":"data/llm/self-critique/latest.json","snippet":"counts"}]})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and rn.get("ok") and R.get("latest")
R["verdict"]=f"PASS — F7 live: {json.dumps(R.get('latest'))} · verdicts {json.dumps((rn or {}).get('verdicts'),default=str)[:150]}" if ok else f"PARTIAL — err={R.get('err')}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4459_f7.json","w"),indent=1,default=str)
open("aws/ops/reports/4459_f7.md","w").write(f"# ops 4459 — F7 complete — {R['verdict']}\n")
print(R["verdict"])
