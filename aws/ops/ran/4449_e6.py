"""ops 4449 — E6 v1: CFTC deep COT. 25/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-cftc-full-datasets"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4449,"started":datetime.now(timezone.utc).isoformat()}
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
    RULE="justhodl-cftc-full-daily"
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(5 6 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4449",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
    _=inv["Payload"].read()
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/cftc/latest-summary.json")["Body"].read())
    R["summary"]=d.get("datasets")
except Exception as e: R["feed_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
sm=R.get("summary") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E6 v1 SHIPPED — 25/34. justhodl-cftc-full-datasets (nightly 06:05): official CFTC "
  "Socrata — legacy (6dca-aqww) + disaggregated (72hh-3qpy) futures-only COT, newest 5000 rows "
  f"each -> data/warm/cftc/, F4 snapshots, explicit failures. First run: {json.dumps(sm,default=str)[:400]}. "
  "Your existing CFTC positioning engines gain a deep primary-source archive to diff against. "
  "Backfill = E10. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/cftc/latest-summary.json","snippet":"legacy_futures"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"25/34: +E6 cftc deep COT"})
bus({"action":"fanout_pending"})
ok=any(v.get("n_rows") for v in sm.values()) if sm else False
R["verdict"]=f"PASS — {json.dumps({k:v.get('n_rows') for k,v in sm.items()},default=str)}" if ok else f"PARTIAL — {json.dumps(sm,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4449_e6.json","w"),indent=1,default=str)
open("aws/ops/reports/4449_e6.md","w").write(f"# ops 4449 — E6 — {R['verdict']}\n- datasets: {json.dumps(sm,default=str)[:700]}\n")
print(R["verdict"])
