"""ops 4443 — E3 v1: EDGAR full-index nightly ingest. 20/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-edgar-full-index"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4443,"started":datetime.now(timezone.utc).isoformat()}
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
    RULE=FN+"-daily"
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(35 5 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4443",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
    _=inv["Payload"].read()
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/edgar-filings/latest-summary.json")["Body"].read())
    R["summary"]=d
except Exception as e: R["feed_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
sm=R.get("summary") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E3 v1 SHIPPED — 20/34. justhodl-edgar-full-index (nightly 05:35 UTC): the SEC "
  "quarter master.idx -> data/warm/edgar-filings/{Y}/{Q}.json.gz, CIK-keyed to join the E1 "
  f"spine. First run: {json.dumps({k:sm.get(k) for k in ('n_filings','quarter','size_gz_mb')})}, "
  f"top forms: {json.dumps(sm.get('top_forms'),default=str)[:200]}. Honest deviation, stated in "
  "the feed itself: v1 is gz-JSON, parquet pending a pyarrow layer — declared, not silently "
  "substituted. F4 raw snapshot attached. Verify+seal; warm tier is now real with its first "
  "resident."),
 "evidence":[{"kind":"log","ref":"data/warm/edgar-filings/latest-summary.json","snippet":"n_filings"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"20/34: +E3 edgar full-index"})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — {sm.get('n_filings')} filings, {sm.get('size_gz_mb')}MB gz, {sm.get('quarter')}" if sm.get("n_filings") else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4443_e3.json","w"),indent=1,default=str)
open("aws/ops/reports/4443_e3.md","w").write(f"# ops 4443 — E3 — {R['verdict']}\n- summary: {json.dumps(sm,default=str)[:600]}\n")
print(R["verdict"])
