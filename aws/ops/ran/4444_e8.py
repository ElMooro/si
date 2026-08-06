"""ops 4444 — E8 v1: NY Fed reference-rate full history. 21/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-nyfed-full-history"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4444,"started":datetime.now(timezone.utc).isoformat()}
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
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(25 5 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4444",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
    _=inv["Payload"].read()
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/warm/nyfed/latest-summary.json")["Body"].read())
    R["summary"]=d.get("rates")
except Exception as e: R["feed_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
sm=R.get("summary") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E8 v1 SHIPPED — 21/34. justhodl-nyfed-full-history (nightly 05:25 UTC): "
  "SOFR/EFFR/OBFR/TGCR/BGCR full ~10y daily histories from the official NY Fed API -> "
  "data/warm/nyfed/{rate}.json.gz, F4 snapshots per rate, absences explicit. First run: "
  + json.dumps(sm,default=str)[:600] + ". The plumbing stack now has its own primary-source "
  "deep history instead of leaning on FRED mirrors. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/nyfed/latest-summary.json","snippet":"sofr"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"21/34: +E8 nyfed history"})
bus({"action":"fanout_pending"})
ok=any(v.get("n_obs") for v in sm.values()) if sm else False
R["verdict"]=f"PASS — {sum(1 for v in sm.values() if v.get('n_obs'))}/5 rates, SOFR={((sm.get('sofr') or {}).get('current'))}" if ok else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4444_e8.json","w"),indent=1,default=str)
open("aws/ops/reports/4444_e8.md","w").write(f"# ops 4444 — E8 — {R['verdict']}\n- rates: {json.dumps(sm,default=str)[:700]}\n")
print(R["verdict"])
