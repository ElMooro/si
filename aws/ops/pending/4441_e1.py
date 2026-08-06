"""ops 4441 — E1 v1 shipped: symbology master (SEC spine). 19/34."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-symbology-master"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4441,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
try:
    try:
        lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") in (None,"Successful"): break
            time.sleep(5)
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
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(15 5 * * ? *)",State="ENABLED",Description="ops4441 E1")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4441",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
    _=inv["Payload"].read()
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/symbology/master.json")["Body"].read())
    samp=d.get("by_ticker",{}).get("AAPL")
    R["master"]={"n_tickers":d.get("n_tickers"),"n_ciks":d.get("n_ciks"),
                 "coverage":d.get("coverage"),"AAPL":samp}
except Exception as e: R["feed_err"]=str(e)[:120]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
m=R.get("master") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("E1 v1 SHIPPED — 19/34. justhodl-symbology-master (nightly 05:15 UTC): SEC "
  "company_tickers.json -> data/symbology/master.json — the TICKER<->CIK<->NAME spine, "
  f"first run: {json.dumps({k:m.get(k) for k in ('n_tickers','n_ciks')})}, AAPL sample: "
  + json.dumps(m.get("AAPL"),default=str)[:220] + ". Enrichment-ready: cusip/isin/figi/sedol "
  "are explicit NULLS (never invented) with a status block naming each pending source "
  "(13f cusip-map join; OpenFIGI needs a key — flag to Khalid). F4 raw snapshot attached; "
  f"honest coverage: {json.dumps(m.get('coverage'),default=str)[:180]}. Verify+seal; next: "
  "E3 edgar-full-index (CIK spine now exists to join against)."),
 "evidence":[{"kind":"log","ref":"data/symbology/master.json","snippet":"by_ticker"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"19/34: +E1 symbology spine"})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — E1 live: {m.get('n_tickers')} tickers, {m.get('n_ciks')} CIKs" if m.get("n_tickers") else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4441_e1.json","w"),indent=1,default=str)
open("aws/ops/reports/4441_e1.md","w").write(f"# ops 4441 — E1 — {R['verdict']}\n- master: {json.dumps(m,default=str)[:600]}\n")
print(R["verdict"])
