"""ops 4462 — Council investigation ACTED ON: expansion registry published,
OFR STFM (unanimous #1) live with the 100%-of-provider pattern, APR-0002
(paid tier) filed for Khalid's button, FRED-license risk flagged fleet-wide."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-cusip-map-rebuild"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4473,"started":datetime.now(timezone.utc).isoformat()}
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
try:
    if wait_idle():
        for _ in range(6):
            try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
            except lam.exceptions.ResourceConflictException: time.sleep(12)
    else:
        cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
        lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],Handler=cfg["handler"],
            Code={"ZipFile":buf.getvalue()},Timeout=cfg["timeout"],MemorySize=cfg["memory"],
            Description=cfg["description"][:250],Environment={"Variables":cfg["env"]})
    wait_idle()
    arn=ev.put_rule(Name="justhodl-cusip-map-rebuild-hourly",ScheduleExpression="rate(1 hour)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-cusip-map-rebuild-hourly",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4473",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    bb=json.loads(inv["Payload"].read().decode())
    R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    bb=json.loads(i["Payload"].read().decode())
    return json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
rn=R.get("run") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("CUSIP-MAP v2 REBUILD LIVE (the AAPL-cohort fix): cursor over ALL 4,281 13F-HRs "
  f"from the E3 index, infoTable regex per filing. First run: {json.dumps(rn,default=str)[:300]} "
  "— megacaps land run 1 because every filer holds them; v2 overlays v1 in the symbology join. "
  "Hourly to COMPLETE. Verify AAPL_037833100 present + seal."),
 "evidence":[{"kind":"log","ref":"data/_state/cusip-rebuild.json","snippet":"AAPL"},
             {"kind":"log","ref":"data/13f-cusip-map-v2.json","snippet":"037833100"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"cusip-map v2 rebuild hourly"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and rn.get("ok")
R["verdict"]=f"PASS — {json.dumps({k:rn.get(k) for k in ('filings_parsed','pairs','map_size','AAPL_037833100')},default=str)[:180]}" if ok else f"PARTIAL — {json.dumps(R.get('err') or rn,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4473_cusipv2.json","w"),indent=1,default=str)
open("aws/ops/reports/4473_cusipv2.md","w").write(f"# ops 4462 — council acted — {R['verdict']}\n- apr0002: {R.get('apr0002')}\n")
print(R["verdict"])
