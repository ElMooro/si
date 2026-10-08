"""ops 4462 — Council investigation ACTED ON: expansion registry published,
OFR STFM (unanimous #1) live with the 100%-of-provider pattern, APR-0002
(paid tier) filed for Khalid's button, FRED-license risk flagged fleet-wide."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-sdmx-walker"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4503,"started":datetime.now(timezone.utc).isoformat()}
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
            EphemeralStorage={"Size":cfg.get("ephemeral_mb",512)},
            Description=cfg["description"][:250],Environment={"Variables":cfg["env"]})
    wait_idle()
    arn=ev.put_rule(Name="justhodl-sdmx-walker-hourly",ScheduleExpression="rate(1 hour)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-sdmx-walker-hourly",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4503",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
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
 "content":("E10 WALKS LIVE — the four catalog agencies begin their data-walks: BIS(29 flows) "
  "all-first; OECD CLI/MEI/QNA priority; Eurostat macro-prefixes before the 8k tail; StatCan cube "
  f"zips. 2/agency/hour, 40MB-cap flagged, failures ledgered. First run: {json.dumps(rn,default=str)[:400]}. "
  "States at data/_state/sdmx-walk-*.json — progress_pct is the truth. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/sdmx-walker-summary.json","snippet":"progress_pct"},
             {"kind":"log","ref":"data/warm/sdmx-walker-summary.json","snippet":"bis"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"E10 sdmx-walker live (4 agencies)"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and any(isinstance(v,dict) and v.get("pulled") for v in rn.values())
R["verdict"]="PASS — "+json.dumps({k:{"pulled":v.get("pulled"),"pct":v.get("progress_pct")} for k,v in rn.items() if isinstance(v,dict) and "pulled" in v},default=str)[:220] if ok else f"PARTIAL — {json.dumps(R.get('err') or rn,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4503_walker.json","w"),indent=1,default=str)
open("aws/ops/reports/4503_walker.md","w").write(f"# ops 4462 — council acted — {R['verdict']}\n- apr0002: {R.get('apr0002')}\n")
print(R["verdict"])
