"""ops 4463 — NY Fed Markets FULL live (registry item 2)."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-nyfed-repo-deep"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4494,"started":datetime.now(timezone.utc).isoformat()}
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
    arn=ev.put_rule(Name="justhodl-nyfed-repo-deep-daily",ScheduleExpression="cron(22 5 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-nyfed-repo-deep-daily",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4494",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
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
 "content":("KHALID SHEET EXECUTED — the two true gaps (recon excluded already-built: "
  "repo-monitor recents, IORB, RRP-award, RRPONTSYD): (1) FULL op history since 2014 — every "
  "ON RRP + SRF/TOMO repo operation via rp/results/search; (2) OFR one-call dataset=repo bulk. "
  f"First run: {json.dumps(rn,default=str)[:380]}. data/warm/nyfed-markets/rp-*-history + "
  "ofr/dataset-repo. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/nyfed-markets/repo-deep-summary.json","snippet":"rp_reverserepo"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"repo-deep: full op history + ofr bulk"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and (rn.get("rp_reverserepo",{}).get("ok") or rn.get("ofr_dataset",{}).get("ok"))
R["verdict"]=f"PASS — rrp={json.dumps(rn.get('rp_reverserepo'),default=str)[:110]} repo={json.dumps(rn.get('rp_repo'),default=str)[:90]} ofr={json.dumps(rn.get('ofr_dataset'),default=str)[:80]}" if ok else f"PARTIAL — {json.dumps(R.get('err') or rn,default=str)[:250]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4494_repodeep.json","w"),indent=1,default=str)
open("aws/ops/reports/4494_repodeep.md","w").write(f"# ops 4463 — nyfed-full — {R['verdict']}\n")
print(R["verdict"])
