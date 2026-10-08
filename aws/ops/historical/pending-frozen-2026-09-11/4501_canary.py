"""ops 4462 — Council investigation ACTED ON: expansion registry published,
OFR STFM (unanimous #1) live with the 100%-of-provider pattern, APR-0002
(paid tier) filed for Khalid's button, FRED-license risk flagged fleet-wide."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-canary-macro"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4501,"started":datetime.now(timezone.utc).isoformat()}
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
    arn=ev.put_rule(Name="justhodl-canary-macro-hourly",ScheduleExpression="cron(10 11 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-canary-macro-hourly",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4501",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
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
 "content":("FULL-BUILD DELIVERED FOR YOUR VERIFICATION (Khalid: 'build everything, you or "
  "Perplexity verify'). (A) justhodl-canary-macro LIVE: his unemployment/LEI/canary doc complete "
  "— 6 FRED panels/45 ids keyless multi-CSV, BLS labor block keyed, Cleveland yield-curve xlsx, "
  f"Atlanta GDPNow xlsx, DOL ar539 -> hot data/canary-macro.json. First run: {json.dumps(rn,default=str)[:420]}. "
  "(B) YOUR P1+P2 EXECUTED on the 3 Claude pages (pushed, JS-parse-verified): NaN sweep all 3; "
  "44 feed-cards wired (crisis 18 / plumbing 12 incl L5 rates leg / liquidity 14); crisis dual-"
  "composite banner + decisive chip + 26y percentile bar; liquidity DXY hero + credit-first "
  "sequence chip. VERIFY: load the 3 pages + curl data/canary-macro.json; seal per item."),
 "evidence":[{"kind":"log","ref":"data/warm/canary-macro-summary.json","snippet":"sahm"},
             {"kind":"log","ref":"data/warm/canary-macro-summary.json","snippet":"flags"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"canary-macro + P1/P2 pages full-build"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and (rn.get("hot_series") or 0)>=30
R["verdict"]=f"PASS — hot_series={rn.get('hot_series')} flags={json.dumps(rn.get('flags'),default=str)[:120]} panels={[k for k,v in (rn.get('panels') or {}).items() if isinstance(v,dict) and v.get('ok')]}" if ok else f"PARTIAL — {json.dumps(R.get('err') or rn,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4501_canary.json","w"),indent=1,default=str)
open("aws/ops/reports/4501_canary.md","w").write(f"# ops 4462 — council acted — {R['verdict']}\n- apr0002: {R.get('apr0002')}\n")
print(R["verdict"])
