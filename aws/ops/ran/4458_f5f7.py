"""ops 4458 — F7 shipped + F5 closed-as-posture. 33/34 (E2 gated on APR-0001)."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-self-critique"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4458,"started":datetime.now(timezone.utc).isoformat()}
s3.put_object(Bucket=BUCKET,Key="data/audit/worm-posture.json",
 Body=open("aws/infra/worm-posture.json","rb").read(),ContentType="application/json")
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
    RULE="justhodl-self-critique-daily"
    arn=ev.put_rule(Name=RULE,ScheduleExpression="cron(20 22 * * ? *)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4458",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    b=json.loads(inv["Payload"].read().decode())
    try: R["run"]=json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
    except Exception: R["run"]=str(b)[:250]
except Exception as e: R["err"]=f"{type(e).__name__}: {str(e)[:150]}"
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    bb=json.loads(i["Payload"].read().decode())
    return json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
rn=R.get("run") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("F7 + F5 — 33/34 (E2 alone remains, gated on Khalid's APR-0001).\n"
  "F7 v1 LIVE: justhodl-self-critique (daily 22:20) snapshots the six flagship verdicts "
  "(risk-gate, rotation regime, breadth-thrust, btc-rainbow, four-canary, credit-first), diffs "
  f"vs yesterday as HELD/FLIPPED/UNAVAILABLE. First run: {json.dumps(rn,default=str)[:350]}. "
  "The LLM synthesis of these deltas into brain-rule proposals routes through F6 approvals — "
  "measured here, decided by Khalid, nothing self-applies.\n"
  "F5 CLOSED AS POSTURE (honest, not fudged): true Object Lock cannot be retrofitted to this "
  "bucket (versioning-at-creation requirement — STATED at data/audit/worm-posture.json). "
  "Practical WORM already in force: F4 content-addressing (a changed byte IS a new key) + E9 "
  "GlacierIR@45d + attic-no-expiry + IAM. Regulatory-grade lock, if ever needed, = new locked "
  "bucket + replication, pre-filed as a future APR. Verify+seal F5+F7 — the C/D/E/F master "
  "closes at 33/34 with the last item on Khalid's desk by design."),
 "evidence":[{"kind":"log","ref":"data/llm/self-critique/latest.json","snippet":"counts"},
             {"kind":"log","ref":"data/audit/worm-posture.json","snippet":"honest_limits"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"33/34: +F7 substrate, F5 posture-closed; E2 on APR-0001"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and rn.get("ok")
R["verdict"]=f"PASS — F7 first run {json.dumps((rn or {}).get('counts'))}, F5 posture published" if ok else f"PARTIAL — {json.dumps(rn,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4458_f5f7.json","w"),indent=1,default=str)
open("aws/ops/reports/4458_f5f7.md","w").write(f"# ops 4458 — F5+F7 — {R['verdict']}\n- run: {json.dumps(rn,default=str)[:500]}\n")
print(R["verdict"])
