"""ops 4462 — Council investigation ACTED ON: expansion registry published,
OFR STFM (unanimous #1) live with the 100%-of-provider pattern, APR-0002
(paid tier) filed for Khalid's button, FRED-license risk flagged fleet-wide."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-ofr-stfm"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4462,"started":datetime.now(timezone.utc).isoformat()}
s3.put_object(Bucket=BUCKET,Key="data/audit/provider-expansion-registry.json",
 Body=open("aws/infra/provider-expansion-registry.json","rb").read(),
 ContentType="application/json",CacheControl="no-cache")
# APR-0002: paid tier on Khalid's button
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/approvals.json")["Body"].read())
except Exception:
    doc={"pending":[],"decided":[]}
if not any(x.get("id")=="APR-0002" for x in doc.get("pending",[])+doc.get("decided",[])):
    doc.setdefault("pending",[]).append({"id":"APR-0002","filed_at":R["started"],
     "proposed_by":"model-council (5 models, unanimous on sequencing)",
     "title":"Paid data tier: FINRA TRACE $1,650/mo + Cboe All-Access T3 $2,499/mo (institutional credit + options parity) — council says Q3/Q4 AFTER free-tier saturation",
     "detail":{"monthly_total":"~$4,149","alternatives":"free tier first: OFR/NYFed-full/SEC-bulk/GLEIF/BIS lift mix to ~35% fleet at $0","council_note":"gpt5.6: do NOT buy until a named workflow pays for it"}})
    doc["as_of"]=R["started"]
    s3.put_object(Bucket=BUCKET,Key="data/audit/approvals.json",
     Body=json.dumps(doc,indent=1,default=str).encode(),ContentType="application/json",CacheControl="no-cache")
    R["apr0002"]="filed"
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
    arn=ev.put_rule(Name="justhodl-ofr-stfm-hourly",ScheduleExpression="rate(1 hour)",State="ENABLED")["RuleArn"]
    fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    ev.put_targets(Rule="justhodl-ofr-stfm-hourly",Targets=[{"Id":FN[:60],"Arn":fa}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4462",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
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
 "content":("COUNCIL ACTED ON (5-model report investigated): registry live at "
  "data/audit/provider-expansion-registry.json (validated-today list, corrected gemini's 99%-"
  "parity error, CRITICAL license flags: FRED ToU vs raw-archiving, CoinMetrics NC, Yahoo/MOVE). "
  f"OFR STFM (unanimous #1, 0%->live) first run: {json.dumps(rn,default=str)[:250]} — "
  "100%-of-provider pattern: catalog-discover -> cursored full-history tranches -> COMPLETE. "
  "Hourly convergence. APR-0002 (paid tier ~$4,149/mo) filed for Khalid's BUTTON per gpt5.6's "
  "'not until a workflow pays' rule. Next E10 worklist: NYFed-full, SEC-bulk, GLEIF, BIS. "
  "Verify+seal registry + OFR."),
 "evidence":[{"kind":"log","ref":"data/warm/ofr/state.json","snippet":"progress_pct"},
             {"kind":"log","ref":"data/audit/provider-expansion-registry.json","snippet":"license_risks"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"council: registry+OFR live, APR-0002 filed"})
bus({"action":"fanout_pending"})
ok=isinstance(rn,dict) and rn.get("ok")
R["verdict"]=f"PASS — OFR: {json.dumps(rn,default=str)[:150]}" if ok else f"PARTIAL — {json.dumps(R.get('err') or rn,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4462_council.json","w"),indent=1,default=str)
open("aws/ops/reports/4462_council.md","w").write(f"# ops 4462 — council acted — {R['verdict']}\n- apr0002: {R.get('apr0002')}\n")
print(R["verdict"])
