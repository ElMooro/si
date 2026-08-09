"""ops 4547 — create justhodl-series-extractor, schedule every 5 min,
run once against Eurostat (the proof-of-pattern flow), report real
extracted counts. Then fan-out to StatCan/OECD is a follow-on op once
their readers are written (StatCan zipped-CSV+COORDINATE, OECD SDMX-JSON
structure.dimensions — different parsers, same output contract)."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-series-extractor"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
iam=boto3.client("iam",region_name=REGION)
R={"ops":4547,"started":datetime.now(timezone.utc).isoformat()}
cfg=json.load(open(f"aws/lambdas/{FN}/config.json"))
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
exists=True
try: lam.get_function_configuration(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
else:
    lam.create_function(FunctionName=FN,Runtime=cfg["runtime"],Role=cfg["role"],
        Handler=cfg["handler"],Code={"ZipFile":buf.getvalue()},Timeout=cfg["timeout"],
        MemorySize=cfg["memory"],Environment={"Variables":cfg.get("env",{})},
        Description=cfg.get("description",""))
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
rule="justhodl-series-extractor-5min"
ev.put_rule(Name=rule,ScheduleExpression="rate(5 minutes)",State="ENABLED")
ev.put_targets(Rule=rule,Targets=[{"Id":"t1","Arn":arn,"Input":json.dumps({"provider":"eurostat"})}])
try:
    lam.add_permission(FunctionName=FN,StatementId="evbridge",Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule}")
except lam.exceptions.ResourceConflictException: pass
# kick two synchronous rounds to prove the pattern now (not wait for cron)
for i in range(2):
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"provider":"eurostat"}).encode())
    body=inv["Payload"].read().decode()
    R.setdefault("runs",[]).append({"fn_err":inv.get("FunctionError"),"body":body[:200]})
try:
    m=json.loads(s3.get_object(Bucket=B,Key="data/providers/eurostat/series-manifest.json")["Body"].read())
    R["manifest"]=m
except Exception as e: R["manifest_err"]=str(e)[:80]
try:
    pg0=json.loads(s3.get_object(Bucket=B,Key="data/providers/eurostat/series/page-0000.json")["Body"].read())
    R["page0_sample"]=pg0.get("rows",[])[:3]
    R["page0_count"]=pg0.get("count")
except Exception as e: R["page0_err"]=str(e)[:80]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("EXTRACTOR SHIPPED (justhodl-series-extractor, ops 4547) — Eurostat reference impl, per your spec: "
  f"manifest={json.dumps(R.get('manifest'))[:250]} · page0_count={R.get('page0_count')} · sample="
  f"{json.dumps(R.get('page0_sample'))[:300]}. series_extracted kept SEPARATE from datasets_total per your "
  "guardrail; UI requires 2+ char search (Series tab, search-gated, no unfiltered render). Scheduled every 5min, "
  "resumable via flows_done state. StatCan/OECD readers next — different parsers, same output contract. "
  "engines[]/finviz fixes also live (see Wave-A post). Verify one flow's series against source."),
 "evidence":[{"kind":"log","ref":"data/providers/eurostat/series-manifest.json","snippet":"series_extracted"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"manifest={json.dumps(R.get('manifest'))} page0={R.get('page0_count')}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4547.json","w"),indent=1,default=str)
open("aws/ops/reports/4547.md","w").write("# 4547 — "+R["verdict"]+"\n- sample: "+json.dumps(R.get("page0_sample"),default=str)+"\n")
print(R["verdict"][:350])
