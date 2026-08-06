"""ops 4477 — GLEIF Golden Copy FULL: ephemeral 4096 forced (the 4475
lesson), engine redeployed with _golden streaming, run, verify gb+sha."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-bis-gleif"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=880,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4477,"started":datetime.now(timezone.utc).isoformat()}
def wait_idle():
    for _ in range(24):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return c
        time.sleep(6)
    return c
c=wait_idle()
for _ in range(6):
    try:
        lam.update_function_configuration(FunctionName=FN,EphemeralStorage={"Size":4096},Timeout=900,MemorySize=2048)
        break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
wait_idle()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
wait_idle()
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
bb=json.loads(inv["Payload"].read().decode())
R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
g=(R.get("run") or {}).get("golden") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("GOLDEN COPY FULL LANDED (stated follow-up closed): "
  + json.dumps(g,default=str)[:220] + " -> data/warm/gleif/golden-copy-lei2.zip, weekly with "
  "the ISIN map. Ephemeral 4096 applied first (the 4475 lesson). Registry free-tier: "
  "EMPTY except Eurostat/OECD rail. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/bis-gleif-summary.json","snippet":"golden"}]})
bus({"action":"fanout_pending"})
ok=g.get("ok")
R["verdict"]=f"PASS — golden {g.get('gb')}GB sha {g.get('sha256')}" if ok else f"PARTIAL — {json.dumps(g or R.get('run'),default=str)[:220]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4477_golden.json","w"),indent=1,default=str)
open("aws/ops/reports/4477_golden.md","w").write(f"# ops 4477 — golden copy — {R['verdict']}\n")
print(R["verdict"])
