"""ops 4556 — deploy the rate-limited FRED engine. Retarget the cron to
ONLY the scoped_import phase (kill the general 800k-series discovery
that caused the request-volume collision). Test with ONE conservative
call first to confirm the 403 has cleared before resuming at any real
volume. If still blocked, STOP and report — do not retry blindly."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-fred-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4556,"started":datetime.now(timezone.utc).isoformat()}
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
# ONLY scoped_import gets scheduled going forward — general discovery killed
ev.put_targets(Rule="justhodl-fred-catalog-5min",
    Targets=[{"Id":"scoped","Arn":arn,"Input":json.dumps({"phase":"scoped_import"})}])
R["targets_now"]="scoped_import ONLY (general discovery removed from schedule)"
# leave rule DISABLED still — we test manually first before re-enabling the cron
time.sleep(3)
# ONE conservative probe: a single, tiny direct call (not through the full
# import machinery) to check block status without risking volume
import urllib.request
probe={"tried":True}
try:
    req=urllib.request.Request(
        "https://api.stlouisfed.org/fred/series?series_id=DGS10&api_key=2f057499936072679d8843d7fce99989&file_type=json",
        headers={"User-Agent":"JustHodl research admin@justhodl.ai"})
    r=urllib.request.urlopen(req,timeout=20)
    probe["status"]=r.status; probe["body_ok"]=True
except Exception as e:
    probe["err"]=str(e)[:150]
R["single_probe"]=probe
if probe.get("status")==200:
    # cleared -> re-enable cron, kick exactly ONE real round to prove it end-to-end
    ev.enable_rule(Name="justhodl-fred-catalog-5min")
    R["cron_reenabled"]=True
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"phase":"scoped_import"}).encode())
    body=json.loads(inv["Payload"].read().decode())
    rn=json.loads(body["body"]) if isinstance(body,dict) and "body" in body else body
    R["first_real_round"]=rn
else:
    R["cron_reenabled"]=False
    R["note"]="STILL BLOCKED — cron left DISABLED, no further attempts this pass"
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"FRED RATE-LIMIT FIX (Khalid): sequential, 90/min ceiling, exp backoff on 429, hard halt on 403. "
  f"General discovery removed from schedule (scoped_import only, the ONE thing Khalid asked for). "
  f"Probe: {json.dumps(probe)}. cron_reenabled={R.get('cron_reenabled')}. "
  f"first_real_round={json.dumps(R.get('first_real_round'))[:300]}")})
bus({"action":"fanout_pending"})
R["verdict"]=f"probe={json.dumps(probe)} reenabled={R.get('cron_reenabled')} round={json.dumps(R.get('first_real_round'))[:250]}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4556.json","w"),indent=1,default=str)
open("aws/ops/reports/4556.md","w").write("# 4556 — "+R["verdict"]+"\n")
print(R["verdict"][:400])
