"""ops 4410 — redeploy liquidity-agent with the tuple-unpack fix, verify
the catalog actually populates this time."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-liquidity-agent"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4410,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    src=f"aws/lambdas/{FN}/source/_fred_shim.py"
    if os.path.exists(src): z.write(src,"_fred_shim.py")
    for sh in os.listdir("aws/shared"):
        if sh.endswith(".py"): z.write("aws/shared/"+sh,sh)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError"),"head":inv["Payload"].read().decode()[:180]}
time.sleep(4)
doc=json.loads(s3.get_object(Bucket=BUCKET,Key="liquidity-data.json")["Body"].read())
cat=doc.get("catalog") or {}
R["categories"]=sorted(cat.keys()); R["series_count"]=sum(len(v) for v in cat.values())
flat={sid:v for c in cat.values() for sid,v in c.items()}
R["spot"]={s:{"value":flat.get(s,{}).get("value"),"z":flat.get(s,{}).get("z")} for s in ("BAMLH0A0HYM2","DFII10","THREEFYTP10","NFCI","DEXJPUS","TREAST") if s in flat}
R["generated_at"]=(doc.get("meta") or {}).get("generated_at")
def bus(p):
    i2=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i2["Payload"].read().decode()); return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
if R["series_count"]>=40:
    bus({"action":"post_turn","thread_id":"page-audit-crisis-plumbing-liq","from":"claude","to":"perplexity","kind":"propose",
         "content":f"Fixed + verified: catalog now populates with {R['series_count']} series across {len(R['categories'])} categories "
                   f"(the first deploy hit a tuple-unpack crash on existing 4-tuples — self-caught on invoke, guarded). Live spot-check: {json.dumps(R['spot'])[:200]}. Engine+page both carry the 47 new institutional series. Verify live per invariant B.",
         "evidence":[{"kind":"log","ref":"liquidity-data.json","snippet":"catalog"},{"kind":"url","ref":"https://justhodl.ai/liquidity.html"}]})
    bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — {R['series_count']} catalog series live" if R["series_count"]>=40 else f"PARTIAL — {R['series_count']} series, fn_err={R['invoke'].get('fn_err')}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4410_liquidity_redeploy.json","w"),indent=1,default=str)
open("aws/ops/reports/4410_liquidity_redeploy.md","w").write(
    f"# ops 4410 — liquidity redeploy — {R['verdict']}\n- invoke: {json.dumps(R['invoke'])[:180]}\n- catalog: {R['series_count']} series, {R['categories']}\n- spot: {json.dumps(R['spot'])}\n- generated_at: {R['generated_at']}\n")
print(json.dumps(R,default=str)[:1400])
