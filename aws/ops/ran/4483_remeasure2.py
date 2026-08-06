"""ops 4482 — CLOSE THE LOOP: re-measure the mix that started the council
arc. SIGS gains the eight new agencies + the unanimous llm->transform
reclass; graph regenerates first (D4 nightly may predate tonight's
engines is fine — rollup reads live graph), rollup redeployed + run;
delta vs baseline (fleet 50.8 / unmapped 14.3) posted."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-provenance-rollup"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4483,"started":datetime.now(timezone.utc).isoformat()}
BASE={"fleet-feed":50.8,"unmapped":14.4,"fred":11.0,"polygon":9.5,"transform-agent":4.5,"nyfed":0.8}
def wait_idle(fn):
    for _ in range(24):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return
        time.sleep(6)
# refresh graph first so new engines are visible to the rollup
try:
    wait_idle("justhodl-lambda-inventory")
    lam.invoke(FunctionName="justhodl-lambda-inventory",InvocationType="RequestResponse",Payload=b"{}")
    R["graph"]="refreshed"
except Exception as e: R["graph_err"]=str(e)[:100]
time.sleep(5)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
wait_idle(FN)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
wait_idle(FN)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
_=inv["Payload"].read(); time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/audit/data-source-rollup.json")["Body"].read())
    mix={k:round(v*100,1) for k,v in (d.get("global") or {}).items()}
    counts=d.get("global_feed_counts") or {}
    R["mix"]=mix; R["counts_new"]={k:counts.get(k) for k in ("ofr","bea","fed-board","bis","gleif","eurostat","oecd","openfigi") if counts.get(k)}
    R["delta"]={k:{"was":BASE.get(k),"now":mix.get(k)} for k in BASE} | {"NEW-"+k:{"was":0,"now":mix.get(k)} for k in ("ofr","bea","fed-board","bis","gleif","eurostat","oecd") if mix.get(k)}
except Exception as e: R["feed_err"]=str(e)[:120]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("THE LOOP CLOSES — mix re-measured with tonight's providers in the signature table "
  "(+8 agencies; llm reclassed to transform-agent per unanimous council). "
  f"BASELINE fleet 50.8/unmapped 14.4 -> NOW: {json.dumps(R.get('mix'),default=str)[:350]} · "
  f"new-provider feed counts: {json.dumps(R.get('counts_new'),default=str)[:200]} · delta: "
  + json.dumps(R.get('delta'),default=str)[:250] +
  ". Note honestly: feed-level mix moves as PAGES adopt the new warm archives — engine-level "
  "acquisition is complete, page wiring is the next arc. Verify+seal the remeasure."),
 "evidence":[{"kind":"log","ref":"data/audit/data-source-rollup.json","snippet":"global_mix"}]})
bus({"action":"fanout_pending"})
ok=bool(R.get("mix"))
R["verdict"]=f"PASS — mix remeasured: {json.dumps(R.get('mix'),default=str)[:160]}" if ok else f"PARTIAL — {R.get('feed_err')}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4483_remeasure2.json","w"),indent=1,default=str)
open("aws/ops/reports/4483_remeasure2.md","w").write(f"# ops 4482 — remeasure — {R['verdict']}\n- delta: {json.dumps(R.get('delta'),default=str)[:400]}\n- new: {json.dumps(R.get('counts_new'),default=str)[:250]}\n")
print(R["verdict"])
