"""1293 — redeploy vintage (no shim) + verify rows + final ask test + worker /ask."""
import json, os, time, zipfile, io, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
def zipdir(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
# redeploy vintage
try:
    lam.update_function_code(FunctionName="justhodl-vintage-fred",ZipFile=zipdir("aws/lambdas/justhodl-vintage-fred/source"))
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-vintage-fred")
        if c.get("LastUpdateStatus") in ("Successful",None): break
    r=lam.invoke(FunctionName="justhodl-vintage-fred",InvocationType="RequestResponse",Payload=b"{}")
    out["vintage_invoke"]=r.get("Payload").read().decode()[:150]
    time.sleep(2)
    idx=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/vintage/_index.json")["Body"].read())
    samples={}
    for sid in (idx.get("series") or [])[:5]:
        v=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/vintage/{sid}.json")["Body"].read())
        vs=v.get("vintages",[]); samples[sid]={"n":len(vs),"latest":vs[-1] if vs else None}
    out["vintage_samples"]=samples
except Exception as e: out["vintage_err"]=str(e)[:200]
# final ask test (Lambda direct)
try:
    r=lam.invoke(FunctionName="justhodl-ask",InvocationType="RequestResponse",Payload=json.dumps({"q":"which compounders are also cheap?"}).encode())
    body=json.loads(r.get("Payload").read().decode()); inner=json.loads(body.get("body","{}"))
    out["ask"]={"answer":(inner.get("answer") or "")[:160],"n_results":len(inner.get("results",[])),"first":(inner.get("results") or [None])[0]}
except Exception as e: out["ask_err"]=str(e)[:200]
# worker /ask reachable?
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/ask",data=json.dumps({"q":"top setups today"}).encode(),headers={"Content-Type":"application/json"},method="POST")
    wr=json.loads(urllib.request.urlopen(req,timeout=40).read().decode())
    out["worker_ask"]={"n_results":len(wr.get("results",[])),"has_answer":bool(wr.get("answer"))}
except Exception as e: out["worker_ask_err"]=str(e)[:150]
open("aws/ops/reports/1293_final.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
