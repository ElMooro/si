import json, os, time, zipfile, io, urllib.request
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
for n,src in [("justhodl-ask","aws/lambdas/justhodl-ask/source"),("justhodl-best-setups","aws/lambdas/justhodl-best-setups/source")]:
    lam.update_function_code(FunctionName=n,ZipFile=zd(src))
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=n)
        if c.get("LastUpdateStatus") in ("Successful",None): break
# best-setups run + check brain_aligned field present
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
out["best_setups_has_brain_field"]=any("brain_aligned" in s for s in bs.get("top_setups",[]))
out["n_brain_aligned"]=len(bs.get("brain_aligned",[]))
# ask: confirm it reads brain (call with a question)
try:
    r=lam.invoke(FunctionName="justhodl-ask",InvocationType="RequestResponse",Payload=json.dumps({"q":"what should I focus on given my philosophy?"}).encode())
    body=json.loads(r["Payload"].read())
    out["ask_status"]=body.get("statusCode")
    inner=json.loads(body.get("body","{}")) if body.get("body") else {}
    out["ask_answered"]=bool(inner.get("answer"))
except Exception as e: out["ask_err"]=str(e)[:120]
open("aws/ops/reports/1339_bw.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
