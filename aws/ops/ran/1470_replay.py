import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-rp"
out={}
# deploy whats-changed (now snapshots best-setups too)
buf=io.BytesIO(); src="aws/lambdas/justhodl-whats-changed/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
try:
    lam.update_function_code(FunctionName="justhodl-whats-changed",ZipFile=buf.getvalue())
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-whats-changed")
        if c.get("LastUpdateStatus") in ("Successful",None): break
    # run it now to seed today's best-setups snapshot
    r=lam.invoke(FunctionName="justhodl-whats-changed",InvocationType="RequestResponse",Payload=b"{}"); out["snapshotter_run"]=r["Payload"].read().decode()[:90]
except Exception as e: out["snap_err"]=str(e)[:90]
# verify the replay page serves
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    try:
        h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/signal-replay.html?t=9",headers={"User-Agent":"jh"}),timeout=15).read().decode()
        return {"served":"Signal" in h and "snapshots" in h and "scrub" in h.lower(),"bytes":len(h)}
    except Exception as ex: return {"err":str(ex)[:60]}
'''
buf2=io.BytesIO()
with zipfile.ZipFile(buf2,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf2.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf2.getvalue()},Timeout=30,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["page"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["page_err"]=str(e)[:90]
open("aws/ops/reports/1470_rp.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
