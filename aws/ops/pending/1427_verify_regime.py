import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-vr"
out={}
# 1) verify fast clean load
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def lambda_handler(e,c):
    t0=time.time()
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+"/brain?uid="+UID+"&t=9",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=30)
        d=json.loads(r.read().decode())
        return {"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,2),"samples":[(n.get("text") or "")[:55] for n in d.get("notes",[])[:8]]}
    except Exception as ex: return {"err":str(ex)[:80]}
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["load"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["load_err"]=str(e)[:100]
# 2) re-run brain-sync now that notes are clean → regime read
try:
    r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
    out["sync_run"]=r["Payload"].read().decode()[:90]
except Exception as e: out["sync_err"]=str(e)[:90]
time.sleep(6)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    out["n_notes_seen"]=b.get("n_notes")
    out["regime_read"]=b.get("regime_read")
    d=b.get("directive"); out["profile"]=(d.get("investor_profile") if d else None)
except Exception as e: out["mirror_err"]=str(e)[:90]
open("aws/ops/reports/1427_vr.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
