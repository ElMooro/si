import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-prebuild"
# trigger the GET once from AWS — it rebuilds + caches bcache. Long timeout OK server-side.
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def lambda_handler(e,c):
    out={}
    # first GET = rebuild+cache (may be slow). give it 60s.
    try:
        t0=time.time();r=urllib.request.urlopen(urllib.request.Request(B+"/brain?uid="+UID+"&t=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=60)
        d=json.loads(r.read().decode());out["rebuild"]={"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,1)}
    except Exception as ex: out["rebuild_err"]=str(ex)[:80]
    # second GET = should be instant from cache
    try:
        t0=time.time();r=urllib.request.urlopen(urllib.request.Request(B+"/brain?uid="+UID+"&t=2",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=30)
        d=json.loads(r.read().decode());out["cached"]={"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,2)}
    except Exception as ex: out["cached_err"]=str(ex)[:80]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=128)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1412_pb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
