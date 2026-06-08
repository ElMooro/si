import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-getfix"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p,to=20):
    try:
        t0=time.time();r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return {"status":r.status,"secs":round(time.time()-t0,2),"body":r.read().decode()[:120]}
    except urllib.error.HTTPError as ex: return {"status":ex.code,"body":ex.read().decode()[:120]}
    except Exception as ex: return {"err":str(ex)[:80]}
def lambda_handler(e,c):
    out={}
    out["empty_uid"]=call("/brain?uid=dev-cleantest987654321&t=1")   # should be 200 fast now
    out["account_first"]=call("/brain?uid="+UID+"&t=1")              # 200 (empty-fast if no cache)
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1415_g.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
