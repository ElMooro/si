import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-iso"
code=r'''
import json,urllib.request
B="https://justhodl-data-proxy.raafouis.workers.dev"
def call(p):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=20); return {"status":r.status,"body":r.read().decode()[:150]}
    except urllib.error.HTTPError as ex: return {"status":ex.code,"body":ex.read().decode()[:150]}
    except Exception as ex: return {"err":str(ex)[:80]}
def lambda_handler(e,c):
    return {
      "health": call("/health"),
      "empty_uid_get": call("/brain?uid=dev-emptytest123456789&t=1"),   # clean uid, 0 shards
      "debug": call("/brain-debug"),
    }
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
open("aws/ops/reports/1414_iso.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
