import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-reset"
code=r'''
import json,urllib.request
BASE="https://justhodl-data-proxy.raafouis.workers.dev"; TOK="jhpurge_9f48_2026"
def call(p):
    try:
        r=urllib.request.urlopen(urllib.request.Request(BASE+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=30)
        return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:80]}
def lambda_handler(e,c):
    out={}
    # reset ALL identities to empty (all are test junk — clean slate). Account too.
    for uid in ["dev-67acfebe-d183-4792-af35-06ef074e2431","dev-b9c1855f-89ac-4cb0-8637-331d19f1ca64","dev-3ddcc752-42be-472c-adb2-58fc66b577f9","dev-6cf35a6c-9314-4040-943d-3455728cbf51","9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"]:
        out[uid]=call("/brain-purge?uid="+uid+"&token="+TOK+"&reset=1")
    # verify
    out["after"]=call("/brain-debug")
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=90,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1388_reset.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
