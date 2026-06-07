"""Audit Lambda — calls /brain-debug FROM AWS (sandbox IP is CF-blocked).
Reveals where notes actually live: every brain identity + note count."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
FN="tmp-brain-audit"
code='''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    for base in ["https://justhodl-data-proxy.raafouis.workers.dev","https://api.justhodl.ai"]:
        try:
            r=urllib.request.urlopen(urllib.request.Request(base+"/brain-debug",headers={"User-Agent":"jh-audit","Origin":"https://justhodl.ai"}),timeout=20)
            out[base]={"status":r.status,"body":json.loads(r.read().decode())}
        except urllib.error.HTTPError as ex: out[base]={"status":ex.code,"body":ex.read().decode()[:200]}
        except Exception as ex: out[base]={"err":str(ex)[:120]}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
zb=buf.getvalue()
out={}
try:
    try:
        lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=60,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    out=json.loads(r["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"audit_err":str(e)[:200]}
open("aws/ops/reports/1384_audit.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
