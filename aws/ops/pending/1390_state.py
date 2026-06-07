import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-state"
code=r'''
import json,urllib.request,time
BASE="https://api.justhodl.ai"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def lambda_handler(e,c):
    out={}
    try:
        t0=time.time()
        r=urllib.request.urlopen(urllib.request.Request(BASE+"/brain?uid="+UID+"&t=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=20)
        d=json.loads(r.read().decode()); notes=d.get("notes",[])
        out["account_load"]={"status":r.status,"notes":len(notes),"secs":round(time.time()-t0,2),"samples":[(n.get("text") or "")[:50] for n in notes[:6]]}
    except urllib.error.HTTPError as ex: out["account_load"]={"status":ex.code,"body":ex.read().decode()[:80]}
    except Exception as ex: out["account_load"]={"err":str(ex)[:100]}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1390_s.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
