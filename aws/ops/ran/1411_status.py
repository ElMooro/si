import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-st"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def lambda_handler(e,c):
    out={}
    # current count via debug (cheap)
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+"/brain-debug",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=30)
        ids=json.loads(r.read().decode()).get("identities",[])
        out["counts"]={i["uid"][:14]:i["notes"] for i in ids}
    except Exception as ex: out["debug_err"]=str(ex)[:60]
    # try a real load + time it
    try:
        t0=time.time(); r=urllib.request.urlopen(urllib.request.Request(B+"/brain?uid="+UID+"&t=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=40)
        d=json.loads(r.read().decode()); out["load"]={"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,2)}
    except Exception as ex: out["load_err"]=str(ex)[:60]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=80,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1411_st.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
