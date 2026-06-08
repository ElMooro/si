import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-n2"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p,to=25):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={}
    out["reset"]=call("/brain-purge?uid="+UID+"&token="+T+"&reset=1")
    # wait 90s WITH the write-guard live — if a tab re-saves junk, guard rejects it
    time.sleep(90)
    g=call("/brain?uid="+UID+"&t=9")
    out["after_90s"]={"notes":len(g.get("notes",[])) if isinstance(g.get("notes"),list) else g}
    # test the guard: try to write a junk note + a real note
    out["junk_write"]=call("/brain?uid="+UID)  # GET, just to confirm reachable
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1431_n2.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
