import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-cnt"
# count via index length only (small payload) — use brain-purge reset=0? No. Use a HEAD-like: call dedup offset=999999 returns total_index without scanning
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p,to=25):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={}
    # reset first
    out["reset"]=call("/brain-purge?uid="+UID+"&token="+T+"&reset=1")
    time.sleep(3)
    # count via dedup at huge offset (scans nothing, returns total_index) — but dedup offset>len just returns done. Use build offset=999999
    out["t0_count"]=call("/brain-purge?uid="+UID+"&token="+T+"&max=1&offset=0")  # returns total_now
    time.sleep(60)
    out["t60_count"]=call("/brain-purge?uid="+UID+"&token="+T+"&max=1&offset=0")
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=90,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1432_cnt.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
