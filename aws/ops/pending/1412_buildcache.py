"""Build a CLEAN bcache for the account: read shards in chunks via the purge
debug, strip garble, write the cache directly to KV. We do it via a temp Lambda
that calls a new /brain-rebuild route. But simplest: call /brain-debug to get the
index, then have the worker rebuild+cache via a GET with a long timeout, chunked.
Actually: trigger purge with garble=1 fully (walk by offset), THEN one GET builds
the (now smaller) cache. Do the garble walk in MANY short calls to avoid timeout."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-bc"
code=r'''
import json,urllib.request
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p,to=25):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={"deleted":0}; off=0
    # walk whole index stripping garble, small windows to stay fast
    for i in range(70):
        d=call("/brain-purge?uid="+UID+"&token="+T+"&garble=1&max=120&offset="+str(off),to=25)
        if d.get("err"): out["err_at_round"]=i; out["err"]=d["err"]; break
        out["deleted"]+=d.get("deleted",0); off=d.get("next_offset",off+120); out["total_now"]=d.get("total_now")
        if not d.get("more"): out["walk_done"]=True; break
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=300,MemorySize=256)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1412_bc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
