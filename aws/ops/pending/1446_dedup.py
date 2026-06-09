import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=400,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-dd"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(p):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=45); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    off=0; out={"rounds":0}
    deadline=time.time()+300
    while time.time()<deadline:
        d=call("/brain?dedup=1&token="+T+"&uid="+NK+"&offset="+str(off))
        out["rounds"]+=1
        if d.get("err"): out["err"]=d["err"]; break
        off=d.get("scanned_to",off+200); out["kept"]=d.get("kept"); out["total"]=d.get("total_index")
        if d.get("done"): out["done"]=True; break
    # final count
    if out.get("done"):
        time.sleep(2); g=call("/brain?uid="+NK+"&t=9")
        out["final"]=len(g.get("notes",[])) if isinstance(g.get("notes"),list) else g
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=320,MemorySize=256)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1446_dd.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
