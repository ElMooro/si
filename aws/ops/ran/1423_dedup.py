"""Run server-side dedup across the 16.6k notes to strip duplicates + the debug-log
junk that polluted recent notes. Walk by offset until done. From AWS."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=350,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-dd"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=45); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    off=int(e.get("offset",0)); out={"start":off,"rounds":0}
    deadline=time.time()+300
    while time.time()<deadline:
        d=call("/brain?dedup=1&token="+T+"&uid="+UID+"&offset="+str(off))
        out["rounds"]+=1
        if d.get("err"): out["err"]=d["err"]; break
        off=d.get("scanned_to",off+200); out["kept"]=d.get("kept"); out["total"]=d.get("total_index"); out["scanned_to"]=off
        if d.get("done"): out["done"]=True; break
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={"invocations":[]}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=320,MemorySize=256)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    off=0
    for inv in range(4):
        r=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"offset":off}).encode())["Payload"].read())
        out["invocations"].append(r)
        if r.get("done"): out["DONE"]=True; break
        if r.get("err"): break
        off=r.get("scanned_to",off)
    lam.delete_function(FunctionName=FN)
except Exception as e: out["err"]=str(e)[:150]
open("aws/ops/reports/1423_dd.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
