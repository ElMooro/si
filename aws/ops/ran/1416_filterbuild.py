import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=600,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-fbuild"
code=r'''
import json,urllib.request
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(p,to=50):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:70]}
def lambda_handler(e,c):
    off=0; out={"rounds":0}
    for i in range(200):    # 26k/150 ≈ 175 rounds
        d=call("/brain?build=1&token="+T+"&uid="+UID+"&offset="+str(off))
        out["rounds"]+=1
        if d.get("err"): out["err"]=d["err"]; out["last_off"]=off; break
        off=d.get("scanned_to",off+150); out["kept"]=d.get("kept"); out["total_index"]=d.get("total_index")
        if d.get("done"): out["done"]=True; break
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=600,MemorySize=256)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1416_fb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
