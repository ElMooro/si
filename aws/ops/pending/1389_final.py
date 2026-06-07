import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-final"
code=r'''
import json,urllib.request,time
BASE="https://api.justhodl.ai"; UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(method,path,body=None):
    try:
        req=urllib.request.Request(BASE+path,data=(json.dumps(body).encode() if body else None),headers={"User-Agent":"jh","Origin":"https://justhodl.ai","Content-Type":"text/plain"},method=method)
        t0=time.time();r=urllib.request.urlopen(req,timeout=20);return {"status":r.status,"secs":round(time.time()-t0,2),"body":json.loads(r.read().decode())}
    except urllib.error.HTTPError as e: return {"status":e.code,"body":e.read().decode()[:80]}
    except Exception as e: return {"err":str(e)[:80]}
def lambda_handler(e,c):
    out={}
    # 1) GET clean (should be instant)
    g=call("GET","/brain?uid="+UID+"&t=1"); out["get_clean"]={"status":g.get("status"),"notes":len(g.get("body",{}).get("notes",[])) if isinstance(g.get("body"),dict) else "?","secs":g.get("secs")}
    # 2) save a real note
    out["save"]=call("PUT","/brain?uid="+UID,{"note":{"id":"welcome1","cat":"philosophy","text":"My brain is clean and working. Notes persist now.","created":int(time.time()*1000),"pinned":True}})
    time.sleep(1)
    # 3) reload — note present?
    g2=call("GET","/brain?uid="+UID+"&t=2"); nb=g2.get("body",{}).get("notes",[]) if isinstance(g2.get("body"),dict) else []
    out["reload"]={"status":g2.get("status"),"notes":len(nb),"has_welcome":any(n.get("id")=="welcome1" for n in nb),"secs":g2.get("secs")}
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
open("aws/ops/reports/1389_f.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
