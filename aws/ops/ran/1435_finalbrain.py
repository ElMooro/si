import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-fb"
out={}
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(p,to=25):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={}
    t0=time.time(); g=call("/brain?uid="+NK+"&t=9")
    out["new_key_load"]={"notes":len(g.get("notes",[])) if isinstance(g.get("notes"),list) else g,"secs":round(time.time()-t0,2)}
    # write ONE real test note to confirm the clean loop works
    note={"id":"seed-"+str(int(time.time())),"cat":"philosophy","text":"When liquidity is draining and the dollar is strong, risk assets struggle — favor cash, gold, and quality. This is my first clean note.","created":int(time.time()*1000),"pinned":True}
    out["write"]=call("/brain?uid="+NK,)  # placeholder; do real PUT below
    return out
'''
# we need PUT, so do it directly with urllib in this ops process via a temp lambda that PUTs
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(method,p,body=None):
    try:
        req=urllib.request.Request(B+p,data=(json.dumps(body).encode() if body else None),headers={"User-Agent":"jh","Origin":"https://justhodl.ai","Content-Type":"text/plain"},method=method)
        r=urllib.request.urlopen(req,timeout=25); return json.loads(r.read().decode())
    except urllib.error.HTTPError as ex: return {"status":ex.code,"body":ex.read().decode()[:80]}
    except Exception as ex: return {"err":str(ex)[:60]}
def lambda_handler(e,c):
    out={}
    g=call("GET","/brain?uid="+NK+"&t=1"); out["before"]=len(g.get("notes",[])) if isinstance(g.get("notes"),list) else g
    note={"id":"seed1","cat":"philosophy","text":"When liquidity drains and the dollar is strong, risk assets struggle. Favor cash, gold, quality. My first clean macro note.","created":int(time.time()*1000),"pinned":True}
    out["write"]=call("PUT","/brain?uid="+NK,{"note":note})
    time.sleep(1)
    g2=call("GET","/brain?uid="+NK+"&t=2"); out["after"]={"notes":len(g2.get("notes",[])) if isinstance(g2.get("notes"),list) else g2,"has_seed":any(n.get("id")=="seed1" for n in g2.get("notes",[])) if isinstance(g2.get("notes"),list) else False}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["test"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["err"]=str(e)[:120]
open("aws/ops/reports/1435_fb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
