import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-ch"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(method,p,body=None):
    try:
        req=urllib.request.Request(B+p,data=(json.dumps(body).encode() if body else None),headers={"User-Agent":"jh","Origin":"https://justhodl.ai","Content-Type":"text/plain"},method=method)
        r=urllib.request.urlopen(req,timeout=30); return json.loads(r.read().decode())
    except urllib.error.HTTPError as ex: return {"status":ex.code,"body":ex.read().decode()[:80]}
    except Exception as ex: return {"err":str(ex)[:60]}
def lambda_handler(e,c):
    out={}
    g=call("GET","/brain?uid="+NK+"&t=1"); out["before"]=len(g.get("notes",[])) if isinstance(g.get("notes"),list) else g
    # a 450-note real-ish batch (within cap)
    batch=[{"id":"chk%d"%i,"cat":"thesis","text":"Real macro note number %d: when the dollar strengthens and liquidity drains, favor quality and cash over leverage."%i,"created":int(time.time()*1000)+i} for i in range(450)]
    out["batch450"]=call("PUT","/brain?uid="+NK,{"notes_upsert":batch})
    time.sleep(1)
    # an over-cap batch (550) should be rejected 413
    big=[{"id":"big%d"%i,"cat":"x","text":"overcap test note number %d about markets and liquidity conditions today."%i,"created":int(time.time()*1000)+i} for i in range(550)]
    out["batch550_rejected"]=call("PUT","/brain?uid="+NK,{"notes_upsert":big})
    time.sleep(1)
    g2=call("GET","/brain?uid="+NK+"&t=2"); out["after"]=len(g2.get("notes",[])) if isinstance(g2.get("notes"),list) else g2
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
open("aws/ops/reports/1443_ch.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
