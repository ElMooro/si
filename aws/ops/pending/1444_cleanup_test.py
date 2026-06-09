import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-ct"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(method,p,body=None):
    try:
        req=urllib.request.Request(B+p,data=(json.dumps(body).encode() if body else None),headers={"User-Agent":"jh","Origin":"https://justhodl.ai","Content-Type":"text/plain"},method=method)
        r=urllib.request.urlopen(req,timeout=30); return json.loads(r.read().decode())
    except Exception as ex: return {"err":str(ex)[:60]}
def lambda_handler(e,c):
    out={}
    # delete the chk* and big* and seed* test notes
    g=call("GET","/brain?uid="+NK+"&t=1")
    notes=g.get("notes",[]) if isinstance(g.get("notes"),list) else []
    test_ids=[n["id"] for n in notes if n.get("id","").startswith(("chk","big","seed"))]
    out["test_notes_found"]=len(test_ids)
    for tid in test_ids[:1000]:
        call("PUT","/brain?uid="+NK,{"delete":tid})
    time.sleep(2)
    g2=call("GET","/brain?uid="+NK+"&t=2")
    out["after"]=len(g2.get("notes",[])) if isinstance(g2.get("notes"),list) else g2
    out["remaining_samples"]=[(n.get("text") or "")[:45] for n in g2.get("notes",[])[:6]] if isinstance(g2.get("notes"),list) else None
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=110,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1444_ct.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
