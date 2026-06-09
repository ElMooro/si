import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-inv"
code=r'''
import json,urllib.request,time
B="https://justhodl-data-proxy.raafouis.workers.dev"; NK="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(p,to=40):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=to); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={}
    g=call("/brain?uid="+NK+"&t=9")
    notes=g.get("notes",[]) if isinstance(g.get("notes"),list) else []
    out["total"]=len(notes)
    # category breakdown + dup check
    bycat={}; texts={}
    for n in notes:
        c2=n.get("cat","?"); bycat[c2]=bycat.get(c2,0)+1
        t=(n.get("text") or "")[:60].lower().strip(); texts[t]=texts.get(t,0)+1
    out["by_cat"]=bycat
    dups={t:cnt for t,cnt in texts.items() if cnt>1}
    out["n_unique"]=len(texts); out["n_dup_groups"]=len(dups)
    out["top_dups"]=dict(sorted(dups.items(),key=lambda x:-x[1])[:5])
    out["samples"]=[(n.get("text") or "")[:50] for n in notes[:8]]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=70,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1445_inv.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
