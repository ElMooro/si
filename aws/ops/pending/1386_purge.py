import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=600,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
FN="tmp-brain-purge"
code=r'''
import json,urllib.request,time
BASE="https://justhodl-data-proxy.raafouis.workers.dev"
TOK="jhpurge_9f48_2026"
def purge(uid):
    total_del=0; rounds=0
    while rounds<400:
        rounds+=1
        try:
            r=urllib.request.urlopen(urllib.request.Request(BASE+"/brain-purge?uid="+uid+"&token="+TOK+"&max=600",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=60)
            d=json.loads(r.read().decode())
        except Exception as e: return {"uid":uid,"err":str(e)[:80],"deleted_so_far":total_del,"rounds":rounds}
        total_del+=d.get("deleted",0)
        if not d.get("more"): return {"uid":uid,"deleted":total_del,"remaining":d.get("remaining_in_index"),"rounds":rounds}
    return {"uid":uid,"deleted":total_del,"rounds":rounds,"capped":True}
def lambda_handler(e,c):
    # purge the giant guest junk piles entirely; keep account but clean its junk
    out={}
    for uid in ["dev-67acfebe-d183-4792-af35-06ef074e2431","dev-b9c1855f-89ac-4cb0-8637-331d19f1ca64","9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"]:
        out[uid]=purge(uid)
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=600,MemorySize=512)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    out=json.loads(r["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:200]}
open("aws/ops/reports/1386_p.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
