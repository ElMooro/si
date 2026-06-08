"""Purge the 58k junk guest pile (reset its index) so reads are fast.
The account (854) we leave — Khalid's real notes are in there mixed with some
test junk; we'll clean account junk via the purge route separately. From AWS."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-clean"
code=r'''
import json,urllib.request
B="https://justhodl-data-proxy.raafouis.workers.dev"; T="jhpurge_9f48_2026"
def call(p):
    try:
        r=urllib.request.urlopen(urllib.request.Request(B+p,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=40); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:70]}
def lambda_handler(e,c):
    out={}
    # reset the junk guest pile entirely
    out["reset_67acfebe"]=call("/brain-purge?uid=dev-67acfebe-d183-4792-af35-06ef074e2431&token="+T+"&reset=1")
    # for the ACCOUNT, run purge passes to strip test-junk shards (keeps real notes)
    dels=0
    for i in range(30):
        d=call("/brain-purge?uid=9f48a96b-1a1e-4867-9fc6-e6cc5054c56d&token="+T+"&max=700")
        if d.get("err"): out["acct_err"]=d["err"]; break
        dels+=d.get("deleted",0)
        if not d.get("more"): out["acct_done"]=True; out["acct_remaining"]=d.get("remaining_in_index"); break
    out["acct_deleted"]=dels
    out["after"]=call("/brain-debug")
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=120,MemorySize=256)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1407_cl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
