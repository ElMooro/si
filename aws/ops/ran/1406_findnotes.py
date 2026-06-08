"""Audit's step 2: scan S3 for ANY notes-like keys, AND list every brain
identity in KV with counts — find where notes actually are. From AWS."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={"s3_note_keys":[],"kv_identities":None}
# 1) S3 keys mentioning brain/note/journal/memory
try:
    paginator=s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
        for o in page.get("Contents",[]):
            k=o["Key"].lower()
            if any(w in k for w in ["brain","note","journal","memory"]):
                out["s3_note_keys"].append({"key":o["Key"],"size":o["Size"],"modified":str(o["LastModified"])})
except Exception as e: out["s3_err"]=str(e)[:100]
# 2) KV identities via /brain-debug (from AWS through a temp lambda)
FN="tmp-find"
code='''
import json,urllib.request
def lambda_handler(e,c):
    try:
        r=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain-debug",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=30)
        return json.loads(r.read().decode())
    except Exception as ex: return {"err":str(ex)[:120]}
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["kv_identities"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["kv_err"]=str(e)[:120]
open("aws/ops/reports/1406_fn.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
