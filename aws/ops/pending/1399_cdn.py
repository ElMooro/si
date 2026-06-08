import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-cdn"
code=r'''
import json,urllib.request
def head(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
        body=r.read(600).decode("utf-8","replace")
        return {"status":r.status,"ctype":r.headers.get("Content-Type"),"starts_with_import":body.lstrip().startswith("import")or"\nimport " in body[:300],"head":body[:120]}
    except Exception as e: return {"err":str(e)[:80]}
def lambda_handler(e,c):
    return {
      "supabase@2": head("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"),
      "supabase@2_umd": head("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.js"),
    }
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=40,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1399_c.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
