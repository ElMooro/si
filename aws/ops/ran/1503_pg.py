import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-pg"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/eu-dump-radar.html?t="+str(__import__("time").time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
    return {"status":200,"has_pass_through":"Bank Pass-Through Premium" in h,"has_all_5":h.count("class=\"num\">#")>=5 or ("#14" in h and "#12" in h and "#8" in h)}
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=30,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1503_pg.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
