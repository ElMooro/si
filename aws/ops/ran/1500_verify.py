import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-edr"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    try:
        h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/eu-dump-radar.html?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
        out["page"]={"status":200,"ok":"EU Dump" in h and "ecb-derived.json" in h and "CISS Acceleration" in h}
    except Exception as ex: out["page_err"]=str(ex)[:50]
    try:
        d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/ecb-derived.json?t=9",headers={"User-Agent":"jh"}),timeout=12).read().decode())
        out["live_signals"]={k:v.get("signal") for k,v in d.get("indicators",{}).items()}
        out["headline"]=d.get("headline")
    except Exception as ex: out["data_err"]=str(ex)[:50]
    return out
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
open("aws/ops/reports/1500_v.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
