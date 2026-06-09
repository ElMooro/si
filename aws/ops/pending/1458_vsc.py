import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-vsc"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    try:
        h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/scorecard.html?t=9",headers={"User-Agent":"jh"}),timeout=15).read().decode()
        out["served"]="Scorecard" in h and "signal-backtest.json" in h and "what actually happened" in h
    except Exception as ex: out["err"]=str(ex)[:60]
    b=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/master-board.html?t=9",headers={"User-Agent":"jh"}),timeout=15).read().decode()
    out["board_ok"]="Master" in b
    return out
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
open("aws/ops/reports/1458_vsc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
