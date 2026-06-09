import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-u"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    K="2f057499936072679d8843d7fce99989"; out={}
    for sid in ["WALCL","RRPONTSYD","WTREGEN"]:
        try:
            u=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={K}&file_type=json&sort_order=desc&limit=2"
            d=json.loads(urllib.request.urlopen(u,timeout=15).read().decode())
            o=d["observations"][0]
            out[sid]={"date":o["date"],"value":o["value"]}
        except Exception as ex: out[sid]=str(ex)[:50]
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
open("aws/ops/reports/1498_u.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
