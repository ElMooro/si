import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-ev"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    for p,key in [("/engines.html","Engine Directory"),("/engine.html?e=magic-formula","Engine Viewer")]:
        try:
            h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai"+p+"&t=9" if "?" in p else "https://justhodl.ai"+p+"?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
            out[p]={"status":200,"ok":key in h,"bytes":len(h)}
            if "engines.html" in p: out[p]["embeds_engines"]="ENGINES=" in h and h.count('"n":')>100
        except Exception as ex: out[p]=str(ex)[:50]
    # confirm a viewer-only engine's data actually exists to render
    for k in ["data/magic-formula.json","data/global-liquidity.json","data/dix.json"]:
        try:
            urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/"+k+"?t=9",headers={"User-Agent":"jh"}),timeout=12)
            out["data_"+k.split('/')[-1]]="exists"
        except urllib.error.HTTPError as ex: out["data_"+k.split('/')[-1]]="HTTP "+str(ex.code)
        except Exception as ex: out["data_"+k.split('/')[-1]]=str(ex)[:30]
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
open("aws/ops/reports/1486_ev.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
