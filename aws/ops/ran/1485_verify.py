import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-vf"
code=r'''
import json,urllib.request
def lambda_handler(e,c):
    out={}
    for p,key in [("/bonds.html","real ICE MOVE"),("/risk.html","VALUE AT RISK")]:
        try:
            h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai"+p+"?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
            out[p]={"status":200,"wired":key in h}
        except Exception as ex: out[p]=str(ex)[:50]
    # confirm the data files have real numbers
    for k in ["data/move-index.json","data/basket-var.json"]:
        try:
            d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/"+k+"?t=9",headers={"User-Agent":"jh"}),timeout=15).read().decode())
            if "move" in k: out["MOVE"]={"level":d.get("level"),"regime":d.get("regime"),"pctile":d.get("percentile")}
            else: out["VaR"]={"var95":d.get("var_1d_95_pct"),"cvar95":d.get("cvar_1d_95_pct"),"beta":d.get("basket_beta_spy"),"n":d.get("n_names")}
        except Exception as ex: out[k]=str(ex)[:50]
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
open("aws/ops/reports/1485_v.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
