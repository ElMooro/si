import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-tgb"
code=r'''
import json,urllib.request,ssl
def lambda_handler(e,c):
    ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
    H={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36","Accept":"text/csv;q=0.9, */*;q=0.5"}
    out={}
    # Get ALL TGB data (no key) to learn the actual series keys
    try:
        u="https://data-api.ecb.europa.eu/service/data/TGB?format=csvdata&lastNObservations=1"
        r=urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=30,context=ctx)
        lines=r.read().decode("utf-8","replace").strip().split("\n")
        out["tgb_status"]=r.status; out["tgb_n_series"]=len(lines)-1
        out["tgb_header"]=lines[0] if lines else None
        # show the KEY column (first col) for DE/IT/ES rows
        keys=[l.split(",")[0] for l in lines[1:]]
        out["sample_keys"]=keys[:20]
        # find Germany / Italy
        out["DE_keys"]=[k for k in keys if ".DE." in k][:5]
        out["IT_keys"]=[k for k in keys if ".IT." in k][:5]
    except urllib.error.HTTPError as ex: out["tgb_err"]={"status":ex.code,"body":ex.read().decode("utf-8","replace")[:120]}
    except Exception as ex: out["tgb_err"]=str(ex)[:60]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1515_tgb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
