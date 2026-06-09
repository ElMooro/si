import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-jsd"
code=r'''
import json,boto3,urllib.request
def lambda_handler(e,c):
    s3=boto3.client("s3",region_name="us-east-1"); out={}
    # copy the already-stored js/ file to data/ (worker serves /data/ reliably)
    try:
        js=s3.get_object(Bucket="justhodl-dashboard-live",Key="js/lightweight-charts.js")["Body"].read()
        s3.put_object(Bucket="justhodl-dashboard-live",Key="data/lightweight-charts.js",Body=js,ContentType="application/javascript",CacheControl="public, max-age=604800")
        out["copied_bytes"]=len(js)
    except Exception as ex: out["err"]=str(ex)[:70]
    # verify both worker + justhodl.ai serve it under /data/
    for u in ["https://justhodl-data-proxy.raafouis.workers.dev/data/lightweight-charts.js","https://justhodl.ai/data/lightweight-charts.js"]:
        try:
            r=urllib.request.urlopen(urllib.request.Request(u+"?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=20); b=r.read()
            out[u]={"status":r.status,"bytes":len(b)}
        except urllib.error.HTTPError as ex: out[u]={"status":ex.code}
        except Exception as ex: out[u]=str(ex)[:40]
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
open("aws/ops/reports/1511_jsd.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
