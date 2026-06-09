"""Self-host Lightweight Charts in S3 so the CDN block stops killing charts."""
import json, os, time, zipfile, io, urllib.request, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-lwc"
# Lambda fetches the lib from cdnjs (server-side, not browser-blocked) and writes to S3
code=r'''
import json,urllib.request,boto3
def lambda_handler(e,c):
    out={}
    url="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.3/lightweight-charts.standalone.production.js"
    try:
        js=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"}),timeout=30).read()
        s3=boto3.client("s3",region_name="us-east-1")
        s3.put_object(Bucket="justhodl-dashboard-live",Key="js/lightweight-charts.js",Body=js,ContentType="application/javascript",CacheControl="public, max-age=604800")
        out["written_bytes"]=len(js)
    except Exception as ex: out["err"]=str(ex)[:80]
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
open("aws/ops/reports/1508_lwc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
