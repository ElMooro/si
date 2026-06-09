import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-lwc2"
code=r'''
import json,urllib.request,boto3
def lambda_handler(e,c):
    out={}
    urls=[
      "https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js",
      "https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js",
      "https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js",
    ]
    js=None
    for u in urls:
        try:
            js=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=30).read()
            out["source"]=u; out["bytes"]=len(js); break
        except Exception as ex: out.setdefault("tried",[]).append(u+" → "+str(ex)[:30])
    if js and len(js)>50000:
        s3=boto3.client("s3",region_name="us-east-1")
        s3.put_object(Bucket="justhodl-dashboard-live",Key="js/lightweight-charts.js",Body=js,ContentType="application/javascript",CacheControl="public, max-age=604800")
        out["written"]=True
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
open("aws/ops/reports/1509_lwc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
