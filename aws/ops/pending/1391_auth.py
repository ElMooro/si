import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-auth"
code=r'''
import json,urllib.request
def g(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=15)
        return r.read().decode()
    except Exception as e: return "ERR:"+str(e)[:60]
def lambda_handler(e,c):
    out={}
    ac=g("https://justhodl.ai/auth-config.js?v=6")
    out["enabled_true"]="enabled: true" in ac or "enabled:true" in ac
    out["has_supa_url"]="bdmjenqcyvzouusfcgow" in ac
    out["has_anon_key"]="sb_publishable" in ac or "eyJ" in ac
    aj=g("https://justhodl.ai/auth.js?v=6")
    out["redirectTo_line"]=[l.strip() for l in aj.split("\n") if "redirectTo" in l][:3]
    out["css_matches"]=".jh-auth-card{" in aj
    # Supabase auth settings — what redirect URLs are allowed? (public settings endpoint)
    s=g("https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/settings")
    out["supa_settings"]=s[:300]
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1391_a.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
