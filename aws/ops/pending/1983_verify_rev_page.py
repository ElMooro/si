import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# confirm engine output is clean WARMING baseline
o=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/estimate-revisions.json")["Body"].read())
print("engine output: status",o["status"],"n_tracked",o["n_tracked"],"n_with_history",o["n_with_history"],"n_state_keys",o.get("n_state_keys"))
CODE='''
import urllib.request,json
def h(e,c):
    o={}
    for lab,u in [("page","https://justhodl.ai/estimate-revisions.html"),("dir","https://justhodl.ai/directory.html")]:
        try:
            b=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"v/1"}),timeout=15).read().decode("utf-8","replace")
            o[lab]={"status":200,"len":len(b),"title":"Estimate Revisions" in b,"link":"/estimate-revisions.html" in b}
        except Exception as ex: o[lab]={"err":str(ex)}
    return o
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py");zi.external_attr=0o644<<16;z.writestr(zi,CODE)
FN="jh-tmp-vrev";ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.delete_function(FunctionName=FN)
except Exception: pass
time.sleep(2)
lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.h",Code={"ZipFile":buf.getvalue()},Timeout=40,MemorySize=256)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("State")=="Active": break
    time.sleep(2)
time.sleep(3)
print("live check:",json.dumps(json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read())))
try: lam.delete_function(FunctionName=FN)
except Exception: pass
print("DONE 1983")
