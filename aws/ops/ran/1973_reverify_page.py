import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1")
CODE='''
import urllib.request, json
def h(event,ctx):
    out={}
    for label,url in [("page","https://justhodl.ai/flow-lookthrough.html"),
                      ("dir","https://justhodl.ai/directory.html"),
                      ("feed_s3","https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/flow-lookthrough.json")]:
        try:
            b=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"v/1"}),timeout=15).read().decode("utf-8","replace")
            o={"status":200,"len":len(b)}
            if label=="page": o["has_title"]="Flow Look-Through" in b; o["has_loader"]="flow-lookthrough.json" in b; o["has_them"]="Thematic Rotation Leaders" in b
            if label=="dir": o["dir_link"]="/flow-lookthrough.html" in b
            if label=="feed_s3": j=json.loads(b); o["n_names"]=j.get("n_names"); o["n_thematic"]=len(j.get("thematic_rotation_leaders",[])); o["top_inflow"]=(j.get("inflow_leaders") or [{}])[0].get("ticker")
            out[label]=o
        except Exception as e: out[label]={"err":f"{type(e).__name__}: {e}"}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,CODE)
FN="jh-tmp-verify2"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.delete_function(FunctionName=FN)
except Exception: pass
time.sleep(2)
lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.h",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=256)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("State")=="Active": break
    time.sleep(2)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print(json.dumps(json.loads(r["Payload"].read()),indent=2))
try: lam.delete_function(FunctionName=FN)
except Exception: pass
print("DONE 1973")
