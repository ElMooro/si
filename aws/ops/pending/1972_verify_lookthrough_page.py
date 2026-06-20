"""1972 — verify flow-lookthrough.html is live + renders (Lambda fetches live URL)."""
import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1")
CODE='''
import urllib.request, json
def h(event,ctx):
    out={}
    for label,url in [("page","https://justhodl.ai/flow-lookthrough.html"),
                      ("dir","https://justhodl.ai/directory.html"),
                      ("feed","https://justhodl.ai/data/flow-lookthrough.json")]:
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"jh-verify/1.0"})
            b=urllib.request.urlopen(req,timeout=15).read().decode("utf-8","replace")
            out[label]={"status":200,"len":len(b),
                "has_title":"Flow Look-Through" in b,
                "has_loader":"flow-lookthrough.json" in b,
                "dir_link":"/flow-lookthrough.html" in b}
            if label=="feed":
                j=json.loads(b); out[label]["n_names"]=j.get("n_names"); out[label]["n_thematic"]=len(j.get("thematic_rotation_leaders",[])); out[label]["top_picks"]=len(j.get("top_picks",[]))
        except Exception as e:
            out[label]={"err":f"{type(e).__name__}: {e}"}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,CODE)
FN="jh-tmp-verify-page"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.delete_function(FunctionName=FN)
except Exception: pass
time.sleep(2)
lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.h",
    Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=256)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active": break
    time.sleep(2)
time.sleep(3)  # let Pages deploy settle
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print(json.dumps(json.loads(r["Payload"].read()),indent=2))
try: lam.delete_function(FunctionName=FN)
except Exception: pass
print("DONE 1972")
