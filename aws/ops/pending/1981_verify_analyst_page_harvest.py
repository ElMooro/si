import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); dynamo=boto3.client("dynamodb","us-east-1")
# 1) invoke harvester so it ingests data/analyst-actions.json top_picks
try:
    r=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse")
    print("harvester invoke:",r.get("StatusCode"),r.get("FunctionError"),"|",r["Payload"].read()[:200])
except Exception as e: print("harvester err:",e)
time.sleep(4)
# 2) scan justhodl-signals for analyst-actions rows
found=[]; tok=None; scanned=0
for _ in range(40):
    kw={"TableName":"justhodl-signals","Limit":300}
    if tok: kw["ExclusiveStartKey"]=tok
    resp=dynamo.scan(**kw); 
    for it in resp.get("Items",[]):
        scanned+=1
        blob=json.dumps(it)
        if "analyst-actions" in blob:
            sym=it.get("symbol",{}).get("S") or it.get("ticker",{}).get("S") or it.get("sym",{}).get("S") or "?"
            eng=it.get("engine",{}).get("S") or it.get("source",{}).get("S") or "?"
            found.append((eng,sym))
    tok=resp.get("LastEvaluatedKey")
    if not tok: break
print(f"scanned {scanned} signal rows; eng:analyst-actions matches={len(found)}")
print("  sample:",found[:12])
# 3) verify live page via temp lambda fetch
CODE='''
import urllib.request,json
def h(e,c):
    o={}
    for lab,u in [("page","https://justhodl.ai/analyst-actions.html"),("dir","https://justhodl.ai/directory.html")]:
        try:
            b=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"v/1"}),timeout=15).read().decode("utf-8","replace")
            o[lab]={"status":200,"len":len(b),"has_title":"Analyst Actions" in b,"link":"/analyst-actions.html" in b}
        except Exception as ex: o[lab]={"err":str(ex)}
    return o
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py");zi.external_attr=0o644<<16;z.writestr(zi,CODE)
FN="jh-tmp-vap";ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
try: lam.delete_function(FunctionName=FN)
except Exception: pass
time.sleep(2)
lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.h",Code={"ZipFile":buf.getvalue()},Timeout=40,MemorySize=256)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("State")=="Active": break
    time.sleep(2)
time.sleep(3)
rr=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("live check:",json.dumps(json.loads(rr["Payload"].read())))
try: lam.delete_function(FunctionName=FN)
except Exception: pass
print("DONE 1981")
