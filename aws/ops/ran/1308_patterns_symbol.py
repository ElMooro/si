"""1308 — deploy pattern engine, verify expectancy + new patterns; check symbol-bar live."""
import json, os, time, zipfile, io, urllib.request
import boto3
from botocore.config import Config
cfg=Config(read_timeout=650,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
buf=io.BytesIO(); src="aws/lambdas/justhodl-chart-patterns/source"
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
    for r,_,fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"): continue
            fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
lam.update_function_code(FunctionName="justhodl-chart-patterns",ZipFile=buf.getvalue())
for _ in range(30):
    time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-chart-patterns")
    if c.get("LastUpdateStatus") in ("Successful",None): break
try:
    t0=time.time(); r=lam.invoke(FunctionName="justhodl-chart-patterns",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"elapsed":round(time.time()-t0,1),"body":r.get("Payload").read().decode()[:120]}
    time.sleep(2); d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/chart-patterns.json")["Body"].read())
    out["counts"]=d.get("counts")
    out["expectancy"]=d.get("expectancy")
    out["vb_sample"]=d.get("volume_breakouts",[])[:3]
except Exception as e: out["err"]=str(e)[:150]
# symbol-bar live?
try:
    req=urllib.request.Request("https://justhodl.ai/symbol-bar.js",headers={"User-Agent":"Mozilla/5.0"})
    j=urllib.request.urlopen(req,timeout=20).read().decode()
    out["symbol_bar"]={"served":"SymbolBar" in j,"bytes":len(j)}
except Exception as e: out["symbol_bar"]=str(e)[:60]
open("aws/ops/reports/1308_pat.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
