"""1292 — raw ALFRED test (diagnose vintage 0-rows) + redeploy ask + final verify."""
import json, os, time, zipfile, io, urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
FRED="2f057499936072679d8843d7fce99989"
# raw ALFRED call exactly like the lambda does
end=date.today(); start=end-timedelta(days=400)
p={"series_id":"CPIAUCSL","api_key":FRED,"file_type":"json","observation_start":start.isoformat(),"observation_end":end.isoformat(),"realtime_start":start.isoformat(),"realtime_end":end.isoformat()}
url="https://api.stlouisfed.org/fred/series/observations?"+urllib.parse.urlencode(p)
try:
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl/1.0"})
    d=json.loads(urllib.request.urlopen(req,timeout=20).read().decode())
    obs=d.get("observations",[])
    out["raw_keys"]=list(d.keys())[:8]
    out["n_obs"]=len(obs)
    out["first_obs"]=obs[0] if obs else None
    out["err_in_resp"]=d.get("error_message")
except Exception as e: out["raw_err"]=str(e)[:200]
# redeploy ask (fence fix) + test via worker-style invoke
try:
    SRC="aws/lambdas/justhodl-ask/source"; buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(SRC):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,SRC))
    lam.update_function_code(FunctionName="justhodl-ask",ZipFile=buf.getvalue())
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName="justhodl-ask")
        if c.get("LastUpdateStatus") in ("Successful",None): break
    r=lam.invoke(FunctionName="justhodl-ask",InvocationType="RequestResponse",Payload=json.dumps({"q":"cheap stocks institutions are buying"}).encode())
    body=json.loads(r.get("Payload").read().decode()); inner=json.loads(body.get("body","{}"))
    out["ask_test"]={"answer":(inner.get("answer") or "")[:140],"n_results":len(inner.get("results",[])),"sample":inner.get("results",[])[:2]}
except Exception as e: out["ask_err"]=str(e)[:200]
open("aws/ops/reports/1292_alfred.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
