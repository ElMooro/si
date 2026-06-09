"""Systematic health sweep from AWS: page HTTP status + data freshness + known
loose ends. Finds real bugs, no assumptions."""
import json, os, time, zipfile, io
import boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-sweep"
out={"pages":{},"stale_data":{},"empty_data":{}}
# 1) data freshness/emptiness for the pages we built + key engines
B="justhodl-dashboard-live"
DATA={
 "best-setups":"data/best-setups.json","opportunities":"data/opportunities.json",
 "master-ranker":"data/master-ranker.json","signal-backtest":"data/signal-backtest.json",
 "compound-signals":"data/compound-signals.json","backlog":"data/backlog.json",
 "deep-value":"data/deep-value-overlap.json","fundamentals":"data/fundamentals.json",
 "brain":"data/brain.json","funding-plumbing":"data/funding-plumbing.json",
 "bond-vol":"data/bond-vol.json","crypto-cycle-risk":"data/crypto-cycle-risk.json",
 "signal-board":"data/signal-board.json","short-interest":"data/short-interest.json",
 "13f-positions":"data/13f-positions.json","estimate-revisions":"data/estimate-revisions-latest.json",
}
now=datetime.now(timezone.utc)
for name,k in DATA.items():
    try:
        o=s3.get_object(Bucket=B,Key=k); d=json.loads(o["Body"].read())
        age=round((now-o["LastModified"]).total_seconds()/3600,1)
        if age>30: out["stale_data"][name]={"age_h":age}
        # emptiness check on common arrays
        if isinstance(d,dict):
            for a in ["top_setups","top_opportunities","top_tickers","by_ticker","companies","board","new_alerts","notes"]:
                if a in d:
                    v=d[a]; n=len(v) if isinstance(v,(list,dict)) else 0
                    if n==0: out["empty_data"].setdefault(name,[]).append(a)
    except Exception as e: out["stale_data"][name]={"MISSING":str(e)[:40]}
# 2) page HTTP status via a probe lambda
PAGES=["/","/master-board.html","/dossier.html?t=NVDA","/scorecard.html","/why-now.html","/signal-replay.html","/brain.html","/journal.html","/cockpit.html","/screener/","/opportunities.html","/signal-board.html","/chart-pro.html","/my-portfolio.html"]
code='''
import json,urllib.request
def lambda_handler(e,c):
    res={}
    for p in e["pages"]:
        try:
            r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai"+p,headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
            res[p]=r.getcode()
        except urllib.error.HTTPError as ex: res[p]=ex.code
        except Exception as ex: res[p]=str(ex)[:30]
    return res
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["pages"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"pages":PAGES}).encode())["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["page_err"]=str(e)[:90]
open("aws/ops/reports/1471_sweep.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
