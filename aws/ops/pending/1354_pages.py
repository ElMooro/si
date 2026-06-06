import json, time, urllib.request
import boto3
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={"pages":{},"my_brief":None}
# invoke my-brief once to create its file
try:
    lam.invoke(FunctionName="justhodl-my-brief",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/my-brief.json")["Body"].read())
    out["my_brief"]={"exists":True,"brief":bool(d.get("brief")),"note":d.get("note")}
except Exception as e: out["my_brief"]="ERR:"+str(e)[:60]
# pages serve?
PAGES=["/brain.html","/journal.html","/cockpit.html","/crypto-risk.html","/funding-plumbing.html"]
def g(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p+"?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=15).getcode()
    except urllib.error.HTTPError as e: return e.code
    except Exception as e: return str(e)[:40]
for p in PAGES: out["pages"][p]=g(p)
open("aws/ops/reports/1354_pg.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
