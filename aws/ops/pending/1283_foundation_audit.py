"""1283 — audit what exists for the 5 builds before constructing."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=60); s3=boto3.client("s3",region_name="us-east-1",config=cfg); lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
out={}
def head(k):
    try: s3.head_object(Bucket="justhodl-dashboard-live",Key=k); return True
    except: return False
def getj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
# #1 vintage
vi=getj("data/vintage/_index.json")
out["vintage"]={"index_exists":"_e" not in vi,"n_series":vi.get("n_series"),"updated":vi.get("updated")}
# #2 portfolio — what do the existing portfolio lambdas output?
out["portfolio_files"]={k:head("data/"+k) for k in ["portfolio-analytics.json","portfolio-risk.json","portfolio-snapshot.json","signal-portfolio.json"]}
# #4 track record
out["track_record_exists"]=head("data/track-record.json")
out["signal_backtest_exists"]=head("data/signal-backtest.json")
# list portfolio lambda outputs by reading their S3_KEY
for ln in ["justhodl-portfolio-analytics","justhodl-portfolio-risk","justhodl-signal-portfolio","justhodl-ai-chat"]:
    try:
        c=lam.get_function_configuration(FunctionName=ln); out[ln+"_state"]=c.get("State")
    except Exception as e: out[ln+"_state"]=str(e)[:50]
open("aws/ops/reports/1283_audit.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
