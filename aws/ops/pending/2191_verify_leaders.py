import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-accumulation-radar")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
ml=d.get("market_leaders") or []; lf=d.get("leaders_fading") or []
print(f"v{d['version']} market_leaders={len(ml)} leaders_fading={len(lf)}")
print("\nTOP MARKET LEADERS (relative strength + accumulation):")
for r in ml[:12]:
    print(f"  {r['ticker']:<6} lead {r['leadership_score']:<5} RS126 {r.get('rs_126d')} | 1m {r.get('ret_21d')}% 3m {r.get('ret_63d')}% 6m {r.get('ret_126d')}% | {r['phase']} cmf {r['cmf']} spark_pts {len(r.get('spark',[]))}")
print("\nLEADERS FADING (was strong, now rolling over):")
for r in lf[:6]:
    print(f"  {r['ticker']:<6} RS126 {r.get('rs_126d')} {r['phase']} vs50 {r.get('pct_vs_50dma')}% obv {r['obv_trend']}")
print("DONE 2191")
