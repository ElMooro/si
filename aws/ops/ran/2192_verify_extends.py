import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=500,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(25):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
# 1) master-ranker cycle gate
ready("justhodl-master-ranker")
try: lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
except Exception as e: print("mr invoke:",str(e)[:60])
mr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
tt=mr.get("top_tickers") or []
tagged=[t for t in tt if t.get("cycle_phase")]
warned=[t for t in tt if t.get("cycle_warning")]
print(f"master-ranker: {len(tt)} top_tickers, {len(tagged)} cycle-tagged, {len(warned)} warned")
for t in (warned or tagged)[:5]:
    print(f"  {t['ticker']:<6} score {t['score']} phase {t.get('cycle_phase')} warn {t.get('cycle_warning')}")
# 2) market-leaders page live
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/market-leaders.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read().decode("utf-8","ignore")
    print("market-leaders.html ->",r.getcode(),"renders:",("Market Leaders" in b and "Leaders fading" in b))
except Exception as e: print("market-leaders.html ->",str(e)[:50])
print("DONE 2192")
