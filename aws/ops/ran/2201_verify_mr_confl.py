import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-master-ranker")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
tt=d.get("top_tickers") or []
conf_sys=("options_confluence","flow_confluence","equity_confluence","earnings_confluence")
n_with=[t for t in tt if any(s in (t.get("systems") or []) for s in conf_sys)]
print(f"top_tickers={len(tt)} | with a confluence system: {len(n_with)}")
for t in n_with[:6]:
    cs=[s for s in (t.get("systems") or []) if s in conf_sys]
    print(f"  {t['ticker']:<6} score {t.get('score')} n_systems {t.get('n_systems')} confluences={cs}")
print("DONE 2201")
