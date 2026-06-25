import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=500,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# master-ranker: invoke + check cycle tags
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-master-ranker")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
try:
    lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")
except Exception as e: print("mr invoke:",str(e)[:60])
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
tt=m.get("top_tickers") or []
tagged=[t for t in tt if t.get("cycle_phase")]
warned=[t for t in tt if t.get("cycle_warning")]
print(f"master-ranker: {len(tt)} top_tickers, {len(tagged)} cycle-tagged, {len(warned)} warned")
for t in (tagged or tt)[:6]:
    print(f"  {t['ticker']:<6} score {t.get('score')} phase {t.get('cycle_phase')} warn {t.get('cycle_warning')}")
# morning-intelligence: read existing output (don't invoke - it makes LLM calls)
for k in ["data/morning-intelligence.json","data/morning-brief.json","data/intelligence.json","data/morning-intelligence-facts.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        cb=json.dumps(d)
        has=("hot_money" in cb or "cross_border" in cb or "cycle_bottoms" in cb or "cycle_tops" in cb)
        print(f"  MI {k}: exists, age-keys={list(d.keys())[:5]}, has cross-border/cycle facts={has}")
        break
    except Exception: pass
print("DONE 2191")
