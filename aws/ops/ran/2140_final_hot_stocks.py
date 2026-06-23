import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=330,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); FN="justhodl-hot-stocks-digest"
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-stocks-digest.json")["Body"].read())
print("llm_narrative:",d.get("llm_narrative"),"(if False, deterministic fallback shown)")
print("MARKET READ:",(d.get("market_read") or "")[:190])
print("\nMORNING BRIEF (institutional):")
for s in d.get("hot_stocks",[])[:8]:
    a=s.get("analyst") or {}; rb=" [rule]" if a.get("rule_based") else ""
    print(f"  {s['ticker']:<6} heat={round(s['score'])} ven={s.get('venue_count')} | NET: {a.get('net','—')}{rb}")
    print(f"        why: {a.get('why_hot','')[:84]}")
    print(f"        + {a.get('bull','')[:84]}")
    print(f"        - {a.get('bear','')[:84]}")
print("\nwarnings:",[w['ticker'] for w in d.get('warnings',[])[:8]])
print("emailed: reports@justhodl.ai | page: https://justhodl.ai/hot-stocks.html")
print("DONE 2140")
