import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); FN="justhodl-hot-stocks-digest"
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:220],f"({time.time()-t:.0f}s)")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-stocks-digest.json")["Body"].read())
print("narrative:",d.get("llm_narrative"),"| hot:",len(d.get("hot_stocks",[])),"| emailed via ses to reports@justhodl.ai")
print("\nMARKET READ:",(d.get("market_read") or "")[:200])
print("\nHOT STOCKS BRIEF:")
for s in d.get("hot_stocks",[])[:8]:
    a=s.get("analyst") or {}
    print(f"  {s['ticker']:<6} heat={round(s['score'])} venues={s.get('venue_count')} bull%={s.get('bull_pct')} | NET: {a.get('net','—')}")
    if a.get('why_hot'): print(f"        why: {a.get('why_hot','')[:90]}")
    if a.get('bull'): print(f"        +  {a.get('bull','')[:88]}")
    if a.get('bear'): print(f"        -  {a.get('bear','')[:88]}")
    gn=s.get('good_news',[]); bn=s.get('bad_news',[])
    if gn: print(f"        good: {(gn[0].get('title') or '')[:72]}")
    if bn: print(f"        bad:  {(bn[0].get('title') or '')[:72]}")
print("\nwarnings:",[w['ticker'] for w in d.get('warnings',[])[:8]])
import urllib.request
try:
    code=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/hot-stocks.html?t="+str(int(time.time())),headers={"User-Agent":"jh"}),timeout=15).getcode()
except Exception as e: code=str(e)[:40]
print("page hot-stocks.html:",code)
print("DONE 2138")
