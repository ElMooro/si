import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")["Payload"].read().decode()[:240],f"({time.time()-t:.0f}s)")
time.sleep(2)
raw=s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read()
d=json.loads(raw)
print("v",d.get("version"),"| JSON size %.1f KB"%(len(raw)/1024))
print("transitions:",{"new_ignitions":[x['ticker'] for x in d.get('transitions',{}).get('new_ignitions',[])],"new_coiled":d.get('transitions',{}).get('new_coiled'),"tg":d.get('transitions',{}).get('telegram_pending')})
b=d.get("about_to_boom",[])
print(f"\nboom {len(b)} — chart+narrative check (first 5):")
for r in b[:5]:
    ch=r.get("chart") or {}
    nh=len(ch.get("closes",[])); nadv=len(ch.get("adverse",[]))
    held=sum(1 for a in ch.get("adverse",[]) if a["abn"]>0); fell=nadv-held
    th=r.get("top_holds",[])
    print(f"  {r['ticker']:<6} {r['stage']:<9} chart:{nh}closes/{nadv}adv (held {held}/fell {fell}) | top_holds:{len(th)} e.g. {th[0] if th else '—'}")
# verify all_resilient has NO chart (size control) and boom HAS chart
ar=d.get("all_resilient",[])
print(f"\nsize-control: all_resilient has _chart/chart? {any('chart' in r or '_chart' in r for r in ar)} (should be False)")
print(f"boom has chart? {all(r.get('chart') for r in b)} (should be True)")
# page
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r: return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
c1,b1=get("https://justhodl.ai/resilience.html?t="+str(int(time.time())))
print("\nresilience.html:",c1,"| sparkline fn:", 'function sparkline' in b1, "| trackRecord:", 'trackRecord' in b1, "| star/localStorage:", 'jh_resil_stars' in b1, "| transitions:", 'Newly ignited' in b1)
print("DONE 2102")
