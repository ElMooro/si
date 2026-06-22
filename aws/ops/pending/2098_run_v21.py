import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")["Payload"].read().decode()[:220],f"({time.time()-t:.0f}s)")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
print("v",d.get("version"),"counts:",d.get("counts"))
sh=d.get("shrugged_off_bad_news_recent",[])
print(f"\n🛡️ HELD THE LINE ON BAD NEWS (last 2wk) — {len(sh)} names:")
for s in sh:
    print(f"  {s['ticker']:<6} {s['type']:<13} {s['date']} | day {s['day_return_pct']:+}% | abnormal +{s['abnormal_pct']}% | {s['basis']}")
print("\n🚀 about-to-boom top 6:")
for r in d.get("about_to_boom",[])[:6]:
    print(f"  {r['ticker']:<6} {r['stage']:<9} res {r['resilience']} {r['abnormal_basis']} dom {r['dominant_adverse_type']}")
# page live?
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r: return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
c1,b1=get("https://justhodl.ai/resilience.html?t="+str(int(time.time())))
print("\nresilience.html:",c1,"| has shrugged section:", 'Held the line on bad news' in b1, "| reads feed:", 'data/resilience.json' in b1)
print("DONE 2098")
