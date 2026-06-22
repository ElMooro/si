import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
print("counts:",d.get("counts"),"| universe",d.get("universe_size"))
cast=[r for r in d.get("about_to_boom",[]) if r["ticker"]=="CAST"]
print("CAST still in boom list?:", bool(cast))
print("\n🚀 ABOUT TO BOOM (clean):")
for r in d.get("about_to_boom",[])[:12]:
    print(f"  {r['ticker']:<6} {r['stage']:<9} res {r['resilience']} | +{r['mean_abnormal_on_adverse_pct']}%/adv ({r['adverse_hit_rate_pct']}% held, n{r['n_adverse_days']}) | asym {r['beta_asymmetry']} | 20d {r['ret_20d_pct']}%")
print("\ntop_picks:",[p["ticker"] for p in d.get("top_picks",[])])
# verify page live
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r: return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
ts=str(int(time.time()))
c1,b1=get(f"https://justhodl.ai/resilience.html?t={ts}")
print("\nresilience.html:",c1,"| reads feed:",'data/resilience.json' in b1)
c2,b2=get(f"https://justhodl.ai/directory.html?t={ts}")
print("directory lists resilience:", '/resilience.html' in b2)
print("DONE 2093")
