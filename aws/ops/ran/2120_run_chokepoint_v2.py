import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-chokepoint")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName="justhodl-chokepoint",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:220],f"({time.time()-t:.0f}s)")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/chokepoint.json")["Body"].read())
print("scope:",d.get("scope"),"| stats:",d["stats"])
print("diag:",d.get("diagnostics"))
print("\n🛰️ NEWLY DISCOVERED chokepoints (broad scan found, curated missed) — the v2 payoff:")
for r in d.get("discovered_chokepoint_book",[])[:18]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {r['gm_level']}%GM ±{r['gm_stability']}  {r['cap_bucket']:<6} {(r.get('name') or '')[:26]:<26} {(r.get('industry') or '')[:26]}")
print("\n💰 cheap among discovered/all:")
for r in d.get("cheap_chokepoint_book",[])[:8]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {r.get('discount_to_fair_pct')}% below  disc={r.get('discovered')}  {(r.get('name') or '')[:26]}")
import urllib.request
def chk(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as x:return x.getcode()
        except Exception:time.sleep(8)
    return None
print("\npage:",chk("https://justhodl.ai/equity-chokepoint.html?t="+str(int(time.time()))))
print("DONE 2120")
