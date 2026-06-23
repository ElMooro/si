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
print("stats:",d["stats"])
print("diag:",d.get("diagnostics"))
print("\n✅ CONFIRMED true chokepoints (discovered + LLM-verified industry can't route around):")
cb=d.get("confirmed_chokepoint_book",[])
if cb:
    for r in cb[:18]:
        print(f"  {r['ticker']:<6}{r['criticality']:>6}  {(r.get('name') or '')[:26]:<26} {(r.get('industry') or '')[:22]:<22} → {r.get('irreplaceability_reason','')}")
else:
    print("  (empty — verification may not have run; see below)")
print("\n❌ REJECTED (high-margin but NOT chokepoints — the false positives filtered out):")
for r in d.get("rejected_high_margin_sample",[])[:12]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {(r.get('name') or '')[:24]:<24} {r.get('reason','')}")
if not d['stats'].get('verified'):
    print("\n⚠️ verified=0 — LLM (GLM/Z.ai) unavailable this run; engine degraded gracefully (criticality still output, no confirmation).")
import urllib.request
def chk(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as x:return x.getcode()
        except Exception:time.sleep(8)
    return None
print("\npage:",chk("https://justhodl.ai/equity-chokepoint.html?t="+str(int(time.time()))))
print("DONE 2121")
