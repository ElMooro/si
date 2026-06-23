import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-chokepoint")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName="justhodl-chokepoint",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:160],f"({time.time()-t:.0f}s)")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/chokepoint.json")["Body"].read())
print("stats:",d["stats"])
# cache size = cumulative coverage
try:
    cache=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_cache/chokepoint-irreplaceability.json")["Body"].read())
    from collections import Counter
    cnt=Counter(v["verdict"] for v in cache.values())
    print("cache total judged:",len(cache),"verdict split:",dict(cnt))
except Exception as e: print("cache:",str(e)[:50])
print("\n✅ CONFIRMED true chokepoints:")
for r in d.get("confirmed_chokepoint_book",[])[:20]:
    print(f"  {r['ticker']:<6}{r['criticality']:>6}  {(r.get('name') or '')[:24]:<24} {(r.get('industry') or '')[:20]:<20} → {r.get('irreplaceability_reason','')}")
print("\n❌ REJECTED sample:")
for r in d.get("rejected_high_margin_sample",[])[:10]:
    print(f"  {r['ticker']:<6} {(r.get('name') or '')[:22]:<22} {r.get('reason','')}")
print("DONE 2122")
