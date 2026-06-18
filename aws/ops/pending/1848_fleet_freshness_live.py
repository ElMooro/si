import boto3, datetime, re
from collections import Counter
s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
pag=s3.get_paginator("list_objects_v2")
DATE=re.compile(r'\d{8}|\d{4}-\d{2}-\d{2}|\d{4}_\d{2}')
live=[]
for page in pag.paginate(Bucket=B, Prefix="data/"):
    for o in page.get("Contents",[]):
        k=o["Key"]
        if not k.endswith(".json"): continue
        rem=k[len("data/"):]
        if "/" in rem: continue                       # only top-level canonical outputs
        if DATE.search(rem): continue                  # skip dated snapshots
        if rem.endswith(("-prev.json","-cache.json")): continue
        age_h=(now-o["LastModified"]).total_seconds()/3600.0
        live.append((rem, age_h, o["Size"]))
print("CANONICAL LIVE OUTPUTS (data/*.json, dateless):", len(live))
def b(h):
    if h<26: return "0 FRESH(<26h)"
    if h<50: return "1 DAY(26-50h)"
    if h<24*4: return "2 2-4d"
    if h<24*8: return "3 4-8d"
    if h<24*21: return "4 8-21d"
    return "5 DEAD(>21d)"
c=Counter(b(h) for _,h,_ in live)
print("\n=== LIVE-OUTPUT FRESHNESS ===")
for k in sorted(c): print("  %-16s %d"%(k,c[k]))
stale=sorted([o for o in live if o[1]>=24*4], key=lambda x:-x[1])
print("\n=== %d LIVE outputs >4 DAYS old (stale; some may be legit weekly/quarterly) ==="%len(stale))
for k,h,sz in stale:
    print("  %6.1fd %9dB  %s"%(h/24.0, sz, k))
tiny=[o for o in live if o[2]<120]
print("\n=== %d tiny live outputs (<120B) ==="%len(tiny))
for k,h,sz in sorted(tiny,key=lambda x:-x[1]):
    print("  %6.1fd %dB  %s"%(h/24.0,sz,k))
