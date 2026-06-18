import boto3, datetime
from collections import Counter, defaultdict
s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
pag=s3.get_paginator("list_objects_v2")
objs=[]
for page in pag.paginate(Bucket=B):
    for o in page.get("Contents",[]):
        k=o["Key"]
        if not k.endswith(".json"): continue
        age_h=(now-o["LastModified"]).total_seconds()/3600.0
        objs.append((k, age_h, o["Size"]))
print("TOTAL .json objects in bucket:", len(objs))

# prefix distribution
pref=Counter(k.split("/")[0] if "/" in k else "(root)" for k,_,_ in objs)
print("\n=== objects by top-level prefix ===")
for p,n in pref.most_common(12): print("  %-22s %d"%(p,n))

def bucket(h):
    if h<26: return "0 FRESH(<26h)"
    if h<50: return "1 DAY(26-50h)"
    if h<24*8: return "2 WEEK(2-8d)"
    if h<24*32: return "3 MONTH(8-32d)"
    return "4 DEAD(>32d)"
c=Counter(bucket(h) for _,h,_ in objs)
print("\n=== FRESHNESS DISTRIBUTION (all .json) ===")
for k in sorted(c): print("  %-16s %d"%(k,c[k]))

stale=sorted([o for o in objs if o[1]>=24*8], key=lambda x:-x[1])
print("\n=== %d outputs >8 DAYS old (stale/dead candidates) ==="%len(stale))
for k,h,sz in stale[:90]:
    print("  %6.1fd %8dB  %s"%(h/24.0, sz, k))

tiny=[o for o in objs if o[2]<60 and o[1]<24*8]
print("\n=== %d FRESH-but-TINY (<60B, possible broken write) ==="%len(tiny))
for k,h,sz in sorted(tiny,key=lambda x:-x[1])[:30]:
    print("  %6.1fh %dB  %s"%(h, sz, k))
