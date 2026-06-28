import boto3, collections
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.now(timezone.utc)
objs=[]; tok=None
while True:
    kw={"Bucket":B,"MaxKeys":1000}
    if tok: kw["ContinuationToken"]=tok
    r=s3.list_objects_v2(**kw)
    for o in r.get("Contents",[]):
        k=o["Key"]
        if k.endswith(".json"):
            objs.append((k,(now-o["LastModified"]).total_seconds()/3600.0))
    tok=r.get("NextContinuationToken")
    if not tok: break
print("total .json:",len(objs))
# focus on LIVE engine outputs: root *.json, or data/<name>.json (one level), exclude history/archive/snapshot/date-stamped
def is_live(k):
    if "/" not in k: return True                       # root
    parts=k.split("/")
    if parts[0]!="data" or len(parts)!=2: return False # only data/<file>.json
    name=parts[1].lower()
    if any(w in name for w in ("history","archive","snapshot","backup","-bak","2024","2025","2026","daily-","_old")): return False
    return True
live=[(k,a) for k,a in objs if is_live(k)]
print("live engine outputs:",len(live))
buckets=collections.Counter()
for k,a in live:
    b=("<6h" if a<6 else "6-25h" if a<25 else "25-49h" if a<49 else "2-7d" if a<168 else "7-30d" if a<720 else ">30d")
    buckets[b]+=1
print("age distribution (live):",dict(buckets))
weekly=("cot","settlement","sovereign","fails","weekly","tic","13f","cftc","fomc","auction")
stale=[(k,a) for k,a in live if a>=49]; stale.sort(key=lambda x:-x[1])
print("\n=== STALE live outputs (>49h), %d total ==="%len(stale))
for k,a in stale:
    tag=" [weekly-ok?]" if any(w in k.lower() for w in weekly) else (" [DEAD>30d]" if a>720 else " [DEAD>7d]" if a>168 else "")
    print("  %7.0fh  %s%s"%(a,k,tag))
print("DONE 2410")
