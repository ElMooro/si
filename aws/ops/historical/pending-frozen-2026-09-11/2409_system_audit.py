import boto3, json, time
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1")
B="justhodl-dashboard-live"
now=datetime.now(timezone.utc)
# 1) list all .json objects (data/ + root), capture LastModified
objs=[]
tok=None
while True:
    kw={"Bucket":B,"MaxKeys":1000}
    if tok: kw["ContinuationToken"]=tok
    r=s3.list_objects_v2(**kw)
    for o in r.get("Contents",[]):
        k=o["Key"]
        if k.endswith(".json"):
            age=(now-o["LastModified"]).total_seconds()/3600.0
            objs.append((k,age,o["Size"]))
    tok=r.get("NextContinuationToken")
    if not tok: break
print("total .json objects:",len(objs))
# bucket by age
import collections
buckets=collections.Counter()
for k,a,s in objs:
    b=("<6h" if a<6 else "6-25h" if a<25 else "25-48h" if a<48 else "2-7d" if a<168 else "7-30d" if a<720 else ">30d")
    buckets[b]+=1
print("age distribution:",dict(buckets))
# 2) STALE files (>48h) — likely broken schedule/engine (exclude obvious weekly/archive by name)
weekly_hint=("cot","settlement","sovereign","fails","weekly","tic","13f","cftc")
stale=[(k,a) for k,a,s in objs if a>=48]
stale.sort(key=lambda x:-x[1])
print("\n=== STALE (>48h) — %d files ==="%len(stale))
for k,a in stale[:50]:
    wk=" [weekly?]" if any(w in k.lower() for w in weekly_hint) else ""
    print("  %6.0fh  %s%s"%(a,k,wk))
# 3) ERROR / null-heavy scan on RECENT files (<30d) — read bodies
print("\n=== ERROR fields / suspicious content (recent files) ===")
errs=[]
for k,a,s in objs:
    if a>720: continue   # skip ancient
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e:
        errs.append((k,"unreadable:"+str(e)[:40])); continue
    if not isinstance(d,dict): continue
    flags=[]
    for ef in ("errors","_err","_diag","error"):
        v=d.get(ef)
        if v: flags.append("%s=%s"%(ef,str(v)[:80]))
    # null-density of top-level scalar values
    sc=[v for v in d.values() if isinstance(v,(int,float,str)) or v is None]
    if sc:
        nulls=sum(1 for v in sc if v in (None,0,"","N/A","-"))
        if len(sc)>=6 and nulls/len(sc)>0.7: flags.append("null_heavy %d/%d"%(nulls,len(sc)))
    if flags: errs.append((k," | ".join(flags)))
for k,f in errs[:60]:
    print("  %s -> %s"%(k,f))
print("\nstale_count:%d  error_count:%d"%(len(stale),len(errs)))
print("DONE 2409")
