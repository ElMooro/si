"""ops 2050: HONEST coverage audit — of all manifested engines, how many carry an extractable
verdict vs are raw-data feeds with no opinion vs are stale? Sample the misses to judge."""
import boto3, json, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
import importlib.util
spec=importlib.util.spec_from_file_location("st","aws/lambdas/justhodl-strategist/source/lambda_function.py")
st=importlib.util.module_from_spec(spec); spec.loader.exec_module(st)

man=json.loads(s3.get_object(Bucket=B,Key="data/engine-manifest.json")["Body"].read())
feeds={}
for e in man.get("engines",[]):
    ks=e.get("keys") or []
    if ks: feeds[ks[0]]=e["engine"].replace("justhodl-","")
print("manifested engines with a primary key:",len(feeds))

def probe(key):
    try:
        o=s3.get_object(Bucket=B,Key=key); d=json.loads(o["Body"].read())
        age=(datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600
        info=st.extract(d)
        topkeys=list(d.keys())[:8] if isinstance(d,dict) else []
        return key,("STALE" if age>=240 else ("VERDICT" if info else "NOVIEW")),age,info,topkeys
    except Exception as e:
        return key,"MISSING",None,None,str(e)[:40]

res=[]
with ThreadPoolExecutor(max_workers=24) as ex:
    res=list(ex.map(probe,list(feeds.keys())))
from collections import Counter
c=Counter(r[1] for r in res)
print("\nCOVERAGE BUCKETS:",dict(c))
print(f"  VERDICT (fresh, has a directional read — IN the Strategist): {c['VERDICT']}")
print(f"  NOVIEW  (fresh but no extractable verdict): {c['NOVIEW']}")
print(f"  STALE   (>10d old): {c['STALE']}")
print(f"  MISSING (key absent): {c['MISSING']}")
print("\n--- SAMPLE of NOVIEW (fresh, no verdict) — are these raw-data feeds or missed opinions? ---")
nv=[r for r in res if r[1]=="NOVIEW"][:18]
for k,b,age,info,tk in nv:
    print(f"  {k.replace('data/','')[:34]:<34} keys={tk}")
print("DONE 2050")
