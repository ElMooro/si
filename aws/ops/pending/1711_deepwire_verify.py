import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
def get(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())

# 1) earnings calendar (via universe)
print("invoking finviz-universe (calendar)..."); lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
c=get("data/finviz-earnings-calendar.json")
print(f"  earnings calendar: {c.get('n_days')} days, {c.get('n_reporters')} reporters next 30d")
for d in c.get("calendar",[])[:4]:
    print(f"    {d['date']}: {d['n']} — {', '.join(r['ticker'] for r in d['reporters'][:8])}")

# 2) sector-rotation finviz layer
print("\ninvoking sector-rotation..."); lam.invoke(FunctionName="justhodl-sector-rotation",InvocationType="RequestResponse")
sr=get("data/sector-rotation.json"); fr=sr.get("finviz_rotation",{})
print(f"  finviz_rotation present: {bool(fr)} | industries_top={len(fr.get('industries_top',[]))} cap_buckets={len(fr.get('mktcap_buckets',[]))}")
print("    top industries:", ", ".join(f"{x['name']}={x.get('perf_m')}%" for x in fr.get("industries_top",[])[:4]))

# 3) retail corroboration FinvizNews venue
print("\ninvoking retail-sentiment..."); lam.invoke(FunctionName="justhodl-retail-sentiment",InvocationType="RequestResponse")
rt=get("data/retail-sentiment.json")
allrows=rt.get("top_30_by_mentions",[]) or []
fvn=[e.get("ticker") for e in allrows if "FinvizNews" in (e.get("corroboration") or [])]
print(f"  names with FinvizNews corroboration (top30): {len(fvn)} {fvn[:8]}")

# 4) valuations sector tilt (async + poll)
print("\ninvoking stock-valuations (async)..."); 
before=get("data/stock-valuations.json").get("generated_at")
lam.invoke(FunctionName="justhodl-stock-valuations",InvocationType="Event")
v=None
for i in range(18):
    time.sleep(20)
    d=get("data/stock-valuations.json")
    if d.get("generated_at")!=before: v=d; print(f"  refreshed after ~{(i+1)*20}s"); break
if not v: v=get("data/stock-valuations.json")
sp=v.get("sp_table",[])
tagged=sum(1 for r in sp if r.get("sector_valuation"))
print(f"  sp_table sector_valuation tagged: {tagged}/{len(sp)}")
for r in sp[:4]: print(f"    {r.get('t'):6} sector={r.get('sector')} secVal={r.get('sector_valuation')} secPE={r.get('sector_fwd_pe')}")
