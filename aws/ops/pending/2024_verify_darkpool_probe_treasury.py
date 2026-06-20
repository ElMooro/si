"""ops 2024: (A) confirm dark-pool quiet picks; (B) probe FRED CMT curve + funding spreads for #3."""
import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1"); B="justhodl-dashboard-live"
FRED="2f057499936072679d8843d7fce99989"
print("="*60);print("A) dark-pool quiet picks");print("="*60)
lam.invoke(FunctionName="justhodl-dark-pool",InvocationType="RequestResponse")
import time; time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/dark-pool.json")["Body"].read())
print("picks (should exclude +20-30% movers):")
for p in d.get("top_picks",[])[:14]:
    print(f"  {p['ticker']:<6} sc={p['score']:<5} dark%={p['dark_pool_pct']:<6} wkRet={p.get('week_return_pct')}%")

print("\n"+"="*60);print("B) FRED Treasury data probe for #3");print("="*60)
def fred(sid,n=3,start=None):
    q=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit={n}"
    if start: q+=f"&observation_start={start}&sort_order=asc&limit=5"
    try:
        with urllib.request.urlopen(q,timeout=20) as r: return json.loads(r.read()).get("observations",[])
    except Exception as e: return [{"err":str(e)[:60]}]
CMT=["DGS1MO","DGS3MO","DGS6MO","DGS1","DGS2","DGS3","DGS5","DGS7","DGS10","DGS20","DGS30"]
print("Latest CMT curve:")
curve={}
for s in CMT:
    o=fred(s,1); v=o[0].get("value") if o else None; curve[s]=v
    print(f"  {s:<8} {v}  ({o[0].get('date') if o else '-'})")
print("\nFunding-stress series:")
for s in ["DTB3","DFF","SOFR","DGS3MO"]:
    o=fred(s,1); print(f"  {s:<6} {o[0].get('value') if o else None} ({o[0].get('date') if o else '-'})")
print("\nHistory depth (DGS10 obs over ~2y):")
o=fred("DGS10",1,start="2024-01-01"); print("  earliest 2024 sample:",o[:2] if o else None)
print("\nSVB-week CMT (2023-03-13) to confirm dislocation potential:")
for s in ["DGS1MO","DGS3MO","DGS1","DGS2","DGS10"]:
    q=f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={FRED}&file_type=json&observation_start=2023-03-13&observation_end=2023-03-13"
    try:
        with urllib.request.urlopen(q,timeout=20) as r: obs=json.loads(r.read()).get("observations",[])
        print(f"  {s:<8} {obs[0].get('value') if obs else None}")
    except Exception as e: print(f"  {s} err {str(e)[:40]}")
print("DONE 2024")
