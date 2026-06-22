import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}

aa=g("data/analyst-actions.json")
print("=== analyst-actions negative arrays ===")
for arr in ("downgrades","pt_cuts","guidance_cuts"):
    v=aa.get(arr,[])
    print(f"  {arr}: n={len(v)}", "| item0:", json.dumps(v[0],default=str)[:240] if v else "—")

et=g("data/earnings-tracker.json")
print("\n=== earnings-tracker dated results ===")
for arr in ("recent_results_30d","pead_signals"):
    v=et.get(arr,[])
    print(f"  {arr}: n={len(v)}")
    if v: print("    keys:",list(v[0].keys()),"\n    item0:",json.dumps(v[0],default=str)[:340])

print("\n=== Polygon reference: does it carry sic/sector? ===")
url=f"https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&limit=3&apiKey={POLY}"
j=json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=20).read())
r0=(j.get("results") or [{}])[0]
print("  reference fields:",list(r0.keys()))
print("  sample:",json.dumps({k:r0.get(k) for k in ("ticker","sic_code","sic_description","type")},default=str))

print("\n=== sector SPDR ETFs present in grouped-daily? ===")
import datetime
# most recent weekday
d=datetime.date.today()
while d.weekday()>=5: d-=datetime.timedelta(days=1)
gd=json.loads(urllib.request.urlopen(urllib.request.Request(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{d}?adjusted=true&apiKey={POLY}",headers={"User-Agent":"jh"}),timeout=30).read())
tk={r["T"] for r in gd.get("results",[])}
spdrs=["XLK","XLF","XLE","XLV","XLI","XLY","XLP","XLU","XLB","XLRE","XLC","SPY","IWM","IVE","IVW"]
print(f"  grouped {d}: {len(tk)} tickers | present:",{s:(s in tk) for s in spdrs})
print("DONE 2095")
