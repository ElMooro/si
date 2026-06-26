import urllib.request, json, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(url):
    try:
        return json.loads(urllib.request.urlopen(url,timeout=20).read())
    except Exception as e: return {"ERR":str(e)[:60]}
print("=== FMP /stable/ratios (MU, quarterly) — find inventory-days field ===")
r=get(f"https://financialmodelingprep.com/stable/ratios?symbol=MU&period=quarter&limit=4&apikey={FMP}")
if isinstance(r,list) and r:
    inv_fields=[k for k in r[0].keys() if "invent" in k.lower() or "dio" in k.lower() or "days" in k.lower()]
    print("  inventory/days-related fields:",inv_fields)
    print("  sample latest values:",{k:r[0].get(k) for k in inv_fields})
    print("  date field:",r[0].get("date"),"| period:",r[0].get("period"))
else: print("  ratios resp:",str(r)[:160])
print("\n=== FMP /stable/key-metrics (MU, quarterly) ===")
km=get(f"https://financialmodelingprep.com/stable/key-metrics?symbol=MU&period=quarter&limit=4&apikey={FMP}")
if isinstance(km,list) and km:
    inv_fields=[k for k in km[0].keys() if "invent" in k.lower() or "days" in k.lower()]
    print("  inventory fields:",inv_fields,{k:km[0].get(k) for k in inv_fields})
else: print("  key-metrics resp:",str(km)[:160])
print("\n=== universe availability ===")
s3=boto3.client("s3","us-east-1")
for key,field in [("data/bottleneck-boom.json","ranks"),("data/chokepoint.json","all_chokepoints"),("data/scarcity-radar.json","full_board")]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        rows=d.get(field) or d.get("stealth_shortage_board") or []
        tks=[r.get("ticker") for r in rows if r.get("ticker")]
        print(f"  {key}::{field} -> {len(tks)} tickers e.g. {tks[:8]}")
    except Exception as e: print(f"  {key} ERR {str(e)[:40]}")
print("DONE 2237")
