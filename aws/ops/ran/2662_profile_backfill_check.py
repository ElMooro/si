"""ops 2662 — check FMP /profile for sector/industry on AI-cohort names missing from the
broad screener (likely below the $300M floor), to design a cheap backfill."""
import urllib.request, json
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(path):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=15).read())
    except Exception as e: return {"__err__": str(e)[:120]}
for sym in ["PRIM","IREN","CALX","ROAD"]:
    d = get(f"profile?symbol={sym}")
    if isinstance(d, list) and d:
        print(f"  {sym}: sector={d[0].get('sector')} industry={d[0].get('industry')} mktCap={d[0].get('marketCap')}")
    else:
        print(f"  {sym}: {d}")
print("DONE 2662")
