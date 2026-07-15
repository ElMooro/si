"""ops 3372 — last-resort probe for CURRENT SG/HK/TW sovereign data before deciding. Try:
(1) FMP index/quote for sovereign-yield tickers, (2) government-bond ETFs that hold local
sovereigns (a real-market proxy), (3) MAS/HKMA-style series on DBnomics other providers."""
import json, urllib.request, urllib.parse
from ops_report import report
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(url, t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode())
    except Exception as e: return {"__err__":f"{type(e).__name__} {str(e)[:50]}"}

with report("3372_asia_last_resort") as r:
    # FMP: does it have any Asian sovereign yield symbols?
    r.section("FMP symbol search — Asian govt bond yields")
    for q in ["Singapore 10Y","Hong Kong bond","Taiwan bond yield"]:
        d=get(f"https://financialmodelingprep.com/stable/search-symbol?query={urllib.parse.quote(q)}&apikey={FMP}")
        if isinstance(d,list): r.log(f"  '{q}': {[x.get('symbol') for x in d[:5]]}")
        else: r.log(f"  '{q}': {str(d)[:80]}")

    # Local-sovereign government bond ETFs (real market instruments tracking these sovereigns)
    r.section("Government-bond ETF proxies (hold LOCAL sovereign debt)")
    etfs = {
        "Singapore Govt Bond ETF (A35.SI)": "A35.SI",
        "iShares Asia local govt bond (N6M.SI)": "N6M.SI",
        "abrdn Asia bond": "ABF",
    }
    for name,sym in etfs.items():
        d=get(f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP}")
        if isinstance(d,list) and d: r.log(f"  {name} [{sym}]: price={d[0].get('price')} yield-proxy via price")
        else: r.log(f"  {name} [{sym}]: {str(d)[:70]}")

    # DBnomics WEO / other providers that might have TW (World Bank uses TWN sometimes)
    r.section("World Bank / other for Taiwan")
    for prov_ds_area in [("WB/WDI","TWN"),("IMF/IFS","TP"),("IMF/IFS","TW")]:
        pd,area=prov_ds_area
        url=f"https://api.db.nomics.world/v22/series/{pd}?dimensions="+urllib.parse.quote(json.dumps({"REF_AREA":[area]}))+"&limit=3"
        d=get(url); n=d.get("series",{}).get("num_found","?") if not d.get("__err__") else d.get("__err__")
        r.log(f"  {pd} area={area}: num_found={n}")
