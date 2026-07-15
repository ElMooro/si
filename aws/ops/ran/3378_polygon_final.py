"""ops 3378 — final: use the real Polygon key to check if the paid tier carries Asian
sovereign yield/bond indices for SG/HK/TW."""
import json, urllib.request, urllib.parse
from ops_report import report
KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def get(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode())
    except Exception as e: return {"__err__":f"{type(e).__name__} {str(e)[:60]}"}
with report("3378_polygon_final") as r:
    r.section("Polygon indices/tickers search — Asian sovereigns")
    for q in ["Singapore bond","Taiwan","Hong Kong bond","government bond yield"]:
        d=get(f"https://api.polygon.io/v3/reference/tickers?search={urllib.parse.quote(q)}&limit=10&apikey={KEY}")
        if d.get("__err__"): r.log(f"  '{q}': {d['__err__']}"); continue
        res=d.get("results",[])
        r.log(f"  '{q}': {len(res)} — {[(x.get('ticker'),x.get('name','')[:28],x.get('market')) for x in res[:4]]}")
    r.section("Direct sovereign-yield index tickers (I:...)")
    for tk in ["I:SG10Y","I:TW10Y","I:HK10Y","SG10Y","TW10Y"]:
        d=get(f"https://api.polygon.io/v3/reference/tickers/{tk}?apikey={KEY}")
        r.log(f"  {tk}: {'FOUND '+str(d.get('results',{}).get('name','')) if d.get('results') else d.get('__err__','not found')}")
