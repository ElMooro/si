"""ops 3377 — final avenue: does the PAID Polygon/Massive tier carry Singapore/HK/Taiwan
sovereign yields or indices? Probe Polygon reference + indices endpoints for Asian govt
bond tickers. Also try TradingEconomics-style tickers via Polygon. Uses existing Massive keys."""
import json, urllib.request
from ops_report import report

# Massive/Polygon keys from memory (base data valid, Benzinga dead)
KEYS = ["ptM","VFPI","JX_d"]  # placeholders — read actual from a known engine env at runtime
def get(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode())
    except Exception as e: return {"__err__":f"{type(e).__name__} {str(e)[:50]}"}

import boto3
lam=boto3.client("lambda",region_name="us-east-1")
def polygon_key():
    # pull a real polygon key from an existing engine's env
    for fn in ["justhodl-bond-desk","justhodl-global-markets","bond-indices-agent"]:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            for k,v in (c.get("Environment",{}).get("Variables",{})).items():
                if "POLYGON" in k.upper() or "MASSIVE" in k.upper():
                    return v
        except Exception: pass
    return None

with report("3377_polygon_asia") as r:
    key=polygon_key()
    r.log(f"polygon key found: {'yes' if key else 'no'}")
    if not key:
        r.log("no polygon key in probed engines — checking wider")
    r.section("Polygon reference — search Asian govt bond / index tickers")
    if key:
        for q in ["Singapore","Taiwan","Hong Kong government bond"]:
            d=get(f"https://api.polygon.io/v3/reference/tickers?search={q.replace(' ','%20')}&market=indices&limit=10&apikey={key}")
            res=d.get("results",[]) if not d.get("__err__") else []
            r.log(f"  '{q}' indices: {[x.get('ticker')+' '+x.get('name','')[:30] for x in res[:5]] if res else d.get('__err__','none')}")
    r.section("VERDICT — synthesize all findings")
    r.log("FRED: SG/HK/TW absent. DBnomics/IMF: SG stale-2021, HK absent, TW gold-only.")
    r.log("WGB: pages exist but yield/CDS load via JS async (not in static HTML).")
    r.log("FMP: no Asian sovereign symbols. This probe: polygon indices result above.")
