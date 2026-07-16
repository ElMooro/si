"""ops 3400 — can we build a LONG-HISTORY eurodollar-hub stress back to ~1990? CDS doesn't
exist pre-2003 (the market is young), but 10Y govt bond YIELDS + spreads-vs-US/Bund do go
back decades on FRED. Probe FRED long-term govt bond yield series (IRLTLT01) for each hub +
their inception dates, so we can reconstruct a yield-spread-based historical barometer."""
import json, urllib.request
from ops_report import report
FRED="2f057499936072679d8843d7fce99989"
def series_start(sid):
    url=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=asc&limit=1"
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=20) as r:
            d=json.loads(r.read().decode())
            obs=[o for o in d.get("observations",[]) if o.get("value") not in (".","",None)]
            if obs: return obs[0]["date"]
            # some series first real obs isn't first row
            return d.get("observations",[{}])[0].get("date","?")
    except Exception as e:
        return f"ERR {str(e)[:30]}"

# FRED long-term (10Y) govt bond yield, OECD MEI: IRLTLT01{CC}M156N (monthly)
HUBS={
 "United States":"IRLTLT01USM156N","United Kingdom":"IRLTLT01GBM156N","Germany":"IRLTLT01DEM156N",
 "France":"IRLTLT01FRM156N","Italy":"IRLTLT01ITM156N","Spain":"IRLTLT01ESM156N",
 "Switzerland":"IRLTLT01CHM156N","Netherlands":"IRLTLT01NLM156N","Belgium":"IRLTLT01BEM156N",
 "Ireland":"IRLTLT01IEM156N","Finland":"IRLTLT01FIM156N","Greece":"IRLTLT01GRM156N",
 "Portugal":"IRLTLT01PTM156N","Sweden":"IRLTLT01SEM156N","Japan":"IRLTLT01JPM156N",
 "South Korea":"IRLTLT01KRM156N","Canada":"IRLTLT01CAM156N","Australia":"IRLTLT01AUM156N",
}
with report("3400_hub_history_probe") as r:
    r.section("FRED 10Y govt bond yield — inception per hub")
    starts={}
    for name,sid in HUBS.items():
        s=series_start(sid); starts[name]=s
        r.log(f"  {name} ({sid}): from {s}")
    # how far back can a BROAD composite go?
    r.section("Composite feasibility")
    valid=[s for s in starts.values() if s and s[0].isdigit()]
    r.log(f"  earliest hub series: {min(valid) if valid else '?'}")
    r.log(f"  hubs with data from ≤1990: {sum(1 for s in valid if s<='1990-12-31')}")
    r.log(f"  hubs with data from ≤2000: {sum(1 for s in valid if s<='2000-12-31')}")
    r.log("  NOTE: HK/Taiwan/Singapore/Chile/Peru NOT on FRED IRLTLT01 → long history only for the DM core.")
