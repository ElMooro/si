"""ops 3368 — for Singapore/HK/Taiwan (no FRED), probe FMP + other available sources for a
10Y sovereign yield or a tradeable ETF proxy. Only what returns real data gets built."""
import json, urllib.request
from ops_report import report

FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

def get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"__err__": f"{type(e).__name__} {str(e)[:50]}"}

with report("3368_asia_yield_probe") as r:
    r.section("FMP treasury / quote probes for SG/HK/TW")
    # FMP treasury endpoint is US-only; try economic + quotes for proxy ETFs/indices
    probes = {
        # Government bond ETFs / sovereign proxies that trade
        "Singapore MSCI ETF EWS": "https://financialmodelingprep.com/stable/quote?symbol=EWS&apikey="+FMP,
        "Hong Kong ETF EWH": "https://financialmodelingprep.com/stable/quote?symbol=EWH&apikey="+FMP,
        "Taiwan ETF EWT": "https://financialmodelingprep.com/stable/quote?symbol=EWT&apikey="+FMP,
        "S.Korea ETF EWY": "https://financialmodelingprep.com/stable/quote?symbol=EWY&apikey="+FMP,
    }
    for name, url in probes.items():
        d = get(url)
        if isinstance(d, list) and d:
            q = d[0]
            r.log(f"  {name}: price={q.get('price')} chg%={q.get('changePercentage')}")
        elif isinstance(d, dict) and d.get("__err__"):
            r.log(f"  {name}: {d['__err__']}")
        else:
            r.log(f"  {name}: {str(d)[:80]}")

    r.section("Verdict")
    r.log("FRED confirmed: South Korea 10Y yield (IRLTLT01KRM156N).")
    r.log("Finland: ECB SovCISS confirmed.")
    r.log("SG/HK/TW: no FRED sovereign yield; ETF price proxies only measure EQUITY not SOVEREIGN stress.")
    r.log("HONEST PLAN: add Finland (SovCISS) + Spain (already) + S.Korea (bond-yield proxy).")
    r.log("SG/HK/TW: add as bond-yield proxy ONLY if a real yield source exists; else mark data_unavailable (named), never fake.")
