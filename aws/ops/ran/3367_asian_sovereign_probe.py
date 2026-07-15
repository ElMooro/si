"""ops 3367 — probe data availability for the new sovereigns before building. Finland via
ECB SovCISS; Asian sovereigns (HK/SG/TW/KR) via FRED 10Y govt bond yield candidates.
Only build on series that actually return data. Read-only."""
import json, urllib.request
from ops_report import report

FRED_KEY = "2f057499936072679d8843d7fce99989"

def fred_check(sid):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&limit=3&sort_order=desc"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read().decode())
            obs = [o for o in d.get("observations",[]) if o.get("value") not in (".","",None)]
            if obs:
                return f"OK latest={obs[0]['value']} @ {obs[0]['date']}"
            return "empty"
    except Exception as e:
        return f"{type(e).__name__} {str(e)[:40]}"

def ecb_check(key):
    url = f"https://data-api.ecb.europa.eu/service/data/CISS/{key}?format=csvdata&lastNObservations=3"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"jh/1.0","Accept":"text/csv, */*"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode()
            lines = [l for l in body.splitlines() if l.strip()]
            return f"OK {len(lines)-1} rows" if len(lines)>1 else "empty"
    except Exception as e:
        return f"{type(e).__name__} {str(e)[:40]}"

with report("3367_new_sovereign_probe") as r:
    r.section("Finland — ECB SovCISS")
    r.log(f"  M.FI SOV_CI: {ecb_check('M.FI.Z0Z.4F.EC.SOV_CI.IDX')}")

    r.section("Asian sovereigns — FRED 10Y govt bond yield candidates")
    candidates = {
        "South Korea IRLTLT01KRM156N": "IRLTLT01KRM156N",
        "South Korea INTGSBKRM193N": "INTGSBKRM193N",
        "Singapore IRLTLT01SGM156N": "IRLTLT01SGM156N",
        "Singapore INTGSBSGM193N": "INTGSBSGM193N",
        "Singapore MYAGM2SGM189N": "MYAGM2SGM189N",
        "Hong Kong (try) IRLTLT01HKM156N": "IRLTLT01HKM156N",
        "Taiwan (try) IRLTLT01TWM156N": "IRLTLT01TWM156N",
        "Taiwan INTGSBTWM193N": "INTGSBTWM193N",
    }
    for name, sid in candidates.items():
        r.log(f"  {name}: {fred_check(sid)}")

    r.section("also probe: 3-month rates + CDS-ish proxies as fallbacks")
    fallbacks = {
        "Singapore 3M INTGSTSGM193N": "INTGSTSGM193N",
        "Korea 3M IR3TIB01KRM156N": "IR3TIB01KRM156N",
        "Korea share prices SPASTT01KRM661N": "SPASTT01KRM661N",
    }
    for name, sid in fallbacks.items():
        r.log(f"  {name}: {fred_check(sid)}")
