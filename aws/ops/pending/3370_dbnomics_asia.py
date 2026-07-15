"""ops 3370 — hunt for REAL Singapore/HK/Taiwan sovereign yields via DBnomics (keyless,
aggregates BIS/central-bank/IMF data beyond FRED's OECD set). Probe candidate providers:
BIS long series, IMF IFS, and DBnomics search. Only what returns real data gets built."""
import json, urllib.request, urllib.parse
from ops_report import report

def get(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"__err__": f"{type(e).__name__} {str(e)[:60]}"}

def latest_from_series(obj):
    """DBnomics series JSON → latest (period, value)."""
    try:
        docs = obj.get("series", {}).get("docs", [])
        if not docs: return None
        d = docs[0]
        per = d.get("period", []); val = d.get("value", [])
        pairs = [(per[i], val[i]) for i in range(len(val)) if val[i] is not None and val[i]==val[i]]
        return pairs[-1] if pairs else None
    except Exception:
        return None

with report("3370_dbnomics_asia_yields") as r:
    # 1. DBnomics SEARCH for each country's govt bond yield
    r.section("DBnomics search — 10Y govt bond yield")
    for q in ["Singapore government bond yield 10 year",
              "Hong Kong government bond yield",
              "Taiwan government bond yield"]:
        url = "https://api.db.nomics.world/v22/search?q=" + urllib.parse.quote(q) + "&limit=5"
        d = get(url)
        if d.get("__err__"):
            r.log(f"  '{q}': {d['__err__']}"); continue
        results = d.get("results", {}).get("docs", [])
        r.log(f"  '{q}': {len(results)} hits")
        for res in results[:4]:
            r.log(f"     {res.get('provider_code')}/{res.get('dataset_code')} — {res.get('name','')[:70]}")

    # 2. Direct BIS long-yield probes (BIS publishes many non-OECD)
    r.section("DBnomics direct — BIS + IMF candidate series")
    candidates = {
        "BIS Singapore policy/long": "https://api.db.nomics.world/v22/series/BIS/long_series?dimensions=%7B%22REF_AREA%22%3A%5B%22SG%22%5D%7D&limit=3&observations=1",
        "IMF IFS Singapore govt bond": "https://api.db.nomics.world/v22/series/IMF/IFS?dimensions=%7B%22REF_AREA%22%3A%5B%22SG%22%5D%2C%22INDICATOR%22%3A%5B%22FIGB_PA%22%5D%7D&limit=3&observations=1",
        "IMF IFS Hong Kong govt bond": "https://api.db.nomics.world/v22/series/IMF/IFS?dimensions=%7B%22REF_AREA%22%3A%5B%22HK%22%5D%2C%22INDICATOR%22%3A%5B%22FIGB_PA%22%5D%7D&limit=3&observations=1",
    }
    for name, url in candidates.items():
        d = get(url)
        if d.get("__err__"):
            r.log(f"  {name}: {d['__err__']}"); continue
        lt = latest_from_series(d)
        n = d.get("series",{}).get("num_found", 0)
        r.log(f"  {name}: num_found={n} latest={lt}")
