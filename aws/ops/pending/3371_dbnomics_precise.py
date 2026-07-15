"""ops 3371 — precise DBnomics probing for SG/HK/TW sovereign yields. Explore IMF IFS
dataset dimensions (find the right INDICATOR for govt bond yield + monthly freq), and try
BIS/other providers. Goal: a series with RECENT observations (2025-2026), not stale."""
import json, urllib.request, urllib.parse
from ops_report import report

def get(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"__err__": f"{type(e).__name__} {str(e)[:70]}"}

def latest(obj):
    try:
        d = obj.get("series",{}).get("docs",[])[0]
        per,val = d.get("period",[]), d.get("value",[])
        pairs=[(per[i],val[i]) for i in range(len(val)) if val[i] is not None and val[i]==val[i]]
        return (d.get("series_code",""), pairs[-1] if pairs else None, len(pairs))
    except Exception as e:
        return (None,None,0)

with report("3371_dbnomics_precise") as r:
    # IMF IFS: monthly govt bond yield. Common indicator FIGB_PA (period avg). Try monthly M.
    r.section("IMF IFS monthly govt bond yield — SG/HK/TW/KR")
    for code, area in [("SG","Singapore"),("HK","Hong Kong"),("TW","Taiwan"),("KR","Korea")]:
        # monthly frequency, govt bond yield
        dim = urllib.parse.quote(json.dumps({"FREQ":["M"],"REF_AREA":[code],"INDICATOR":["FIGB_PA"]}))
        url = f"https://api.db.nomics.world/v22/series/IMF/IFS?dimensions={dim}&observations=1&limit=2"
        d = get(url)
        if d.get("__err__"): r.log(f"  {area}: {d['__err__']}"); continue
        r.log(f"  {area}: {latest(d)}")

    # try dataset browse to see what indicators exist for Singapore
    r.section("What bond/yield indicators does IMF IFS have for Singapore?")
    url = "https://api.db.nomics.world/v22/series/IMF/IFS?dimensions=" + urllib.parse.quote(json.dumps({"REF_AREA":["SG"]})) + "&limit=1000&facets=1"
    d = get(url)
    if not d.get("__err__"):
        docs = d.get("series",{}).get("docs",[])
        yieldish=[x for x in docs if any(t in (x.get("series_name","")+x.get("series_code","")).lower() for t in ("bond","yield","government secur","long-term","treasury"))]
        r.log(f"  Singapore series total={len(docs)}, yield-ish={len(yieldish)}")
        for x in yieldish[:8]:
            r.log(f"     {x.get('series_code')} — {x.get('series_name','')[:70]}")

    # BIS via DBnomics — the effective/policy + long series datasets
    r.section("Alternative providers for TW/HK")
    for prov, ds, area, note in [
        ("BIS","WS_LONG_CPI","TW","BIS"),
        ("Eurostat","irt_lt_mcby_m","","EU only-check"),
    ]:
        url=f"https://api.db.nomics.world/v22/datasets/{prov}/{ds}"
        d=get(url)
        r.log(f"  {prov}/{ds}: {'exists' if not d.get('__err__') else d.get('__err__')}")
