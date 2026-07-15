"""ops 3373 — Taiwan has 124 IMF IFS series under REF_AREA=TW. Find the bond-yield/interest
indicator + check recency. Also re-list Singapore's yield series recency definitively."""
import json, urllib.request, urllib.parse
from ops_report import report
def get(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode())
    except Exception as e: return {"__err__":f"{type(e).__name__} {str(e)[:60]}"}
def latest(obj):
    try:
        d=obj.get("series",{}).get("docs",[])[0]
        per,val=d.get("period",[]),d.get("value",[])
        pairs=[(per[i],val[i]) for i in range(len(val)) if val[i] is not None and val[i]==val[i]]
        return (d.get("series_code"), pairs[-1] if pairs else None, len(pairs))
    except: return (None,None,0)

with report("3373_taiwan_indicator") as r:
    r.section("Taiwan IMF IFS — all interest/yield indicators")
    url="https://api.db.nomics.world/v22/series/IMF/IFS?dimensions="+urllib.parse.quote(json.dumps({"REF_AREA":["TW"]}))+"&limit=1000"
    d=get(url)
    docs=d.get("series",{}).get("docs",[]) if not d.get("__err__") else []
    hits=[x for x in docs if any(t in (x.get("series_name","")+x.get("series_code","")).lower() for t in ("bond","yield","interest","lending","deposit","money market","discount","secur"))]
    r.log(f"  Taiwan total={len(docs)} interest/yield-ish={len(hits)}")
    for x in hits[:14]:
        r.log(f"     {x.get('series_code')} — {x.get('series_name','')[:68]}")

    r.section("Recency check — best Taiwan + Singapore yield candidates")
    for code in ["M.TW.FIGB_PA","M.TW.FIMM_PA","M.TW.FID_PA","M.TW.FILR_PA","M.SG.FIGB_PA","M.SG.FITB_PA"]:
        u=f"https://api.db.nomics.world/v22/series/IMF/IFS/{code}?observations=1"
        d=get(u)
        r.log(f"  {code}: {latest(d)}")
