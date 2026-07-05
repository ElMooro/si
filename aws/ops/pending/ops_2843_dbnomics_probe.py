"""ops 2843 — discover DBnomics series IDs for canary-grid Phase 3:
Taiwan export orders, Chile/Peru copper production, KOF barometer, semiconductor billings."""
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
R={"ops":2843,"ts":datetime.now(timezone.utc).isoformat()}
API="https://api.db.nomics.world/v22"
def _get(url,t=40):
    req=urllib.request.Request(url,headers={"User-Agent":"jh-probe/1.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read())
def search(q,limit=8):
    try:
        d=_get(API+"/search?"+urllib.parse.urlencode({"q":q,"limit":limit}))
        docs=((d.get("results") or {}).get("docs")) or []
        return [{"id":"%s/%s"%(x.get("provider_code"),x.get("code")),"name":(x.get("name") or "")[:70],
                 "nb_series":x.get("nb_series"),"provider":x.get("provider_code")} for x in docs]
    except Exception as e: return [{"err":str(e)[:90]}]
def latest(series_id):
    try:
        d=_get(API+"/series/"+urllib.parse.quote(series_id)+"?observations=1")
        docs=((d.get("series") or {}).get("docs")) or []
        if not docs: return {"empty":True}
        doc=docs[0]; per=doc.get("period") or []; val=doc.get("value") or []
        pts=[(p,v) for p,v in zip(per,val) if isinstance(v,(int,float))]
        return {"name":(doc.get("series_name") or "")[:60],"n":len(pts),
                "latest":pts[-1] if pts else None,"series":series_id}
    except Exception as e: return {"err":str(e)[:80]}
for q in ["Taiwan export orders","Taiwan exports","Chile copper production","copper mine production Chile",
          "KOF economic barometer Switzerland","semiconductor billings","world semiconductor sales WSTS"]:
    R.setdefault("search",{})[q]=search(q)
# try some known/guessed series ids directly
for sid in ["KOF/BAROMETER/CH","IMF/CPI/M.TW.PCPI_IX","CEIC/...","DGBAS/..."]:
    pass
print(json.dumps(R,indent=1,default=str)[:3800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2843_dbnomics_probe.json","w"),indent=1,default=str)
print("OPS 2843 COMPLETE")
