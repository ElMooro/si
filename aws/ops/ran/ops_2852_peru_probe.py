"""ops 2852 — find BCRP (Peru central bank) copper production series code + confirm API."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2852,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=45):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
# BCRP data API: /estadisticas/series/api/{code}/json/{start}/{end}/{lang}
def bcrp(code):
    try:
        d=json.loads(_get("https://estadisticas.bcrp.gob.pe/estadisticas/series/api/%s/json/2015-1/2026-12/ing"%code))
        name=(d.get("config",{}).get("series",[{}])[0] or {}).get("name","")
        per=d.get("periods",[])
        pts=[(p["name"],p["values"][0]) for p in per if p.get("values") and p["values"][0] not in ("n.d.","",None)]
        return {"name":name[:70],"n":len(pts),"latest":pts[-1] if pts else None}
    except Exception as e: return {"err":str(e)[:70]}
# candidate national copper production series (mensual). Try known + discovered codes.
cands=["PN01730AM","PN01731AM","PN01870AM","PN02079AM","PN01729AM","PN01732AM","PN01733AM"]
for c in cands:
    R.setdefault("probe",{})[c]=bcrp(c)
# discovery: fetch the mining production page + extract codes near 'obre' (Cobre=copper)
try:
    html=_get("https://estadisticas.bcrp.gob.pe/estadisticas/series/mensuales/produccion-de-productos-mineros-segun-departamentos")
    codes=re.findall(r'P[A-Z]?\d{4,6}[A-Z]{2}', html)
    R["page_codes_sample"]=sorted(set(codes))[:20]
    # find codes whose row mentions cobre
    R["cobre_context"]=[m[:90] for m in re.findall(r'.{0,40}[Cc]obre.{0,40}', html)][:6]
except Exception as e: R["page_err"]=str(e)[:80]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2852_peru_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2852 COMPLETE")
