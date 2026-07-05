"""ops 2854 — find Singapore NODX (non-oil domestic exports) on SingStat TableBuilder API."""
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
R={"ops":2854,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=45):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode("utf-8","ignore"))
# 1) resource search
for kw in ["non-oil domestic exports","NODX","domestic exports"]:
    try:
        d=_get("https://tablebuilder.singstat.gov.sg/api/table/resourceid?keyword="+urllib.parse.quote(kw))
        recs=(d.get("Data") or {}).get("records") or d.get("records") or []
        R.setdefault("search",{})[kw]=[{"id":r.get("id"),"title":(r.get("title") or "")[:70]} for r in recs][:8]
    except Exception as e: R.setdefault("search",{})[kw]=[{"err":str(e)[:70]}]
# 2) inspect the most promising (monthly NODX total, YoY or level). Try a known id if search returns one.
cand=None
for kw,recs in (R.get("search") or {}).items():
    for r in recs:
        t=(r.get("title") or "").lower()
        if r.get("id") and "non-oil" in t and ("month" in t or "nodx" in t or "domestic export" in t):
            cand=r["id"]; break
    if cand: break
if not cand:
    for kw,recs in (R.get("search") or {}).items():
        if recs and recs[0].get("id"): cand=recs[0]["id"]; break
if cand:
    try:
        m=_get("https://tablebuilder.singstat.gov.sg/api/table/tabledata/%s?seriesNoORrowNo=1&sortBy=key&offset=0&limit=6"%cand)
        data=m.get("Data") or {}
        rows=data.get("row") or []
        R["sample"]={"id":cand,"title":(data.get("title") or "")[:80],
                     "rows":[{"key":(rw.get("rowText") or "")[:50],"cols":[c for c in (rw.get("columns") or [])][:3]} for rw in rows[:4]]}
    except Exception as e: R["sample_err"]=str(e)[:100]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2854_singapore_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2854 COMPLETE")
