"""ops 2856 — enumerate all series in SingStat M450981 (NODX by product) to pick total + electronics."""
import os, json, urllib.request
from datetime import datetime, timezone
R={"ops":2856,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=50):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode("utf-8","ignore"))
for tid in ["M450981","M450971","M083571"]:
    try:
        d=_get("https://tablebuilder.singstat.gov.sg/api/table/tabledata/%s"%tid)
        data=d.get("Data") or {}; rows=data.get("row") or []
        R.setdefault("tables",{})[tid]={"title":(data.get("title") or "")[:80],"n_rows":len(rows),
            "series":[{"name":(rw.get("rowText") or "")[:48],
                       "latest":[(c.get("key"),c.get("value")) for c in (rw.get("columns") or [])[-2:]]} for rw in rows[:14]]}
    except Exception as e: R.setdefault("tables",{})[tid]={"err":str(e)[:80]}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2856_sg_full.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2856 COMPLETE")
