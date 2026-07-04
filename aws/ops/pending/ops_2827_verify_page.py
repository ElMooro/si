"""ops 2827 — verify US Data Desk page + 3 feeds live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2827,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read().decode("utf-8","ignore")
okp=False
for _ in range(5):
    time.sleep(20)
    try:
        st,b=get("https://justhodl.ai/us-data-desk.html")
        marks=["US Data Desk","bls-labor.json","bea-economic.json","census-economic.json"]
        hits={m:(m in b) for m in marks}; okp=(st==200 and all(hits.values()))
        R["page"]={"status":st,"markers":hits}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:60]
for nm,key in [("bls","data/bls-labor.json"),("bea","data/bea-economic.json"),("census","data/census-economic.json")]:
    try:
        st,b=get("https://justhodl.ai/"+key); d=json.loads(b)
        R[nm]={"status":st,"series_live":d.get("_series_live") if d.get("_series_live") is not None else d.get("_blocks_live"),
               "gen":d.get("generated_at","")[:10]}
    except Exception as e: R[nm]="err "+str(e)[:50]
R["status"]="US DATA DESK LIVE" if okp else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2827_verify_page.json","w"),indent=1,default=str)
print("OPS 2827 COMPLETE")
