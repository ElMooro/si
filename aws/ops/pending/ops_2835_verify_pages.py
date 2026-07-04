import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2835,"ts":datetime.now(timezone.utc).isoformat()}
def get(u):
    req=urllib.request.Request(u+"?cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=25) as r: return r.status,r.read().decode("utf-8","ignore")
res={}
for _ in range(5):
    time.sleep(20); ok=True
    for pg,marks in {"us-data-desk.html":["Supercore","Control group","Core capex"],"macro-leads.html":["freight_activity","Freight"]}.items():
        try:
            st,b=get("https://justhodl.ai/"+pg); hits={m:(m in b) for m in marks}
            res[pg]={"status":st,"all":all(hits.values()),"markers":hits}
            if not(st==200 and all(hits.values())): ok=False
        except Exception as e: res[pg]="err "+str(e)[:50]; ok=False
    if ok: break
R["pages"]=res
R["status"]="PAGES LIVE" if all(isinstance(v,dict) and v.get("all") for v in res.values()) else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2835_verify_pages.json","w"),indent=1,default=str)
print("OPS 2835 COMPLETE")
