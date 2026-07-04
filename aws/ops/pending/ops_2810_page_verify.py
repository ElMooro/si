"""ops 2810 — verify macro-leads.html + feed live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2810,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read()
okp=False
for _ in range(5):
    time.sleep(25)
    try:
        st,b=get("https://justhodl.ai/macro-leads.html")
        okp=(st==200 and b"Macro Leads" in b); R["page"]={"status":st,"has_marker":b"Macro Leads" in b}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:50]
try:
    st,b=get("https://justhodl.ai/data/macro-leads.json"); d=json.loads(b)
    R["feed"]={"status":st,"populated":d.get("_populated")}
except Exception as e: R["feed"]="err "+str(e)[:60]
R["status"]="PAGE LIVE" if okp else "CHECK"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2810_page_verify.json","w"),indent=1,default=str)
print("page:",json.dumps(R.get("page"))); print("feed:",json.dumps(R.get("feed"))); print("STATUS:",R["status"]); print("OPS 2810 COMPLETE")
