"""ops 2815 — verify eia.html live at edge + reads feed."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2815,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read()
okp=False
for a in range(5):
    time.sleep(20)
    try:
        st,b=get("https://justhodl.ai/eia.html"); okp=(st==200 and b"eia-energy.json" in b and b"EIA Energy" in b)
        R["page"]={"status":st,"reads_feed":b"eia-energy.json" in b}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:50]
try:
    st,b=get("https://justhodl.ai/data/eia-energy.json"); d=json.loads(b)
    R["feed"]={"status":st,"metrics_ok":d.get("metrics_ok"),"wti":(d.get("all_series") or {}).get("WTIPUUS",{}).get("data",{}).get("value")}
except Exception as e: R["feed"]="err "+str(e)[:50]
R["status"]="EIA PAGE+FEED LIVE" if okp else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2815_page.json","w"),indent=1,default=str)
print("OPS 2815 COMPLETE")
