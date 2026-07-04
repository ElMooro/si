"""ops 2818 — verify nowcast-desk page + feed live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2818,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read()
okp=False
for a in range(5):
    time.sleep(22)
    try:
        st,b=get("https://justhodl.ai/nowcast-desk.html"); okp=(st==200 and b"Nowcast Desk" in b and b"nowcast-desk.json" in b)
        R["page"]={"status":st,"reads_feed":b"nowcast-desk.json" in b}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:50]
try:
    st,b=get("https://justhodl.ai/data/nowcast-desk.json"); d=json.loads(b)
    R["feed"]={"status":st,"blocks_live":d.get("_blocks_live"),"gdpnow":(d.get("gdp_nowcast") or {}).get("value"),
        "underlying_infl":((d.get("underlying_inflation") or {}).get("composite") or {}).get("underlying_inflation_pct"),
        "regime":(d.get("nowcast_quadrant") or {}).get("regime")}
except Exception as e: R["feed"]="err "+str(e)[:50]
R["status"]="NOWCAST PAGE+FEED LIVE" if okp else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2818_nowcast_page.json","w"),indent=1,default=str)
print("OPS 2818 COMPLETE")
