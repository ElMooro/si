"""ops 2808 — verify macro-leads page + feed live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2808,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read()
# page live?
okp=False
for a in range(5):
    time.sleep(20)
    try:
        st,b=get("https://justhodl.ai/macro-leads.html"); okp=(st==200 and b"Macro Leads" in b)
        R["page"]={"status":st,"has_title":b"Macro Leads" in b,"reads_feed":b"macro-leads.json" in b}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:50]
# feed live + fresh?
try:
    st,b=get("https://justhodl.ai/data/macro-leads.json"); d=json.loads(b)
    R["feed"]={"status":st,"populated":d.get("_populated"),"generated_at":d.get("generated_at"),
        "copper_gold":(d.get("copper_gold_silver") or {}).get("copper_gold",{}).get("ratio") if isinstance((d.get("copper_gold_silver") or {}).get("copper_gold"),dict) else None,
        "rate_cut":(d.get("rate_cut_diffusion") or {}).get("regime"),
        "gpr":(d.get("geopolitical_risk") or {}).get("gpr"),
        "truck_yoy":(d.get("heavy_truck_sales") or {}).get("yoy_pct")}
except Exception as e: R["feed"]="err "+str(e)[:60]
R["status"]="MACRO-LEADS LIVE" if okp and isinstance(R.get("feed"),dict) else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2808_verify_edge.json","w"),indent=1,default=str)
print("OPS 2808 COMPLETE")
