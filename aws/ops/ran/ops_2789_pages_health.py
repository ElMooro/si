"""ops 2789 — confirm justhodl.ai + Options Desk healthy after self-heal deploy work."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2789,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read()
for name,u in [("home","https://justhodl.ai/"),("options","https://justhodl.ai/options.html")]:
    try:
        st,b=get(u); R[name]={"status":st,"bytes":len(b),"has_body":b"</html>" in b or b"</body>" in b}
        print("%s: HTTP %d, %d bytes, ok=%s"%(name,st,len(b),R[name]["has_body"]))
    except Exception as e:
        R[name]={"err":str(e)[:80]}; print("%s ERR %s"%(name,str(e)[:80]))
R["status"]="PAGES HEALTHY" if all(isinstance(R.get(k),dict) and R[k].get("has_body") for k in ("home","options")) else "CHECK"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2789_pages_health.json","w"),indent=1,default=str)
print("OPS 2789:",R["status"])
