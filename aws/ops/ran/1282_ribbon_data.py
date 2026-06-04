"""1282 — confirm regime-ribbon data + the ribbon JS is served."""
import json, urllib.request
out={}
def get(url):
    try:
        req=urllib.request.Request(url+("?t=1" if "?" not in url else ""),headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=15) as r: return r.status, r.read().decode()[:120]
    except urllib.error.HTTPError as e: return e.code, "err"
    except Exception as e: return None, str(e)[:80]
# ribbon data via worker
s,b=get("https://justhodl-data-proxy.raafouis.workers.dev/data/bond-vol.json")
out["bond_vol_via_worker"]={"status":s,"preview":b}
# ribbon JS on the site
s2,b2=get("https://justhodl.ai/regime-ribbon.js")
out["ribbon_js"]={"status":s2,"is_js":"RegimeRibbon" in (b2 or "")}
# parse the regime out
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/bond-vol.json?t=1",headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"})
    d=json.loads(urllib.request.urlopen(req,timeout=15).read())
    out["regime"]=d.get("regime"); out["z"]=d.get("composite_z_score"); out["posture"]=d.get("risk_posture")
    out["ts_signal"]=(d.get("term_structure") or {}).get("signal")
except Exception as e: out["parse_err"]=str(e)[:100]
open("aws/ops/reports/1282_ribbon.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
