import json, urllib.request
out={}
def get(u):
    try:
        req=urllib.request.Request(u+"?t=999",headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return "ERR:"+str(e)[:50]
d=get("https://justhodl-data-proxy.raafouis.workers.dev/data/funding-plumbing.json")
try:
    j=json.loads(d); out["worker_data"]={"regime":j.get("regime"),"bs":j.get("balance_sheet_direction"),"score":j.get("plumbing_stress_score"),"qt_not_qe":j.get("qt_ended_not_qe")}
except: out["worker_data"]=d[:80]
open("aws/ops/reports/1335_w.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
