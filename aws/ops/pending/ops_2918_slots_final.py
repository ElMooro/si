#!/usr/bin/env python3
import json,sys,time,urllib.request
sys.path.insert(0,"aws/ops")
from ops_report import report
def get(u,to=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=to)
        return r.getcode(),r.read()
    except Exception: return None,b""
def deep(o,k):
    if not isinstance(o,dict): return None
    if isinstance(o.get(k),(int,float)): return o[k]
    for v in o.values():
        r=deep(v,k)
        if r is not None: return r
    return None
out={}
with report("2918") as r:
    for att in range(10):
        c,b=get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        idx=b.decode("utf-8","replace")
        if "cluster_alerts" in idx and "deep(d" in idx: break
        time.sleep(18)
    out["page_attempt"]=att+1; r.ok(f"patched page live attempt {att+1}")
    c,b=get(f"https://justhodl.ai/data/khalid-metrics.json?t={int(time.time())}")
    ka=json.loads(b.decode()) if c==200 else {}
    out["ka"]={"http":c,"khalid_index":deep(ka,"khalid_index"),"wow":deep(ka,"khalid_index_wow")}
    (r.ok if out["ka"]["khalid_index"] is not None else r.fail)(f"KA deep: {out['ka']}")
    c,b=get("https://justhodl-dashboard-live.s3.amazonaws.com/cot/extremes/current.json")
    cot=json.loads(b.decode()) if c==200 else {}
    n=len(cot.get("cluster_alerts",[])) if isinstance(cot.get("cluster_alerts"),list) else None
    out["cot"]={"http":c,"n_crowded":n}
    (r.ok if n is not None else r.fail)(f"COT cluster_alerts: {n}")
    json.dump(out,open("aws/ops/reports/2918.json","w"),indent=2,default=str)
print("DONE 2918"); sys.exit(0)
