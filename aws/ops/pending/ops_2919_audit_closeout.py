#!/usr/bin/env python3
import json,sys,time,urllib.request
sys.path.insert(0,"aws/ops")
from ops_report import report
def get(u,to=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=to)
        return r.getcode(),r.read()
    except Exception: return None,b""
ok_all=True; out={}
with report("2919") as r:
    for att in range(10):
        c,b=get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        idx=b.decode("utf-8","replace")
        if 'JH_V="v1.2.3"' in idx and "data/report.json" in idx: break
        time.sleep(18)
    live = 'JH_V="v1.2.3"' in idx
    out["page_attempt"]=att+1; ok_all&=live
    (r.ok if live else r.fail)(f"v1.2.3 page live attempt {att+1}")
    c,b=get(f"https://justhodl.ai/data/report.json?t={int(time.time())}")
    rep=json.loads(b.decode()) if c==200 else {}
    ko=rep.get("khalid_index")
    kv=ko if isinstance(ko,(int,float)) else (ko or {}).get("value") if isinstance(ko,dict) else None
    kw=(ko or {}).get("wow") if isinstance(ko,dict) else None
    out["ka"]={"http":c,"shape":type(ko).__name__,"value":kv,"wow":kw}; ok_all&=(kv is not None)
    (r.ok if kv is not None else r.fail)(f"KA report.json: {out['ka']}")
    c,b=get("https://justhodl-dashboard-live.s3.amazonaws.com/cot/extremes/current.json")
    cot=json.loads(b.decode()) if c==200 else {}
    n=len(cot.get("cluster_alerts",[])) if isinstance(cot.get("cluster_alerts"),list) else None
    out["cot"]={"http":c,"n_crowded":n}; ok_all&=isinstance(n,int)
    (r.ok if isinstance(n,int) else r.fail)(f"COT cluster_alerts: {n}")
    c,b=get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg=json.loads(b.decode()) if c==200 else {}
    nreg=reg.get("count") or len(reg.get("engines",{}))
    blob=json.dumps(reg)[:500000]
    newin={f:(f.replace("justhodl-","") in blob) for f in ("justhodl-market-tape","justhodl-investor-lenses","justhodl-technical-overlays")}
    fresh=all(newin.values()); ok_all&=fresh
    out["registry"]={"n":nreg,"gen":reg.get("generated_at") or reg.get("generated"),"new":newin}
    (r.ok if fresh else r.fail)(f"registry n={nreg} gen={out['registry']['gen']} new={newin}")
    json.dump(out,open("aws/ops/reports/2919.json","w"),indent=2,default=str)
print("DONE 2919", "PASS" if ok_all else "FAIL")
sys.exit(0 if ok_all else 1)
