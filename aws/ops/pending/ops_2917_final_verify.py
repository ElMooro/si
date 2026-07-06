#!/usr/bin/env python3
"""ops 2917 — close the audit: hooks live, registry fresh w/ 3 new engines,
KA value live, COT extremes real keys + robust count."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report
def get(u,to=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=to)
        return r.getcode(), r.read()
    except Exception: return None,b""
out={}
with report("2917") as r:
    ok=False
    for att in range(10):
        c,b=get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        idx=b.decode("utf-8","replace")
        hooks={h:(h in idx) for h in ['JH_V="v1.2.1"',"data/khalid-metrics.json","cot/extremes/current.json","AMBER TERMINAL"]}
        if all(hooks.values()): ok=True; break
        time.sleep(18)
    out["hooks"]=hooks; out["hook_attempt"]=att+1
    (r.ok if ok else r.fail)(f"index hooks attempt {att+1}: {hooks}")
    c,b=get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg=json.loads(b.decode()) if c==200 else {}
    n=reg.get("count") or len(reg.get("engines",{}))
    out["registry"]={"n":n,"gen":reg.get("generated_at") or reg.get("generated")}
    blob=json.dumps(reg)[:500000]
    newin={fn:(fn.replace("justhodl-","") in blob) for fn in
           ("justhodl-market-tape","justhodl-investor-lenses","justhodl-technical-overlays")}
    out["new_in_registry"]=newin
    (r.ok if all(newin.values()) else r.fail)(f"registry n={n} gen={out['registry']['gen']} new={newin}")
    c,b=get(f"https://justhodl.ai/data/khalid-metrics.json?t={int(time.time())}")
    ka=json.loads(b.decode()) if c==200 else {}
    out["ka"]={"http":c,"khalid_index":ka.get("khalid_index"),"wow":ka.get("khalid_index_wow") or ka.get("wow")}
    (r.ok if c==200 and ka.get("khalid_index") is not None else r.fail)(f"KA {c} idx={ka.get('khalid_index')}")
    c,b=get("https://justhodl-dashboard-live.s3.amazonaws.com/cot/extremes/current.json")
    cot=json.loads(b.decode()) if c==200 else {}
    keys=sorted(cot.keys()) if isinstance(cot,dict) else []
    n2=0
    for k,v in (cot.items() if isinstance(cot,dict) else []):
        if isinstance(v,list) and __import__("re").search(r"crowd|extreme",k,2): n2+=len(v)
    out["cot"]={"http":c,"keys":keys[:8],"n_crowded":n2}
    (r.ok if c==200 and n2>0 else r.fail)(f"COT {c} keys={keys[:6]} n={n2}")
    json.dump(out,open("aws/ops/reports/2917.json","w"),indent=2,default=str)
    r.ok("report -> 2917.json")
print("DONE 2917"); sys.exit(0)
