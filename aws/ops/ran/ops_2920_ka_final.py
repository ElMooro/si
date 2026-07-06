#!/usr/bin/env python3
import json,sys,time,urllib.request
sys.path.insert(0,"aws/ops")
from ops_report import report
def get(u,to=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=to)
        return r.getcode(),r.read()
    except Exception: return None,b""
ok=False
with report("2920") as r:
    for att in range(10):
        c,b=get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.4"' in b.decode("utf-8","replace"): break
        time.sleep(18)
    r.ok(f"v1.2.4 live attempt {att+1}")
    c,b=get(f"https://justhodl.ai/data/report.json?t={int(time.time())}")
    rep=json.loads(b.decode()) if c==200 else {}
    ko=rep.get("khalid_index")
    keys=sorted(ko.keys()) if isinstance(ko,dict) else type(ko).__name__
    kv=None
    if isinstance(ko,(int,float)): kv=ko
    elif isinstance(ko,dict):
        for k in ("value","index","score","index_0_100","composite","level_score"):
            if isinstance(ko.get(k),(int,float)): kv=ko[k]; break
        if kv is None:
            for v in ko.values():
                if isinstance(v,(int,float)): kv=v; break
    ok = kv is not None
    (r.ok if ok else r.fail)(f"KA dict keys={keys} -> value={kv}")
    json.dump({"attempt":att+1,"ka_keys":keys,"ka_value":kv},open("aws/ops/reports/2920.json","w"),default=str)
print("DONE 2920","PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
