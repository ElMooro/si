#!/usr/bin/env python3
import json,sys,time,urllib.request
sys.path.insert(0,"aws/ops")
from ops_report import report
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20)
        return r.getcode(),r.read().decode("utf-8","replace")
    except Exception: return None,""
ok=False
with report("2915") as r:
    for a in range(9):
        c,b=get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        clean = c==200 and "sv.indexOf" in b and "[object Object]" not in b.replace('indexOf("[object")',"")
        r.log(f"attempt {a+1}: http={c} hardened={'sv.indexOf' in b} objectleak={'[object Object]' in b.replace(chr(39)+'[object'+chr(39),'')}")
        if clean: ok=True; r.ok(f"clean on attempt {a+1}"); break
        time.sleep(20)
    json.dump({"clean":ok},open("aws/ops/reports/2915.json","w"))
print("DONE 2915"); sys.exit(0 if ok else 1)
