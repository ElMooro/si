"""ops 3460 — sidebar categories, shape-adaptive close. Handles manifest as
{cat:[items]}, {categories:{...}}, {items:[...]}, or flat list of objects."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3460)"}
def get(url,t=25):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r:
        return r.read().decode("utf-8","replace")
def cat_of(m,href):
    if isinstance(m,dict):
        pools=m.get("categories") if isinstance(m.get("categories"),dict) else \
              (m if all(isinstance(v,list) for v in m.values()) else None)
        if isinstance(pools,dict):
            for c,items in pools.items():
                for it in items:
                    if href in json.dumps(it): return c
        items=m.get("items") or m.get("pages") or []
        for it in items if isinstance(items,list) else []:
            if isinstance(it,dict) and href in json.dumps(it):
                return it.get("category") or it.get("cat") or it.get("section")
    if isinstance(m,list):
        for it in m:
            if isinstance(it,dict) and href in json.dumps(it):
                return it.get("category") or it.get("cat") or it.get("section")
    return None
with report("3460_sidebar_close") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    m=json.loads(get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}"))
    shape=(type(m).__name__)+":"+(",".join(list(m.keys())[:5]) if isinstance(m,dict) else f"n={len(m)}")
    print("shape:",shape); rep.log("shape: "+shape)
    ref=cat_of(m,"/primary-dealers.html")
    print("ref primary-dealers →",ref); rep.log(f"ref primary-dealers -> {ref}")
    want={"/alpha-families.html":"Portfolio & Execution",
          "/proven-portfolio.html":"Portfolio & Execution",
          "/short-book.html":"Portfolio & Execution",
          "/political.html":"Research & Tools"}
    det={h:cat_of(m,h) for h in want}
    gate("G1_categories", all(det[h]==c for h,c in want.items()),
         json.dumps({"shape":shape,"ref":ref,**det}))
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3460.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
