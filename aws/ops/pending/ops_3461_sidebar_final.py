"""ops 3461 — sidebar categories final: extractor for the REAL shape
(categories = list of {name, count, pages:[{href,title}]})."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3461)"}
def get(url,t=25):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r:
        return r.read().decode("utf-8","replace")
def cat_of(m,href):
    for c in (m.get("categories") or []):
        for p in (c.get("pages") or []):
            if p.get("href")==href:
                return c.get("name")
    return None
with report("3461_sidebar_final") as rep:
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    want={"/alpha-families.html":"Portfolio & Execution",
          "/proven-portfolio.html":"Portfolio & Execution",
          "/short-book.html":"Portfolio & Execution",
          "/political.html":"Research & Tools"}
    ok1,det=False,{}
    dl=time.time()+240
    while time.time()<dl:
        try:
            m=json.loads(get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}"))
            det={"generated_at":m.get("generated_at"),
                 "ref_primary_dealers":cat_of(m,"/primary-dealers.html")}
            for h in want: det[h]=cat_of(m,h)
            if all(det[h]==c for h,c in want.items()): ok1=True; break
        except Exception as e: det={"err":str(e)[:60]}
        time.sleep(15)
    gate("G1_categories", ok1, json.dumps(det))
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3461.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
