"""ops 3459 — Alpha Lab page + sidebar accessibility proof. Gates:
G1 page live with all six family sections · G2 nav-manifest.json (regen by
pages.yml on this push) contains alpha-families + proven-portfolio +
short-book + political with pinned categories · G3 the six family feeds all
reachable so the page renders real data."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3459)"}
def get(url,t=25):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r:
        return r.read().decode("utf-8","replace")
with report("3459_alpha_lab_sidebar") as rep:
    rep.heading("ops 3459 — alpha lab + sidebar")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:300]; print(line); rep.log(line)
        if not ok: fails.append(n)
    need=["Alpha Lab","Congress Alpha","Cannibals + Insiders","Credit-Plumbing Composite",
          "Auction Tail","Stealth Flow","Fade Desk"]
    ok1,missing=False,need; dl=time.time()+420
    while time.time()<dl:
        try:
            b=get(f"https://justhodl.ai/alpha-families.html?t={int(time.time())}")
            missing=[m for m in need if m not in b]
            if not missing: ok1=True; break
        except Exception: pass
        time.sleep(15)
    gate("G1_page_live", ok1, f"missing={missing}")
    want={"/alpha-families.html":"Portfolio & Execution",
          "/proven-portfolio.html":"Portfolio & Execution",
          "/short-book.html":"Portfolio & Execution",
          "/political.html":"Research & Tools"}
    ok2,det=False,{}; dl=time.time()+300
    while time.time()<dl:
        try:
            m=json.loads(get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}"))
            blob=json.dumps(m)
            det={}
            for href,cat in want.items():
                det[href]="in" if href in blob else "MISSING"
                if href in blob:
                    cats=m.get("categories") or m
                    if isinstance(cats,dict):
                        hit=[c for c,items in cats.items() if isinstance(items,list)
                             and any(href in json.dumps(i) for i in items)]
                        det[href]=hit[0] if hit else "uncategorized"
            if all(v==want[k] for k,v in det.items()): ok2=True; break
        except Exception as e: det={"err":str(e)[:60]}
        time.sleep(15)
    gate("G2_sidebar_manifest", ok2, json.dumps(det))
    feeds=["data/congress-alpha.json","data/cannibals.json","data/credit-composite.json",
           "data/auction-tail.json","data/stealth-flow.json","data/alpha-triage.json"]
    okf=[]
    for k in feeds:
        try:
            j=json.loads(get(f"https://justhodl.ai/{k}?t={int(time.time())}",20)); okf.append(bool(j))
        except Exception:
            try:
                j=json.loads(get(f"https://justhodl-dashboard-live.s3.amazonaws.com/{k}",20)); okf.append(bool(j))
            except Exception: okf.append(False)
    gate("G3_feeds", all(okf), f"{sum(okf)}/{len(feeds)} reachable")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3459.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
