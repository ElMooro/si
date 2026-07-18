"""ops 3438 — census #3: mock-data purge verification over LIVE pages.
Per page: no live execute-api base (dead gateways blanked), OPS3438
real-feed override present, real feed itself reachable. chart-pro/brain/
journal/dep-graph audited clean (Math.random = ids/layout only)."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3438)"}
CHECKS={
 "ofr.html":      {"absent":["https://6nl5fzfus7.execute-api"], "present":["OPS3438","Short-Term Funding"]},
 "ny-fed.html":   {"absent":["https://jc6ripzwk1.execute-api"], "present":["OPS3438","nyfed-primary-dealer.json"]},
 "treasury-auctions.html":{"absent":["https://klehdyiwrl.execute-api"], "present":["OPS3438","treasury-auctions.json"]},
 "ai_predictions.html":{"absent":["0.6 + Math.random()"], "present":["OPS3438","wl-predictions.json"]},
}
FEEDS=["data/nyfed-primary-dealer.json","data/treasury-auctions.json","data/wl-predictions.json"]
def get(url,t=25):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r:
        return r.read().decode("utf-8","replace")
with report("3438_mock_purge") as rep:
    rep.heading("ops 3438 — mock purge")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    dl=time.time()+420; res={}
    while time.time()<dl:
        res={}
        for pg,c in CHECKS.items():
            try:
                b=get(f"https://justhodl.ai/{pg}?t={int(time.time())}")
                miss=[m for m in c["present"] if m not in b]
                live=[a for a in c["absent"] if a in b]
                res[pg]=(not miss and not live, f"miss={miss} live_dead_api={live}")
            except Exception as e:
                res[pg]=(False,str(e)[:60])
        if all(v[0] for v in res.values()): break
        time.sleep(20)
    for pg,(ok,d) in res.items():
        gate(f"G_{pg.replace('.html','').replace('-','_')}", ok, d)
    fok=[]
    for k in FEEDS:
        try:
            j=json.loads(get(f"https://justhodl.ai/{k}?t={int(time.time())}",20))
            fok.append(bool(j))
        except Exception:
            try:
                j=json.loads(get(f"https://justhodl-dashboard-live.s3.amazonaws.com/{k}",20)); fok.append(bool(j))
            except Exception: fok.append(False)
    gate("G_feeds_reachable", all(fok), f"{sum(fok)}/{len(FEEDS)}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3438.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
