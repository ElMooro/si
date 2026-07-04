"""ops 2820 — verify nowcast surfacing on master-rank + master-board pages + feeds."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R={"ops":2820,"ts":datetime.now(timezone.utc).isoformat()}
def get(u,t=25):
    req=urllib.request.Request(u+("&" if "?" in u else "?")+"cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status,r.read().decode("utf-8","ignore")
# pages (retry for Pages propagation)
pages={"master-rank.html":["nowcastRegime","tc-nc","nowcast_regime_mult","Fed nowcast"],
       "master-board.html":["nc-tilt","nowcast_regime","NCREG","Fed nowcast"]}
res={}
for _ in range(5):
    time.sleep(20); allok=True
    for pg,marks in pages.items():
        try:
            st,b=get("https://justhodl.ai/"+pg); hits={m:(m in b) for m in marks}
            res[pg]={"status":st,"markers":hits,"all":all(hits.values())}
            if not (st==200 and all(hits.values())): allok=False
        except Exception as e: res[pg]="err "+str(e)[:60]; allok=False
    if allok: break
R["pages"]=res
# feeds carry the data
try:
    st,b=get("https://justhodl.ai/data/master-ranker.json"); d=json.loads(b)
    tilted=[x.get("ticker") for x in (d.get("top_tickers") or []) if x.get("nowcast_regime_mult") not in (None,1.0)]
    R["master_ranker_feed"]={"regime":(d.get("nowcast_regime") or {}).get("regime"),"n_tilted":len(tilted),"sample":tilted[:5]}
except Exception as e: R["master_ranker_feed"]="err "+str(e)[:60]
try:
    st,b=get("https://justhodl.ai/data/best-setups.json"); d=json.loads(b)
    ts=d.get("top_setups") or []; tl=[x.get("ticker") for x in ts if x.get("nowcast_regime_mult") not in (None,1.0)]
    R["best_setups_feed"]={"regime":(d.get("nowcast_regime") or {}).get("regime"),"n_setups":len(ts),"n_tilted":len(tl),"sample":tl[:5]}
except Exception as e: R["best_setups_feed"]="err "+str(e)[:60]
pg_ok=all(isinstance(v,dict) and v.get("all") for v in res.values())
R["status"]="NOWCAST SURFACED ON BOTH PAGES" if pg_ok else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2820_verify_pages.json","w"),indent=1,default=str)
print("OPS 2820 COMPLETE")
