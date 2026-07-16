"""ops 3405 — final: confirm the long-history file is served by the CF proxy (what the chart
fetches) so the 1990→now chart actually loads."""
import json, urllib.request
from ops_report import report
def get(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return None, str(e)[:60]
with report("3405_verify_lh_live") as r:
    code,body=get("https://justhodl-data-proxy.raafouis.workers.dev/data/global-sovereign-longhistory.json")
    if code==200:
        d=json.loads(body)
        h=d.get("history",[])
        r.ok(f"proxy serves long-history — {len(h)} points, {d.get('start')} → {d.get('end')}, v{d.get('version')}")
        r.log(f"  basis: {d.get('basis','')[:100]}")
        # percentile of current live reading
        gs=json.loads(get("https://justhodl-data-proxy.raafouis.workers.dev/data/global-sovereign.json")[1])
        now=gs.get("eurodollar_hub_stress_0_100")
        vals=sorted(p["stress"] for p in h)
        pct=round(sum(1 for v in vals if v<=now)/len(vals)*100) if now else None
        r.log(f"  current reading {now} = {pct}th percentile of 36-year history (range {vals[0]:.0f}-{vals[-1]:.0f})")
    else:
        r.fail(f"proxy status={code}: {body[:80]}")
