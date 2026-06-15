"""Confirm the CF data-proxy serves the research feed the page fetches."""
import json, urllib.request
def get(u):
    try:
        r=urllib.request.Request(u,headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(r,timeout=20) as x: return x.status, x.read()
    except Exception as e: return None, str(e)[:100].encode()
for name,u in [("research","https://justhodl-data-proxy.raafouis.workers.dev/data/bottleneck-boom-research.json"),
               ("boom","https://justhodl-data-proxy.raafouis.workers.dev/data/bottleneck-boom.json")]:
    st,body=get(u)
    if st==200:
        try:
            d=json.loads(body)
            bt=d.get("by_ticker") or {}
            extra=f"tickers={len(bt)} clean_theses="+str(sum(1 for v in bt.values() if v.get('thesis') and 'Draft' not in v['thesis'])) if bt else f"ranks={len(d.get('ranks',[]))}"
            print(f"{name}: HTTP 200 via proxy ✓  {extra}")
        except Exception as e:
            print(f"{name}: 200 but parse err {e}")
    else:
        print(f"{name}: proxy returned {st} {body[:80]}")
