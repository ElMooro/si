"""ops 2605 — inspect real master-ranker.json schema; confirm buyback contributes to rank."""
import json, urllib.request, time
def get(p):
    return json.loads(urllib.request.urlopen(urllib.request.Request(
        f"https://justhodl-data-proxy.raafouis.workers.dev/{p}?t={int(time.time())}",
        headers={"User-Agent":"M"}),timeout=25).read())
j=get("data/master-ranker.json")
print("TOP-LEVEL KEYS:", list(j.keys()))
# find list-valued keys (the ranked rows live in one of them)
for k,v in j.items():
    if isinstance(v,list) and v and isinstance(v[0],dict):
        print(f"  list '{k}': {len(v)} rows; row[0] keys: {list(v[0].keys())[:14]}")
# locate the main ranked list = the one whose rows have a ticker + a systems/score field
cand=None
for k,v in j.items():
    if isinstance(v,list) and v and isinstance(v[0],dict) and (v[0].get('ticker') or v[0].get('symbol')):
        cand=k; break
print("\nMAIN RANKED LIST KEY:", cand)
if cand:
    rows=j[cand]
    r0=rows[0]
    print("row[0] full sample:", json.dumps({kk:r0.get(kk) for kk in list(r0.keys())[:16]}, default=str)[:600])
    # how are systems stored per row?
    syskey=None
    for kk in ("systems","systems_dict","contributions","signals","components"):
        if kk in r0: syskey=kk; break
    print("systems field on a row:", syskey, "->", type(r0.get(syskey)).__name__ if syskey else None)
    # count rows where buyback contributes
    def has_bb(r):
        s=r.get(syskey) if syskey else None
        if isinstance(s,dict) and "buyback" in s: return True
        if isinstance(s,list) and any((isinstance(x,dict) and x.get("system")=="buyback") for x in s): return True
        return "buyback" in str(r.get("rationale","")).lower() or "net shrinker" in str(r.get("rationale","")).lower()
    bb=[r for r in rows if has_bb(r)]
    print(f"\nranked rows with buyback contributing: {len(bb)} / {len(rows)}")
    for r in bb[:10]:
        sc=r.get("conviction") or r.get("score") or r.get("master_score") or r.get("composite_score")
        print(f"  {r.get('ticker') or r.get('symbol')}: score={sc} | {str(r.get('rationale',''))[:95]}")
print("DONE 2605")
