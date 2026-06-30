"""ops 2579 — probe live shapes of every feed the attention-confluence engine will fuse."""
import urllib.request, json, time
PX="https://justhodl-data-proxy.raafouis.workers.dev/data"
def get(f):
    try:
        r=urllib.request.urlopen(urllib.request.Request(f"{PX}/{f}?t={int(time.time())}",headers={"User-Agent":"Mozilla/5.0"}),timeout=25)
        return json.loads(r.read().decode("utf-8","ignore"))
    except Exception as e: return {"__err__":str(e)[:60]}
def shape(j, depth=0):
    if isinstance(j,dict):
        if "__err__" in j: return "ERR "+j["__err__"]
        out={}
        for k,v in list(j.items())[:14]:
            if isinstance(v,list):
                out[k]=f"[list {len(v)}]"+(" e0keys="+",".join(list(v[0].keys())[:12]) if v and isinstance(v[0],dict) else "")
            elif isinstance(v,dict):
                out[k]="{dict "+",".join(list(v.keys())[:8])+"}"
            else:
                out[k]=repr(v)[:40]
        return out
    if isinstance(j,list):
        return f"[toplist {len(j)}]"+(" e0keys="+",".join(list(j[0].keys())[:14]) if j and isinstance(j[0],dict) else "")
    return repr(j)[:60]
FEEDS=["attention-signals.json","insider-clusters.json","insider-buyback-confluence.json","options-flow.json",
       "options-gamma.json","13f-positions.json","smart-money-clusters.json","dark-pool.json","political-stocks.json",
       "gdelt-news.json","short-interest.json","squeeze-pretrigger.json","rating-change-cluster.json","patent-velocity.json"]
for f in FEEDS:
    j=get(f)
    print(f"\n=== {f} ===")
    s=shape(j)
    if isinstance(s,dict):
        for k,v in s.items(): print(f"   {k}: {v}")
    else: print("  ",s)
print("\nDONE 2579")
