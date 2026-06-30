"""ops 2612 — audit feeds for dollar-shortage/basis, fails, swaps, cross-asset flows."""
import urllib.request, json, time
PX="https://justhodl-data-proxy.raafouis.workers.dev"; FRED="2f057499936072679d8843d7fce99989"
def gp(p):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(f"{PX}/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=18).read())
    except Exception as e: return {"__err__":str(e)[:40]}
def keys(j,n=20): return list(j.keys())[:n] if isinstance(j,dict) else type(j).__name__
for f in ["settlement-fails","repo-lending","etf-flows","capital-flow","dealer-survey","hot-money"]:
    j=gp(f"data/{f}.json")
    print(f"\n### {f} ###")
    if isinstance(j,dict) and "__err__" not in j:
        print("  keys:", keys(j))
        hot={k:v for k,v in j.items() if not isinstance(v,(dict,list)) and any(w in k.lower() for w in ["fail","deliver","receive","score","regime","stress","level","trend","signal","net","flow","direction","state"])}
        print("  headline:", dict(list(hot.items())[:10]))
    else: print("  MISS:", j)
# eurodollar-stress signals detail (look for xccy basis / FRA-OIS / CP)
eds=gp("data/eurodollar-stress.json")
print("\n### eurodollar-stress.signals (basis/FRA-OIS?) ###")
sigs=eds.get("signals") if isinstance(eds,dict) else None
if isinstance(sigs,dict): print("  signal keys:", list(sigs.keys()))
elif isinstance(sigs,list): print("  signals:", [s.get("name") or s.get("signal") for s in sigs][:14])
# eurodollar-plumbing layers (basis/fails/swaps?)
edp=gp("data/eurodollar-plumbing.json")
print("\n### eurodollar-plumbing.layers ###")
lay=edp.get("layers") if isinstance(edp,dict) else None
if isinstance(lay,list): print("  layers:", [ (l.get("layer") or l.get("name"), l.get("metric") or l.get("signal")) for l in lay][:12])
elif isinstance(lay,dict): print("  layer keys:", list(lay.keys()))
print("  massive_fx:", (edp.get("massive_fx") if isinstance(edp,dict) else None))
# FRED facility/swap series
def fred(sid):
    try:
        u=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=2"
        return [(o["date"],o["value"]) for o in json.loads(urllib.request.urlopen(u,timeout=15).read()).get("observations",[])]
    except Exception as e: return [("err",str(e)[:40])]
print("\n##### FRED facility/swap series #####")
for sid in ["SWPT","WLCFLPCL","DISCBORR","H41RESPPALDKNWW","COMPOUT","WLEMUINDXD","DGS10","DSWP10"]:
    print(f"  {sid}: {fred(sid)}")
print("DONE 2612")
