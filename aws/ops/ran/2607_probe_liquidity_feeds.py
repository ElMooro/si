"""ops 2607 — probe liquidity-engine feeds + FRED series for inflection rebuild."""
import urllib.request, json, time
PX="https://justhodl-data-proxy.raafouis.workers.dev"
FRED="2f057499936072679d8843d7fce99989"
def get(p):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(f"{PX}/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=20).read())
    except Exception as e: return {"__err__":str(e)[:50]}
feeds=["funding-plumbing","eurodollar-stress","eurodollar-plumbing","move-index","global-liquidity",
       "china-liquidity","credit-stress","plumbing-stress","risk-regime","liquidity-inflection","global-stress"]
for f in feeds:
    j=get(f"data/{f}.json")
    if isinstance(j,dict) and "__err__" not in j:
        keys=list(j.keys())
        # headline numeric/score fields
        hot={k:v for k,v in j.items() if any(w in k.lower() for w in ["score","state","regime","health","stress","z","level","yoy","impulse","direction","posture","signal","bps","spread","basis","net_liq"]) and not isinstance(v,(dict,list))}
        print(f"\n### {f} ###")
        print("  keys:", keys[:16])
        print("  headline:", dict(list(hot.items())[:10]))
    else:
        print(f"\n### {f} ### MISSING/err: {j.get('__err__') if isinstance(j,dict) else j}")
# FRED series to add
print("\n##### FRED series check #####")
def fred(sid):
    try:
        u=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=3"
        j=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M"}),timeout=20).read())
        obs=[(o["date"],o["value"]) for o in j.get("observations",[])]
        return obs
    except Exception as e: return [("err",str(e)[:50])]
for sid in ["WRESBAL","SOFR","IORB","RRPONTSYD","WALCL","WTREGEN","NFCI","WALCL","BAMLH0A0HYM2","DTWEXBGS"]:
    print(f"  {sid}: {fred(sid)}")
print("DONE 2607")
