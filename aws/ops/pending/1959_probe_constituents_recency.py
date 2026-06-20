"""1959 — can ETF Global Constituents return a CURRENT holdings snapshot?
Try recency sorts/filters. If newest achievable is still years old -> product is
stale at this tier and should be cancelled. If current -> build look-through."""
import os, json, urllib.request, urllib.error
KEY=os.environ.get("MASSIVE_API_KEY",""); BASE="https://api.polygon.io"
def get(path):
    sep="&" if "?" in path else "?"; url=f"{BASE}{path}{sep}apiKey={KEY}"
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req,timeout=30) as r: return r.status, json.loads(r.read().decode()), url.replace(KEY,"***")
    except urllib.error.HTTPError as e: return e.code, {"_err":e.read().decode()[:200]}, url.replace(KEY,"***")
    except Exception as e: return None, {"_exc":f"{type(e).__name__}:{e}"}, ""
def summ(j):
    res=j.get("results") or []
    if not res: return f"n=0 {json.dumps(j,default=str)[:160]}"
    effs=[r.get("effective_date") for r in res if r.get("effective_date")]
    procs=[r.get("processed_date") for r in res if r.get("processed_date")]
    real=[r.get("constituent_ticker") for r in res[:10]]
    return (f"n={len(res)} max_eff={max(effs) if effs else '?'} max_proc={max(procs) if procs else '?'} "
            f"top={real[:6]}")
variants=[
 "/etf-global/v1/constituents?composite_ticker=SPY&order=desc&sort=effective_date&limit=50",
 "/etf-global/v1/constituents?composite_ticker=SPY&order=desc&sort=processed_date&limit=50",
 "/etf-global/v1/constituents?composite_ticker=SPY&effective_date.gte=2026-01-01&limit=50",
 "/etf-global/v1/constituents?composite_ticker=SPY&processed_date.gte=2026-05-01&limit=50",
 "/etf-global/v1/constituents?composite_ticker=SPY&sort=effective_date.desc&limit=50",
]
for p in variants:
    code,j,u=get(p)
    print(f"\nHTTP {code} {u.split('apiKey')[0]}")
    print("   ",summ(j))
# follow next_url chain a few hops to see if newest data lives at the end
print("\n--- walking pagination to find newest (up to 6 hops) ---")
code,j,u=get("/etf-global/v1/constituents?composite_ticker=SPY&limit=1000")
hop=0; newest="0000"
while j.get("results"):
    effs=[r.get("effective_date") for r in j["results"] if r.get("effective_date")]
    if effs: newest=max(newest,max(effs))
    nxt=j.get("next_url")
    hop+=1
    if not nxt or hop>=6: break
    try:
        req=urllib.request.Request(nxt+f"&apiKey={KEY}",headers={"User-Agent":"x"})
        with urllib.request.urlopen(req,timeout=30) as r: j=json.loads(r.read().decode())
    except Exception as e: print("   pag err",e); break
print(f"   after {hop} hops, newest effective_date seen = {newest}")
print("\nDONE 1959")
