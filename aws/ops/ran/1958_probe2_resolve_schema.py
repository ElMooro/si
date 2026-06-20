"""1958 — resolve the 2 open schema questions before building:
(1) ETF constituents via correct ?composite_ticker= param (SPY/QQQ/XLK), check freshness
(2) Benzinga earnings REPORTED rows: do they carry actual EPS + surprise?"""
import os, json, urllib.request, urllib.error, datetime
KEY=os.environ.get("MASSIVE_API_KEY",""); BASE="https://api.polygon.io"
def get(path):
    sep="&" if "?" in path else "?"; url=f"{BASE}{path}{sep}apiKey={KEY}"
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req,timeout=25) as r: return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e: return e.code, {"_err": e.read().decode()[:300]}
    except Exception as e: return None, {"_exc": f"{type(e).__name__}:{e}"}

print("="*64); print("(1) ETF CONSTITUENTS via composite_ticker"); print("="*64)
for etf in ["SPY","QQQ","XLK"]:
    code,j=get(f"/etf-global/v1/constituents?composite_ticker={etf}&limit=200")
    res=j.get("results") or []
    if res:
        dates=sorted({r.get("effective_date") for r in res}, reverse=True)
        names=[r.get("constituent_ticker") for r in res[:8]]
        comp=res[0].get("composite_ticker")
        print(f"\n{etf}: HTTP {code} | n={len(res)} | composite_ticker_field={comp} | latest_effective={dates[0] if dates else '?'}")
        print(f"   top8 holdings: {names}")
        print(f"   sample: {json.dumps(res[0],default=str)[:260]}")
    else:
        print(f"\n{etf}: HTTP {code} | {json.dumps(j,default=str)[:200]}")

print("\n"+"="*64); print("(2) BENZINGA EARNINGS — reported rows w/ actuals?"); print("="*64)
# pull AAPL full history, find rows with date in the past, dump ALL fields
code,j=get("/benzinga/v1/earnings?ticker=AAPL&limit=100")
res=j.get("results") or []
today=datetime.date.today().isoformat()
past=[r for r in res if (r.get("date") or "9999") < today]
print(f"AAPL earnings rows: {len(res)} total | {len(past)} with date<{today}")
if past:
    # newest past row = most recent reported quarter
    past.sort(key=lambda r: r.get("date",""), reverse=True)
    r=past[0]
    print("  most-recent REPORTED row ALL fields:")
    for k,v in r.items(): print(f"     {k}: {v}")
    # scan for any surprise/actual field names across all rows
    allkeys=set()
    for x in res: allkeys|=set(x.keys())
    surprise_keys=[k for k in allkeys if any(t in k.lower() for t in ("actual","surprise","reported","eps","revenue"))]
    print("  EPS/revenue/surprise-related fields seen:", sorted(surprise_keys))
print("\nDONE 1958")
