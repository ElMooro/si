import json, urllib.request
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"; POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(u):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    except urllib.error.HTTPError as e: return {"_http":e.code}
    except Exception as e: return {"_err":str(e)[:50]}
def fmp(c): return g(f"https://financialmodelingprep.com/stable/{c}{'&' if '?' in c else '?'}apikey={FMP}")
print("=== ANALYST RECOMMENDATIONS / RATINGS MOMENTUM (Bloomberg ANR) ===")
for c in ["grades?symbol=AAPL","grades-consensus?symbol=AAPL","grades-historical?symbol=AAPL","price-target-news?symbol=AAPL&limit=3","price-target-summary?symbol=AAPL"]:
    r=fmp(c); ok=isinstance(r,list) and r
    print(f"{'OK ' if ok else '-- '}{c.split('?')[0]:24}", (json.dumps(r[0])[:150] if ok else json.dumps(r)[:80]))
print("\n=== OPTIONS-IMPLIED EXPECTATIONS (Bloomberg OMON) — Polygon ===")
for u,lbl in [(f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=3&apiKey={POLY}","options-snapshot"),
              (f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=AAPL&limit=2&apiKey={POLY}","contracts")]:
    r=g(u); ok=isinstance(r,dict) and (r.get("results") or r.get("status")=="OK")
    res=r.get("results") if isinstance(r,dict) else None
    print(f"{'OK ' if res else '-- '}{lbl:18}", (json.dumps(res[0])[:200] if res else json.dumps(r)[:120]))
print("\n=== EVENTS CALENDAR (Bloomberg EVTS) ===")
for c in ["earnings?symbol=AAPL&limit=3","dividends?symbol=AAPL&limit=2"]:
    r=fmp(c); ok=isinstance(r,list) and r
    print(f"{'OK ' if ok else '-- '}{c.split('?')[0]:12}", (json.dumps(r[0])[:140] if ok else json.dumps(r)[:80]))
print("\n=== ESG (Refinitiv/Bloomberg ESG) ===")
r=fmp("esg-disclosures?symbol=AAPL"); 
print("esg-disclosures:", (json.dumps(r[0])[:140] if isinstance(r,list) and r else json.dumps(r)[:80]))
print("DONE 2271")
