"""ops 2592 — probe FMP /stable/ buyback fields + live buyback-scanner output."""
import urllib.request, json, time
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def fmp(path):
    try:
        u=f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=20)
        return json.loads(r.read().decode())
    except Exception as e: return {"__err__":str(e)[:80]}
for t in ["AAPL"]:
    print(f"\n##### {t} #####")
    cf=fmp(f"cash-flow-statement?symbol={t}&period=quarter&limit=5")
    if isinstance(cf,list) and cf:
        r=cf[0]
        bk=[k for k in r if any(w in k.lower() for w in ["repurchas","stock","issu","dividend","commonstock"])]
        print("  cash-flow buyback fields:", {k:r.get(k) for k in bk})
        print("  cash-flow dates:", [x.get("date") for x in cf])
    else: print("  cash-flow:", str(cf)[:120])
    km=fmp(f"key-metrics?symbol={t}&period=quarter&limit=2")
    if isinstance(km,list) and km:
        r=km[0]; bk=[k for k in r if any(w in k.lower() for w in ["buyback","yield","dividend","earningsyield","fcf","marketcap"])]
        print("  key-metrics fields:", {k:r.get(k) for k in bk})
    else: print("  key-metrics:", str(km)[:120])
    rt=fmp(f"ratios?symbol={t}&period=quarter&limit=1")
    if isinstance(rt,list) and rt:
        r=rt[0]; bk=[k for k in r if any(w in k.lower() for w in ["dividendyield","pricetoearnings","pe","payout","pricetofreecash"])]
        print("  ratios fields:", {k:r.get(k) for k in bk})
    pr=fmp(f"profile?symbol={t}")
    if isinstance(pr,list) and pr:
        r=pr[0]; print("  profile:", {k:r.get(k) for k in ["mktCap","price","lastDividend","sharesOutstanding","companyName","industry"] if k in r})
    ev=fmp(f"enterprise-values?symbol={t}&period=quarter&limit=5")
    if isinstance(ev,list) and ev:
        print("  enterprise-values numberOfShares trend:", [(x.get('date'),x.get('numberOfShares')) for x in ev])
print("\n##### live buyback-scanner.json #####")
try:
    j=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/buyback-scanner.json?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read())
    print("  top keys:", list(j.keys())[:20])
    print("  state:", j.get("state"), "| n_fresh:", j.get("n_fresh"), "| n_opportunities:", j.get("n_opportunities"))
    opp=j.get("opportunities") or j.get("ranked") or j.get("enriched") or []
    if isinstance(opp,list) and opp: print("  opp[0] keys:", list(opp[0].keys()))
    else:
        for k,v in j.items():
            if isinstance(v,list) and v and isinstance(v[0],dict): print(f"  list '{k}'[0] keys:", list(v[0].keys())[:18]); break
except Exception as e: print("  scanner err:", str(e)[:100])
print("DONE 2592")
