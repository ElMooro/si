import urllib.request, json, datetime
KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def get(u):
    try:
        return json.loads(urllib.request.urlopen(u+("&" if "?" in u else "?")+"apiKey="+KEY,timeout=40).read())
    except Exception as e: return {"_err":str(e)[:60]}
y=(datetime.date.today()-datetime.timedelta(days=1)).isoformat()
# 1) grouped crypto
g=get(f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{y}?adjusted=true")
if "_err" in g: print("GROUPED crypto:",g["_err"])
else:
    res=g.get("results") or []
    usd=[r for r in res if str(r.get("T","")).endswith("USD")]
    print(f"GROUPED crypto: OK pairs={len(res)} usd={len(usd)}")
    for r in sorted(usd,key=lambda x:-(x.get('c',0)*x.get('v',0)))[:6]:
        print("   ",r['T'],"c",r.get('c'),"$vol",round(r.get('c',0)*r.get('v',0)/1e6,1),"M")
# 2) per-ticker daily aggs
frm=(datetime.date.today()-datetime.timedelta(days=30)).isoformat()
a=get(f"https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/{frm}/{y}?adjusted=true&limit=40")
if "_err" in a: print("PER-TICKER X:BTCUSD:",a["_err"])
else: print(f"PER-TICKER X:BTCUSD: OK status={a.get('status')} bars={a.get('resultsCount')}")
print("DONE 2166")
