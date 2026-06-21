import urllib.request, json, time
def get(u,h=None):
    req=urllib.request.Request(u,headers=h or {"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=45) as r: return json.loads(r.read())

print("=== alternative.me Fear & Greed (full history) ===")
try:
    d=get("https://api.alternative.me/fng/?limit=0&format=json")
    arr=d.get("data",[])
    print("points:",len(arr),"| keys:",list(arr[0].keys()) if arr else None)
    print("latest:",json.dumps(arr[0])[:160] if arr else None)
    print("oldest:",json.dumps(arr[-1])[:160] if arr else None)
    def dt(x): return time.strftime("%Y-%m-%d",time.gmtime(int(x["timestamp"])))
    if arr: print("span:",dt(arr[-1]),"->",dt(arr[0]))
except Exception as e: print("ERR fng:",str(e)[:150])

print("\n=== CoinGecko /global (current total mcap + stablecoin share) ===")
try:
    g=get("https://api.coingecko.com/api/v3/global")
    dd=g.get("data",{})
    tmc=dd.get("total_market_cap",{}).get("usd")
    mcp=dd.get("market_cap_percentage",{})
    print(f"total crypto mcap: ${tmc/1e9:.0f}B" if tmc else "no mcap")
    print("btc.d:",mcp.get("btc"),"| eth.d:",mcp.get("eth"),"| usdt.d:",mcp.get("usdt"),"| usdc.d:",mcp.get("usdc"))
except Exception as e: print("ERR global:",str(e)[:150])

print("\n=== CoinGecko BTC mcap history free-tier depth check (days=365) ===")
try:
    mc=get("https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365&interval=daily")
    caps=mc.get("market_caps",[])
    print("btc mcap points (365d req):",len(caps),"| latest $%.0fB"%(caps[-1][1]/1e9) if caps else "none")
except Exception as e: print("ERR cg mcap:",str(e)[:150])
print("DONE 2082")
