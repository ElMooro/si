import json, urllib.request, urllib.parse, urllib.error
def g(url,t=40):
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read())

print("=== CFTC TFF — CME Bitcoin & Ether (properly encoded) ===")
for asset in ["BITCOIN","ETHER"]:
    try:
        params={"$where":f"upper(contract_market_name) like '%{asset}%'","$order":"report_date_as_yyyy_mm_dd DESC","$limit":"1"}
        url="https://publicreporting.cftc.gov/resource/gpe5-46if.json?"+urllib.parse.urlencode(params)
        rows=g(url)
        if rows:
            r=rows[0]
            print(f"  {asset}: '{r.get('contract_market_name')}' code={r.get('cftc_contract_market_code')} date={str(r.get('report_date_as_yyyy_mm_dd'))[:10]}")
            print(f"    AM L/S {r.get('asset_mgr_positions_long_all')}/{r.get('asset_mgr_positions_short_all')} | LevFund L/S {r.get('lev_money_positions_long_all')}/{r.get('lev_money_positions_short_all')} | Dealer L/S {r.get('dealer_positions_long_all')}/{r.get('dealer_positions_short_all')} | OI {r.get('open_interest_all')}")
        else: print(f"  {asset}: no rows")
    except Exception as e: print(f"  {asset} err:",str(e)[:100])

print("\n=== CoinMetrics community — per-metric free test for BTC ===")
cms=["PriceUSD","CapRealUSD","SplyCur","CapMVRVCur","NVTAdj","SOPR","FlowInExNtv","FlowOutExNtv","SplyAdrBalNtv1K","SplyAdrBalUSD1M","SplyActPct1yr","CapAct1yrUSD","CapMVRVFF","RealizedPriceUSD"]
free=[]
for m in cms:
    try:
        u=f"https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets=btc&metrics={m}&frequency=1d&page_size=1"
        d=g(u); data=d.get("data",[])
        if data and m in data[0]:
            print(f"  {m}: FREE  ({data[0].get(m)})"); free.append(m)
        else:
            print(f"  {m}: empty/no-field")
    except urllib.error.HTTPError as e:
        print(f"  {m}: HTTP {e.code} (not in community tier)")
    except Exception as e:
        print(f"  {m}: err {str(e)[:50]}")
print("FREE metrics:",free)
print("DONE 2396")
