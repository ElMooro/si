import json, urllib.request, urllib.parse
def g(url,t=40):
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read())

print("=== 1. CFTC TFF — CME Bitcoin & Ether COT ===")
try:
    base="https://publicreporting.cftc.gov/resource/gpe5-46if.json"
    for asset in ["BITCOIN","ETHER"]:
        q=base+"?$where="+urllib.parse.quote(f"upper(contract_market_name) like '%{asset}%'")+"&$order=report_date_as_yyyy_mm_dd desc&$limit=1"
        rows=g(q)
        if rows:
            r=rows[0]
            print(f"  {asset}: market='{r.get('contract_market_name')}' code={r.get('cftc_contract_market_code')} date={r.get('report_date_as_yyyy_mm_dd','')[:10]}")
            print(f"    asset_mgr L/S: {r.get('asset_mgr_positions_long_all')}/{r.get('asset_mgr_positions_short_all')} | lev_money L/S: {r.get('lev_money_positions_long_all')}/{r.get('lev_money_positions_short_all')} | dealer L/S: {r.get('dealer_positions_long_all')}/{r.get('dealer_positions_short_all')} | OI: {r.get('open_interest_all')}")
        else:
            print(f"  {asset}: no rows")
except Exception as e: print("  CFTC err:",str(e)[:120])

print("\n=== 2. CoinMetrics community — which cohort metrics are FREE for BTC ===")
try:
    cat=g("https://community-api.coinmetrics.io/v4/catalog/asset-metrics?assets=btc")
    metrics=[]
    for a in cat.get("data",[]):
        for m in a.get("metrics",[]):
            metrics.append(m.get("metric"))
    kw=["SOPR","NUPL","CapReal","Sply","Flow","Adr","MVRV","RevAll","Profit","Loss","CapAct","Real"]
    hits=sorted(set(m for m in metrics if any(k.lower() in (m or "").lower() for k in kw)))
    print("  total metrics:",len(metrics),"| cohort/flow/realized candidates:",len(hits))
    for m in hits[:40]: print("   ",m)
except Exception as e: print("  CM catalog err:",str(e)[:120])

print("\n=== 3. Coinbase premium (Coinbase vs Kraken spot) ===")
try:
    cb=g("https://api.exchange.coinbase.com/products/BTC-USD/ticker")
    kr=g("https://api.kraken.com/0/public/Ticker?pair=XBTUSD")
    cbp=float(cb.get("price"))
    krp=float(list(kr["result"].values())[0]["c"][0])
    print(f"  Coinbase ${cbp} | Kraken ${krp} | premium {round((cbp/krp-1)*100,3)}%")
except Exception as e: print("  premium err:",str(e)[:120])

print("\n=== 4. Stablecoin depeg (CoinGecko) ===")
try:
    sp=g("https://api.coingecko.com/api/v3/simple/price?ids=tether,usd-coin,dai,first-digital-usd&vs_currencies=usd")
    for k,v in sp.items(): print(f"  {k}: ${v.get('usd')} (depeg {round((v.get('usd',1)-1)*100,3)}%)")
except Exception as e: print("  stablecoin err:",str(e)[:120])
print("DONE 2395")
