import json, urllib.request, time
def get(url, tmo=12):
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        r=urllib.request.urlopen(req, timeout=tmo); return r.getcode(), json.loads(r.read())
    except urllib.error.HTTPError as e: return e.code, None
    except Exception as e: return str(e)[:60], None

print("=== DEFILLAMA STABLECOINS ===")
c,d=get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
if d:
    arr=d.get("peggedAssets") or d
    print("  code",c,"| n stablecoins:",len(arr))
    top=sorted(arr,key=lambda x:-(x.get("circulating",{}).get("peggedUSD",0) if isinstance(x.get("circulating"),dict) else 0))[:5]
    for s in top: print("   ",s.get("symbol"),s.get("name"),"circ:",round((s.get("circulating",{}) or {}).get("peggedUSD",0)/1e9,1),"B")
else: print("  code",c,"FAIL")
c,d=get("https://stablecoins.llama.fi/stablecoincharts/all")
if isinstance(d,list): print("  agg chart points:",len(d),"| latest:",d[-1] if d else None)

print("\n=== DERIBIT (DVOL + perp funding/OI) ===")
c,d=get("https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL")
if d and d.get("result"):
    r=d["result"]; print("  BTC-PERP code",c,"| funding_8h:",r.get("funding_8h"),"| current_funding:",r.get("current_funding"),"| OI:",r.get("open_interest"),"| mark:",r.get("mark_price"),"| index:",r.get("index_price"))
else: print("  code",c,"FAIL")
now=int(time.time()*1000)
c,d=get(f"https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC&start_timestamp={now-3*86400000}&end_timestamp={now}&resolution=43200")
if d and d.get("result"):
    data=d["result"].get("data") or []
    print("  BTC DVOL points:",len(data),"| latest close:",data[-1][4] if data else None)
else: print("  DVOL code",c,"FAIL")
c,d=get("https://www.deribit.com/api/v2/public/ticker?instrument_name=ETH-PERPETUAL")
if d and d.get("result"): print("  ETH-PERP OI:",d["result"].get("open_interest"),"| funding_8h:",d["result"].get("funding_8h"))

print("\n=== BINANCE (US geo-block test) ===")
c,d=get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")
print("  premiumIndex code",c,"| lastFundingRate:",(d or {}).get("lastFundingRate") if d else "—")
c,d=get("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT")
print("  openInterest code",c,"|",(d or {}).get("openInterest") if d else "—")

print("\n=== BYBIT (US geo-block test) ===")
c,d=get("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT")
if d and d.get("result"):
    li=(d["result"].get("list") or [{}])[0]; print("  code",c,"| funding:",li.get("fundingRate"),"| OI:",li.get("openInterest"),"| basis via mark/index:",li.get("markPrice"),li.get("indexPrice"))
else: print("  code",c,"FAIL")

print("\n=== OKX (US geo-block test) ===")
c,d=get("https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP")
print("  OKX funding code",c,"|",((d or {}).get("data") or [{}])[0].get("fundingRate") if d else "—")
print("DONE 2361")
