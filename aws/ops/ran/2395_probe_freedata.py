import json, urllib.request, time
import boto3
def g(url,t=30):
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode()
def gj(url,t=30): return json.loads(g(url,t))

print("=== 1. crypto-cot live? ===")
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1")
try:
    lam.invoke(FunctionName="justhodl-crypto-cot",InvocationType="Event",Payload=b"{}")
    print("  invoked async")
except Exception as e: print("  invoke err",str(e)[:80])

print("\n=== 2. CoinMetrics community — which metrics are FREE? ===")
CM="https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets=btc&page_size=1&metrics="
for m in ["SplyCur","SplyActPct1yr","SplyAdrBalUSD1K","SOPR","FlowInExUSD","FlowOutExUSD","SplyExpFut","CapRealUSD"]:
    try:
        d=gj(CM+m); ok=bool(d.get("data")); val=(d.get("data") or [{}])[0].get(m) if ok else None
        print(f"  {m:18s} -> {'OK val='+str(val) if ok else 'empty'}")
    except urllib.error.HTTPError as e:
        print(f"  {m:18s} -> HTTP {e.code} (not free)")
    except Exception as e:
        print(f"  {m:18s} -> err {str(e)[:40]}")
    time.sleep(0.3)

print("\n=== 3. Coinbase premium sources (Coinbase vs Kraken vs OKX) ===")
try:
    cb=float(gj("https://api.exchange.coinbase.com/products/BTC-USD/ticker")["price"]); print("  Coinbase BTC-USD:",cb)
except Exception as e: print("  coinbase err",str(e)[:50])
try:
    kr=gj("https://api.kraken.com/0/public/Ticker?pair=XBTUSD"); k=list(kr["result"].values())[0]; print("  Kraken XBTUSD:",float(k["c"][0]))
except Exception as e: print("  kraken err",str(e)[:50])
try:
    ok=gj("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"); print("  OKX BTC-USDT:",float(ok["data"][0]["last"]))
except Exception as e: print("  okx err",str(e)[:50])

print("\n=== 4. Stablecoin depeg (Coingecko) ===")
try:
    sp=gj("https://api.coingecko.com/api/v3/simple/price?ids=tether,usd-coin,dai&vs_currencies=usd")
    print("  ",sp)
except Exception as e: print("  coingecko err",str(e)[:60])
print("DONE 2395")
