import json, urllib.request, time
from datetime import datetime, timezone
def g(url):
    req=urllib.request.Request(url,headers={"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=30) as r: return json.loads(r.read())
B="https://www.deribit.com/api/v2/public/"
# 1) instruments
inst=g(B+"get_instruments?currency=BTC&kind=option&expired=false")["result"]
print("BTC options live:",len(inst))
print("sample instrument keys:",sorted(inst[0].keys()))
print("sample:",{k:inst[0].get(k) for k in ("instrument_name","strike","option_type","expiration_timestamp","tick_size")})
# group by expiry, find ~30d
now=int(time.time()*1000)
exps=sorted(set(i["expiration_timestamp"] for i in inst))
print("n_expiries:",len(exps))
def days(ts): return round((ts-now)/86400000,1)
print("expiries (days out):",[days(e) for e in exps][:12])
# pick expiry nearest 30d
tgt=min(exps,key=lambda e:abs((e-now)/86400000-30))
strikes=[i for i in inst if i["expiration_timestamp"]==tgt]
print("target expiry ~30d:",datetime.fromtimestamp(tgt/1000,tz=timezone.utc).date().isoformat(),"| n_strikes:",len(strikes))
# 2) ticker on one near-money call to see greeks/iv schema
und=g(B+"ticker?instrument_name="+strikes[0]["instrument_name"])["result"].get("underlying_price")
print("underlying_price:",und)
# find a call strike just above underlying
calls=sorted([s for s in strikes if s["option_type"]=="call"],key=lambda s:s["strike"])
near=min(calls,key=lambda s:abs(s["strike"]-(und or s["strike"])))
tk=g(B+"ticker?instrument_name="+near["instrument_name"])["result"]
print("ticker keys:",sorted(tk.keys()))
print("ticker sample:",{k:tk.get(k) for k in ("instrument_name","mark_iv","bid_iv","ask_iv","underlying_price")})
print("greeks:",tk.get("greeks"))
# 3) bulk book summary (for ATM/term structure in one call)
bs=g(B+"get_book_summary_by_currency?currency=BTC&kind=option")["result"]
print("book_summary n:",len(bs),"| sample keys:",sorted(bs[0].keys()))
print("book_summary mark_iv present?:","mark_iv" in bs[0],"| underlying present?:","underlying_price" in bs[0])
print("DONE 2380")
