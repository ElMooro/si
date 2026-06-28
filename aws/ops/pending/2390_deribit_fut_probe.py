import json, urllib.request, time
from datetime import datetime, timezone
def g(url):
    req=urllib.request.Request(url,headers={"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=30) as r: return json.loads(r.read())
B="https://www.deribit.com/api/v2/public/"
now=int(time.time()*1000)
inst=g(B+"get_instruments?currency=BTC&kind=future&expired=false")["result"]
print("BTC futures:",len(inst))
for i in sorted(inst,key=lambda x:x.get("expiration_timestamp",0)):
    exp=i.get("expiration_timestamp")
    d=round((exp-now)/86400000,1) if exp and exp<32503680000000 else "perp"
    print(f"  {i['instrument_name']:22s} settlement={i.get('settlement_period'):10s} exp_days={d}")
# book summary for futures
bs=g(B+"get_book_summary_by_currency?currency=BTC&kind=future")["result"]
print("\nbook_summary future keys:",sorted(bs[0].keys()))
# ticker on a dated future to see basis fields
dated=[i for i in inst if i.get("settlement_period") in ("month","week") and i.get("expiration_timestamp",0)>now]
if dated:
    nm=sorted(dated,key=lambda x:x["expiration_timestamp"])[0]["instrument_name"]
    tk=g(B+"ticker?instrument_name="+nm)["result"]
    print("\nticker(%s) keys:"%nm,sorted(tk.keys()))
    print("  mark_price:",tk.get("mark_price")," index_price:",tk.get("index_price")," last:",tk.get("last_price"))
# perpetual ticker (funding)
pk=g(B+"ticker?instrument_name=BTC-PERPETUAL")["result"]
print("\nperp: mark",pk.get("mark_price"),"index",pk.get("index_price"),"funding_8h",pk.get("current_funding"),"funding_1d",pk.get("funding_8h"))
print("DONE 2390")
