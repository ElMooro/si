import json, urllib.request
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(u):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    except urllib.error.HTTPError as e: return {"_http":e.code,"_b":e.read(120).decode("utf-8","replace")}
    except Exception as e: return {"_err":str(e)[:60]}
# full snapshot — inspect ONE contract's complete field set
r=g(f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=5&apiKey={POLY}")
res=r.get("results") if isinstance(r,dict) else None
print("snapshot status:", r.get("status") if isinstance(r,dict) else r)
if res:
    c=res[0]
    print("ONE CONTRACT keys:", list(c.keys()))
    print("  details:", json.dumps(c.get("details")))
    print("  implied_volatility:", c.get("implied_volatility"))
    print("  greeks:", json.dumps(c.get("greeks")))
    print("  open_interest:", c.get("open_interest"))
    print("  day:", json.dumps(c.get("day"))[:160])
    print("  last_quote:", json.dumps(c.get("last_quote"))[:200])
    print("  last_trade:", json.dumps(c.get("last_trade"))[:160])
    print("  underlying_asset:", json.dumps(c.get("underlying_asset"))[:160])
# can we filter by expiration + strike range? (for ATM straddle near earnings)
r2=g(f"https://api.polygon.io/v3/snapshot/options/AAPL?expiration_date.gte=2026-07-01&strike_price.gte=180&strike_price.lte=200&contract_type=call&limit=5&apiKey={POLY}")
res2=r2.get("results") if isinstance(r2,dict) else None
print("\nfiltered (calls 180-200, exp>=jul):", len(res2) if res2 else r2)
if res2:
    for c in res2[:4]:
        d=c.get("details") or {}
        print(f"  {d.get('expiration_date')} {d.get('strike_price')} IV={c.get('implied_volatility')} bid/ask={(c.get('last_quote') or {}).get('bid')}/{(c.get('last_quote') or {}).get('ask')} OI={c.get('open_interest')}")
print("DONE 2277")
