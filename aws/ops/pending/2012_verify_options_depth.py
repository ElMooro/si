"""ops 2012: verify Polygon options snapshot field richness (greeks/IV/OI) + AV options reality."""
import os, json, urllib.request, urllib.error
POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
AV=os.environ.get("ALPHAVANTAGE_KEY","EOLGKSGAYZUXKPUL")
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh/1.0"}),timeout=25) as r:
            return r.getcode(), r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e:
        return e.code, (e.read().decode("utf-8","replace") if hasattr(e,'read') else "")
    except Exception as e: return None, str(e)[:120]

print("="*68);print("POLYGON OPTIONS SNAPSHOT — full field inventory (AAPL)");print("="*68)
code,body=get(f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=3&apiKey={POLY}")
print("HTTP",code)
j=json.loads(body); res=j.get("results") or []
print("contracts returned:",len(res))
if res:
    c=res[0]
    print("TOP-LEVEL KEYS:",sorted(c.keys()))
    print("greeks:",c.get("greeks"))
    print("implied_volatility:",c.get("implied_volatility"))
    print("open_interest:",c.get("open_interest"))
    print("day:",json.dumps(c.get("day"),default=str)[:200])
    print("last_quote:",json.dumps(c.get("last_quote"),default=str)[:160])
    print("last_trade:",json.dumps(c.get("last_trade"),default=str)[:160])
    print("details:",json.dumps(c.get("details"),default=str)[:200])
    print("underlying_asset:",json.dumps(c.get("underlying_asset"),default=str)[:160])

print("\n"+"="*68);print("POLYGON — full chain depth + a single contract aggs");print("="*68)
# how many contracts total available (pagination)?
code,body=get(f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=250&apiKey={POLY}")
j=json.loads(body); res=j.get("results") or []
n_greeks=sum(1 for c in res if c.get("greeks")); n_iv=sum(1 for c in res if c.get("implied_volatility") is not None); n_oi=sum(1 for c in res if c.get("open_interest") is not None)
print(f"chain page: {len(res)} contracts | with greeks={n_greeks} | with IV={n_iv} | with OI={n_oi}")
if res:
    tk=res[0].get("details",{}).get("ticker")
    if tk:
        code,b2=get(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/2026-05-20/2026-06-19?apiKey={POLY}")
        j2=json.loads(b2) if code==200 else {}
        print(f"option contract aggs {tk}: HTTP {code} bars={len(j2.get('results') or [])}")

print("\n"+"="*68);print("ALPHAVANTAGE options — real data or premium nag?");print("="*68)
for fn in ["REALTIME_OPTIONS&symbol=AAPL","HISTORICAL_OPTIONS&symbol=AAPL"]:
    code,body=get(f"https://www.alphavantage.co/query?function={fn}&apikey={AV}")
    print(f"\n{fn.split('&')[0]} HTTP {code}:")
    print(" ",body[:240].replace("\n"," "))
print("\nDONE 2012")
