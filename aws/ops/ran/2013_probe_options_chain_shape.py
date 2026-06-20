"""ops 2013: probe Polygon options snapshot filtering/pagination + spot source, to size the build."""
import os, json, urllib.request, urllib.error
POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh/1.0"}),timeout=30) as r:
            return r.getcode(), json.loads(r.read().decode("utf-8","replace"))
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, {}
    except Exception as e: return None, {"err":str(e)[:100]}

print("="*68);print("1) SPOT price source (need underlying spot for GEX/moneyness)");print("="*68)
code,j=get(f"https://api.polygon.io/v2/aggs/ticker/AAPL/prev?adjusted=true&apiKey={POLY}")
res=(j.get("results") or [{}])
print("AAPL /prev:",code,"close=",res[0].get("c") if res else None)
spot = res[0].get("c") if res else 220

print("\n"+"="*68);print("2) snapshot with strike+expiry+type FILTERS + pagination");print("="*68)
lo,hi=round(spot*0.80),round(spot*1.20)
url=(f"https://api.polygon.io/v3/snapshot/options/AAPL?"
     f"strike_price.gte={lo}&strike_price.lte={hi}&expiration_date.lte=2026-09-30"
     f"&limit=250&apiKey={POLY}")
code,j=get(url)
res=j.get("results") or []
print(f"AAPL filtered strikes[{lo},{hi}] exp<=2026-09-30: HTTP {code} contracts={len(res)} next_url={'YES' if j.get('next_url') else 'no'}")
if res:
    exps=sorted({c.get("details",{}).get("expiration_date") for c in res})
    print(" expiries present:",len(exps),exps[:8])
    ng=sum(1 for c in res if c.get("greeks")); niv=sum(1 for c in res if c.get("implied_volatility") is not None)
    print(f" greeks={ng}/{len(res)} IV={niv}/{len(res)}")
    c=res[len(res)//2]
    print(" mid sample:",{"strike":c.get("details",{}).get("strike_price"),"type":c.get("details",{}).get("contract_type"),
                          "exp":c.get("details",{}).get("expiration_date"),"iv":c.get("implied_volatility"),
                          "gamma":(c.get("greeks") or {}).get("gamma"),"oi":c.get("open_interest"),
                          "vol":(c.get("day") or {}).get("volume")})
# contract_type filter
code,j2=get(url+"&contract_type=put")
print(" contract_type=put filter:",code,"n=",len(j2.get('results') or []))

print("\n"+"="*68);print("3) SIZE high-options names (cost sizing)");print("="*68)
for t in ["NVDA","TSLA","SPY","GME"]:
    cc,jj=get(f"https://api.polygon.io/v2/aggs/ticker/{t}/prev?adjusted=true&apiKey={POLY}")
    rr=jj.get("results") or [{}]; sp=rr[0].get("c") if rr else 100
    lo2,hi2=round(sp*0.80),round(sp*1.20)
    c2,j3=get(f"https://api.polygon.io/v3/snapshot/options/{t}?strike_price.gte={lo2}&strike_price.lte={hi2}&expiration_date.lte=2026-09-30&limit=250&apiKey={POLY}")
    r3=j3.get("results") or []
    print(f" {t}: spot={sp} contracts(±20%,<=Sep)={len(r3)} next={'Y' if j3.get('next_url') else 'n'}")
print("DONE 2013")
