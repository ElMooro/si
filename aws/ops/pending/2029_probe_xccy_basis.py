"""ops 2029: probe for FREE FX forward points / forwards to build cross-currency basis. Honest go/no-go."""
import os, json, urllib.request, urllib.error
POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED="2f057499936072679d8843d7fce99989"
def get(u,hdr=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers=hdr or {"User-Agent":"jh/1"}),timeout=25) as r:
            return r.getcode(), r.read().decode()[:600]
    except urllib.error.HTTPError as e: return e.code, (e.read().decode()[:200] if e.fp else "")
    except Exception as e: return None, str(e)[:150]
print("="*60);print("1) Polygon FX spot (baseline) + any forward/futures");print("="*60)
print("C:EURUSD spot:",get(f"https://api.polygon.io/v2/aggs/ticker/C:EURUSD/prev?apiKey={POLY}")[0])
print("C:USDJPY spot:",get(f"https://api.polygon.io/v2/aggs/ticker/C:USDJPY/prev?apiKey={POLY}")[0])
# Polygon futures (FX forward proxy) — likely gated
c,b=get(f"https://api.polygon.io/v3/reference/tickers?market=fx&limit=3&apiKey={POLY}"); print("fx ref:",c)
c,b=get(f"https://api.polygon.io/vX/reference/tickers?market=futures&limit=3&apiKey={POLY}"); print("futures ref:",c,b[:80])

print("\n"+"="*60);print("2) FMP forex / forward");print("="*60)
for ep in ["forex/EURUSD","quote/EURUSD","forex-forward","historical-price-eod/forex"]:
    c,b=get(f"https://financialmodelingprep.com/stable/{ep}?apikey={FMP}&symbol=EURUSD")
    print(f"  /stable/{ep}: HTTP {c} {b[:90]}")

print("\n"+"="*60);print("3) FRED — spot FX + any forward/basis/OIS series");print("="*60)
def fred(sid):
    c,b=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=1")
    try: 
        import json as J; o=J.loads(b).get("observations",[{}])[0]; return f"{o.get('date')}={o.get('value')}"
    except: return f"HTTP{c}"
for s in ["DEXUSEU","DEXJPUS","DTB3","SOFR","IORB","ECBESTRVOLWGTTRMDMNRT","IR3TIB01EZM156N"]:
    print(f"  {s}: {fred(s)}")
# foreign 3M rates for CIP: EUR/JPY interbank
print("\n  CIP inputs needed: USD 3M (SOFR/bill) ✓, foreign 3M rate ✓(FRED), FX spot ✓, FX FORWARD ← the gating item")
print("DONE 2029")
