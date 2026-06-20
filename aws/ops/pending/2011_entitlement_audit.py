"""ops 2011: ENTITLEMENT AUDIT — probe everything I previously assumed was paid/gated,
to see what's ALREADY included in the paid plans (Polygon base+Massive, FMP /stable/, CMC, AV)."""
import os, json, urllib.request, urllib.error, boto3

ssm=boto3.client("ssm","us-east-1")
def ssm_get(p):
    try: return ssm.get_parameter(Name=p,WithDecryption=True)["Parameter"]["Value"]
    except Exception as e: return None

POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")  # base stocks-only tier
MASSIVE=os.environ.get("MASSIVE_API_KEY") or ssm_get("/justhodl/massive-api-key")
FMP=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
CMC=os.environ.get("CMC_KEY","17ba8e87-53f0-46f4-abe5-014d9cd99597")
AV=os.environ.get("ALPHAVANTAGE_KEY","EOLGKSGAYZUXKPUL")
print("keys: base_poly=%s massive=%s(%s) fmp=%s cmc=%s" % (
    POLY[:6], (MASSIVE[:6] if MASSIVE else None), "SSM/env" if MASSIVE else "MISSING", FMP[:6], CMC[:6]))

def probe(label, url, headers=None, want=None):
    try:
        req=urllib.request.Request(url, headers=headers or {"User-Agent":"jh-audit/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            code=r.getcode(); body=r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e:
        body=""; 
        try: body=e.read().decode("utf-8","replace")
        except Exception: pass
        print(f"  [{code if False else e.code}] {label}: {body[:90]}")
        return
    except Exception as e:
        print(f"  [ERR] {label}: {str(e)[:80]}"); return
    # usable heuristic
    usable=True; note=""
    try:
        j=json.loads(body)
        if isinstance(j,dict):
            st=j.get("status"); res=j.get("results"); err=j.get("error") or j.get("message")
            cnt=(len(res) if isinstance(res,(list,dict)) else None)
            if err and not res: usable=False; note=str(err)[:70]
            elif res is not None and cnt==0: usable=False; note="empty results"
            else: note=f"status={st} n={cnt}" if cnt is not None else f"status={st}"
        elif isinstance(j,list):
            usable=len(j)>0; note=f"list n={len(j)}" + ("" if j else " EMPTY")
    except Exception:
        note=body[:60]
    flag="✅" if usable else "⚠️ "
    print(f"  [{code}]{flag}{label}: {note}")

pk=f"apiKey={POLY}"; mk=f"apiKey={MASSIVE}" if MASSIVE else pk
print("\n"+"="*70); print("POLYGON — base stocks-only tier  vs  MASSIVE add-on key"); print("="*70)
print("-- OPTIONS (I assumed gated except SPY/QQQ/HYG/TLT) --")
probe("options contracts ref [base]", f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=AAPL&limit=2&{pk}")
probe("options contracts ref [MASSIVE]", f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=AAPL&limit=2&{mk}")
probe("options chain snapshot AAPL [base]", f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=2&{pk}")
probe("options chain snapshot AAPL [MASSIVE]", f"https://api.polygon.io/v3/snapshot/options/AAPL?limit=2&{mk}")
probe("options chain NVDA [MASSIVE]", f"https://api.polygon.io/v3/snapshot/options/NVDA?limit=2&{mk}")
print("-- INDICES (I assumed 403 → I use FRED for VIX) --")
probe("indices snapshot VIX/SPX/NDX [base]", f"https://api.polygon.io/v3/snapshot/indices?ticker.any_of=I:VIX,I:SPX,I:NDX&{pk}")
probe("indices snapshot VIX/SPX/NDX [MASSIVE]", f"https://api.polygon.io/v3/snapshot/indices?ticker.any_of=I:VIX,I:SPX,I:NDX&{mk}")
probe("index aggs I:VIX daily [MASSIVE]", f"https://api.polygon.io/v2/aggs/ticker/I:VIX/range/1/day/2026-06-10/2026-06-19?{mk}")
print("-- TICK DATA trades/quotes (I assumed top-tier only) --")
probe("trades AAPL [base]", f"https://api.polygon.io/v3/trades/AAPL?limit=1&{pk}")
probe("trades AAPL [MASSIVE]", f"https://api.polygon.io/v3/trades/AAPL?limit=1&{mk}")
probe("NBBO quotes AAPL [MASSIVE]", f"https://api.polygon.io/v3/quotes/AAPL?limit=1&{mk}")
print("-- OTHER --")
probe("technical RSI AAPL [base]", f"https://api.polygon.io/v1/indicators/rsi/AAPL?limit=1&{pk}")
probe("forex aggs EURUSD [MASSIVE]", f"https://api.polygon.io/v2/aggs/ticker/C:EURUSD/range/1/day/2026-06-17/2026-06-19?{mk}")
probe("crypto aggs BTCUSD [base]", f"https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/2026-06-17/2026-06-19?{pk}")
probe("futures snapshot [MASSIVE]", f"https://api.polygon.io/futures/vX/products?limit=2&{mk}")
probe("financials AAPL [base]", f"https://api.polygon.io/vX/reference/financials?ticker=AAPL&limit=1&{pk}")
probe("ticker news AAPL [base]", f"https://api.polygon.io/v2/reference/news?ticker=AAPL&limit=1&{pk}")

print("\n"+"="*70); print("MASSIVE / BENZINGA — beyond earnings/ratings/guidance"); print("="*70)
if MASSIVE:
    probe("benzinga news", f"https://api.polygon.io/benzinga/v1/news?limit=1&{mk}")
    probe("benzinga analyst-insights", f"https://api.polygon.io/benzinga/v1/analyst-insights?limit=1&{mk}")
    probe("etf-global fund-flows", f"https://api.polygon.io/etf-global/v1/fund-flows?limit=1&{mk}")

print("\n"+"="*70); print("FMP /stable/ — premium-sounding endpoints I route around"); print("="*70)
for path in ["senate-trades?symbol=AAPL","insider-trading/latest?page=0","institutional-ownership/symbol-ownership?symbol=AAPL",
             "earning-call-transcript?symbol=AAPL&year=2025&quarter=1","discounted-cash-flow?symbol=AAPL",
             "price-target-consensus?symbol=AAPL","commitment-of-traders-report?symbol=ES","treasury-rates",
             "economic-indicators?name=GDP","esg-disclosures?symbol=AAPL","earnings-transcript-list?symbol=AAPL",
             "historical-market-capitalization?symbol=AAPL&limit=1","analyst-estimates?symbol=AAPL&limit=1"]:
    sep="&" if "?" in path else "?"
    probe(f"FMP {path.split('?')[0]}", f"https://financialmodelingprep.com/stable/{path}{sep}apikey={FMP}")

print("\n"+"="*70); print("CMC — OHLCV + others (I assumed OHLCV gated)"); print("="*70)
h={"X-CMC_PRO_API_KEY":CMC,"Accept":"application/json"}
probe("CMC listings latest", "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=1", headers=h)
probe("CMC OHLCV latest", "https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/latest?symbol=BTC", headers=h)
probe("CMC OHLCV historical", "https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical?symbol=BTC&count=2", headers=h)
probe("CMC quotes latest", "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol=BTC", headers=h)
probe("CMC global metrics", "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest", headers=h)

print("\n"+"="*70); print("ALPHAVANTAGE — premium-sounding (free tier?)"); print("="*70)
for fn in ["NEWS_SENTIMENT&tickers=AAPL","INSIDER_TRANSACTIONS&symbol=AAPL","HISTORICAL_OPTIONS&symbol=AAPL",
           "REALTIME_OPTIONS&symbol=AAPL","EARNINGS_CALL_TRANSCRIPT&symbol=AAPL&quarter=2024Q1"]:
    probe(f"AV {fn.split('&')[0]}", f"https://www.alphavantage.co/query?function={fn}&apikey={AV}")
print("DONE 2011")
