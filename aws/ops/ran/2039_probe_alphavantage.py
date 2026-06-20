"""ops 2039: honest audit of AlphaVantage value — entitlements, rate limit, redundancy vs existing stack."""
import json, urllib.request, urllib.error, time
AV="EOLGKSGAYZUXKPUL"
def get(fn_params):
    u=f"https://www.alphavantage.co/query?{fn_params}&apikey={AV}"
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh/1"}),timeout=25) as r:
            return r.getcode(), r.read().decode()
    except urllib.error.HTTPError as e: return e.code,(e.read().decode()[:200] if e.fp else "")
    except Exception as e: return None,str(e)[:150]
def probe(label, params, keys_to_show):
    c,b=get(params)
    try: j=json.loads(b)
    except: print(f"{label}: HTTP {c} non-json {b[:120]}"); return None
    # AV throttle/entitlement messages
    for warn in ("Note","Information","Error Message"):
        if warn in j:
            print(f"{label}: ⚠️ {warn}: {str(j[warn])[:180]}"); return j
    present=[k for k in keys_to_show if k in j]
    size=len(b)
    print(f"{label}: HTTP {c} OK | keys present: {present} | payload {size}B")
    return j

print("="*64);print("ALPHAVANTAGE ENTITLEMENT + RATE-LIMIT AUDIT (key EOLGK…)");print("="*64)
j=probe("1. GLOBAL_QUOTE AAPL (core)", "function=GLOBAL_QUOTE&symbol=AAPL", ["Global Quote"])
time.sleep(1)
j=probe("2. NEWS_SENTIMENT (ticker sentiment — potentially unique)", "function=NEWS_SENTIMENT&tickers=AAPL&limit=5", ["feed","sentiment_score_definition","items"])
if j and j.get("feed"):
    f0=j["feed"][0]
    print("     sample article sentiment:",{k:f0.get(k) for k in ("overall_sentiment_label","overall_sentiment_score")},
          "| ticker_sentiment[0]:", (f0.get("ticker_sentiment") or [{}])[0])
time.sleep(1)
j=probe("3. LISTING_STATUS delisted (survivorship-bias universe — possibly unique)", "function=LISTING_STATUS&state=delisted", [])
# LISTING_STATUS returns CSV not JSON
c,b=get("function=LISTING_STATUS&state=delisted")
print("     LISTING_STATUS raw head:", b[:160].replace("\n"," | "))
time.sleep(1)
probe("4. INSIDER_TRANSACTIONS AAPL (redundant w/ FMP?)", "function=INSIDER_TRANSACTIONS&symbol=AAPL", ["data","transactions"])
time.sleep(1)
probe("5. EARNINGS_CALENDAR (redundant w/ FMP/Benzinga?)", "function=EARNINGS_CALENDAR&horizon=3month", [])
time.sleep(1)
probe("6. REAL_GDP (redundant w/ FRED?)", "function=REAL_GDP&interval=quarterly", ["data"])
time.sleep(1)
probe("7. HISTORICAL_OPTIONS AAPL (memory says PREMIUM)", "function=HISTORICAL_OPTIONS&symbol=AAPL", ["data","message"])
time.sleep(1)
probe("8. ANALYTICS_FIXED_WINDOW (corr/stats — premium?)", "function=ANALYTICS_FIXED_WINDOW&SYMBOLS=AAPL,MSFT&RANGE=60day&INTERVAL=DAILY&CALCULATIONS=CORRELATION", ["payload","Meta Data"])
print("DONE 2039")
