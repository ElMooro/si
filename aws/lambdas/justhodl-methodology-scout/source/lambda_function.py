"""justhodl-methodology-scout v1.0 — the organism learns from other
minds. Reads PUBLIC teachers (Microsoft Qlib, OpenBB, TA-Lib, ta,
awesome-quant) with attribution, harvests indicator vocabularies and
data-source atlases, diffs against OUR fleet vocabulary, and emits a
consensus-ranked GAP report. Constitution: borrowed ideas enter as
shadow candidates and must EARN adoption via the graded ledger --
copying is proposal, grades are law.
Outputs: data/methodology-kb.json · data/methodology-gaps.json"""
import json, re, time, urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
UA = {"User-Agent": "justhodl-methodology-scout/1.0"}
TEACHERS = [
    ("microsoft/qlib",
     "https://raw.githubusercontent.com/microsoft/qlib/main/"
     "README.md"),
    ("bukosabino/ta",
     "https://raw.githubusercontent.com/bukosabino/ta/master/"
     "README.md"),
    ("TA-Lib/ta-lib-python",
     "https://raw.githubusercontent.com/TA-Lib/ta-lib-python/"
     "master/docs/funcs.md"),
    ("twopirllc/pandas-ta",
     "https://raw.githubusercontent.com/twopirllc/pandas-ta/"
     "master/README.md"),
    ("OpenBB-finance/OpenBB-providers",
     "https://raw.githubusercontent.com/OpenBB-finance/OpenBB/"
     "develop/openbb_platform/PROVIDERS.md"),
    ("wilsonfreitas/awesome-quant",
     "https://raw.githubusercontent.com/wilsonfreitas/"
     "awesome-quant/master/README.md"),
]
IND_RX = re.compile(
    r"\b(RSI|MACD|ADX|ATR|OBV|VWAP|CCI|MFI|ROC|TRIX|EMA|SMA|WMA|"
    r"DEMA|TEMA|KAMA|PPO|APO|AROON(?:OSC)?|BOP|CMO|DX|MINUS_DI|"
    r"PLUS_DI|STOCH(?:RSI|F)?|ULTOSC|WILLR|AD|ADOSC|BBANDS?|"
    r"BOLLINGER|DONCHIAN|KELTNER|ICHIMOKU|PSAR|SUPERTREND|"
    r"CHAIKIN\w*|FORCE.?INDEX|EASE.?OF.?MOVEMENT|"
    r"ACCUM\w*/?DIST\w*|ELDER.?RAY|MASS.?INDEX|VORTEX|KST|DPO|"
    r"COPPOCK|ZIGZAG|PIVOT.?POINTS?|HEIKIN.?ASHI|RENKO|"
    r"HURST|KALMAN|GARCH|SHARPE|SORTINO|CALMAR|MAX.?DRAWDOWN|"
    r"INFORMATION.?RATIO|ALPHA158|ALPHA360|IC\b|RANKIC|"
    r"TSMOM|CROSS.?SECTIONAL.?MOMENTUM|PAIRS?.?TRADING|"
    r"MEAN.?REVERSION|ORNSTEIN|COINTEGRATION|PCA|HMM|LSTM|"
    r"GBDT|LIGHTGBM|XGBOOST|TRANSFORMER)\b", re.I)
SRC_RX = re.compile(
    r"\b(Yahoo\s?Finance|Alpha\s?Vantage|Quandl|Nasdaq\s?Data|"
    r"IEX\s?Cloud|Polygon(?:\.io)?|Tiingo|Finnhub|FRED|"
    r"SEC\s?EDGAR|EDGAR|CBOE|CME|Binance|Coinbase|CoinGecko|"
    r"CoinMetrics|Glassnode|Kaiko|Refinitiv|Bloomberg|"
    r"Interactive\s?Brokers|Alpaca|EODHD|Stooq|"
    r"World\s?Bank|IMF|OECD|Eurostat|BIS|ECB|"
    r"Financial\s?Modeling\s?Prep|Intrinio|Barchart|"
    r"Wharton|WRDS|CRSP|Compustat|Ken\s?French)\b")
CAT = [("momentum", ("RSI", "ROC", "MOM", "MACD", "TRIX", "CMO",
                     "TSMOM", "KST", "COPPOCK", "STOCH", "WILLR",
                     "ULTOSC", "PPO", "APO")),
       ("trend", ("EMA", "SMA", "WMA", "DEMA", "TEMA", "KAMA",
                  "ADX", "AROON", "PSAR", "SUPERTREND",
                  "ICHIMOKU", "VORTEX", "DPO", "DX", "DI")),
       ("volatility", ("ATR", "BBANDS", "BOLLINGER", "KELTNER",
                       "DONCHIAN", "GARCH", "MASS")),
       ("volume", ("OBV", "VWAP", "MFI", "AD", "ADOSC",
                   "CHAIKIN", "FORCE", "EASE", "ACCUM")),
       ("stat-ml", ("HURST", "KALMAN", "PCA", "HMM", "LSTM",
                    "GBDT", "LIGHTGBM", "XGBOOST",
                    "TRANSFORMER", "COINTEGRATION", "ORNSTEIN",
                    "ALPHA158", "ALPHA360", "RANKIC", "IC")),
       ("risk-perf", ("SHARPE", "SORTINO", "CALMAR",
                      "DRAWDOWN", "INFORMATION"))]


def cat_of(name):
    up = name.upper()
    for c, keys in CAT:
        if any(k in up for k in keys):
            return c
    return "other"


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def our_vocab():
    v = set()
    for x in (rd("data/engine-leaderboard.json")
              or {}).get("board") or []:
        v.add(str(x.get("engine", "")).upper())
    bus = (rd("data/feature-bus.json") or {}).get("tickers") or {}
    for row in list(bus.values())[:5]:
        for k in row:
            v.add(str(k).upper())
    fab = rd("data/signal-fabric.json") or {}
    for t in (fab.get("tickers") or [])[:50]:
        for e in t.get("engines") or []:
            v.add(str(e.get("kind", "")).upper())
    return v


def lambda_handler(event=None, context=None):
    t0 = time.time()
    inds = {}
    srcs = {}
    tlog = []
    for name, url in TEACHERS:
        try:
            body = urllib.request.urlopen(
                urllib.request.Request(url, headers=UA),
                timeout=25).read().decode("utf-8",
                                          "ignore")[:400000]
            hits = set(m.group(0).upper().replace(" ", "_")
                       for m in IND_RX.finditer(body))
            shits = set(m.group(0)
                        for m in SRC_RX.finditer(body))
            for h in hits:
                inds.setdefault(h, set()).add(name)
            for s0 in shits:
                srcs.setdefault(s0, set()).add(name)
            tlog.append({"teacher": name, "url": url,
                         "indicators": len(hits),
                         "data_sources": len(shits)})
        except Exception as e:
            tlog.append({"teacher": name, "url": url,
                         "error": str(e)[:90]})
    ours = our_vocab()
    kb_ind = sorted(
        ({"name": k, "teachers": sorted(v),
          "n_teachers": len(v), "category": cat_of(k),
          "in_our_fleet": any(k in o or o in k
                              for o in ours if len(o) > 2)}
         for k, v in inds.items()),
        key=lambda x: (-x["n_teachers"], x["name"]))
    kb_src = sorted(
        ({"source": k, "teachers": sorted(v),
          "n_teachers": len(v)} for k, v in srcs.items()),
        key=lambda x: (-x["n_teachers"], x["source"]))
    gaps = [x for x in kb_ind if not x["in_our_fleet"]]
    kb = {"engine": "justhodl-methodology-scout",
          "version": "1.0",
          "generated_at": datetime.now(timezone.utc).isoformat(),
          "elapsed_s": round(time.time() - t0, 1),
          "constitution": ("borrowed ideas enter as SHADOW "
                           "candidates and must earn adoption "
                           "via the graded ledger"),
          "teachers": tlog,
          "n_indicators": len(kb_ind),
          "n_data_sources": len(kb_src),
          "indicators": kb_ind,
          "data_sources": kb_src}
    s3.put_object(Bucket=B, Key="data/methodology-kb.json",
                  Body=json.dumps(kb, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    s3.put_object(Bucket=B, Key="data/methodology-gaps.json",
                  Body=json.dumps({
                      "generated_at": kb["generated_at"],
                      "n_gaps": len(gaps),
                      "note": ("indicators the teachers run that "
                               "our fleet's vocabulary does not "
                               "-- ranked by cross-teacher "
                               "consensus; each is a shadow-"
                               "candidate proposal"),
                      "gaps": gaps[:120]},
                      default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    OURS_SRC = {"FMP": "core fundamentals+prices",
                "FRED": "macro", "SEC EDGAR": "filings",
                "EDGAR": "filings", "FINRA": "short interest",
                "CoinMetrics": "crypto", "Polygon": "options",
                "Yahoo Finance": "prices", "IMF": "gov",
                "OECD": "gov", "ECB": "gov", "BIS": "gov",
                "World Bank": "gov", "Eurostat": "gov"}
    PRICING = {"Alpha Vantage": ("FREE", 0, "free key; 25 req/d"),
               "Stooq": ("FREE", 0, "free EOD CSV, no key"),
               "CoinGecko": ("FREE", 0, "free tier 10k/mo"),
               "Finnhub": ("FREEMIUM", 0,
                           "free tier 60/min; paid $50+"),
               "Tiingo": ("CHEAP", 10,
                          "$10/mo EOD+news+fundamentals"),
               "EODHD": ("CHEAP", 20,
                         "$20/mo global EOD+fundamentals"),
               "Polygon.io": ("CHEAP", 29,
                              "$29/mo stocks starter; "
                              "options higher"),
               "Polygon": ("CHEAP", 29, "see Polygon.io"),
               "Intrinio": ("PAID", 150, "institutional"),
               "Quandl": ("MIXED", 0,
                          "Nasdaq Data Link; many free sets"),
               "Nasdaq Data": ("MIXED", 0, "see Quandl"),
               "Glassnode": ("PAID", 39, "on-chain tiers"),
               "Kaiko": ("PAID", 500, "institutional crypto"),
               "Refinitiv": ("PAID", 1000, "enterprise"),
               "Bloomberg": ("PAID", 2000, "terminal"),
               "Barchart": ("PAID", 84, "onDemand APIs"),
               "Alpaca": ("FREE", 0, "free IEX-feed data w/ "
                                     "brokerage acct"),
               "Binance": ("FREE", 0, "free public API"),
               "Coinbase": ("FREE", 0, "free public API"),
               "Ken French": ("FREE", 0, "factor library CSV"),
               "Wharton": ("PAID", 0, "academic access"),
               "IEX Cloud": ("DEAD", 0, "shut down 2024")}
    PROBE = {"Stooq": "https://stooq.com/q/d/l/?s=aapl.us&i=d",
             "CoinGecko":
                 "https://api.coingecko.com/api/v3/ping",
             "Ken French":
                 "https://mba.tuck.dartmouth.edu/pages/faculty/"
                 "ken.french/data_library.html"}
    intel = []
    for x in kb_src:
        nm = x["source"]
        tier, usd, note = PRICING.get(
            nm, ("UNKNOWN", None, "verify pricing"))
        integrated = any(k.lower() in nm.lower()
                         or nm.lower() in k.lower()
                         for k in OURS_SRC)
        row = {"source": nm, "teachers": x["teachers"],
               "tier": tier, "usd_mo": usd, "note": note,
               "already_integrated": integrated}
        if not integrated and tier == "FREE" and nm in PROBE:
            try:
                rq = urllib.request.Request(PROBE[nm],
                                            headers=UA)
                code = urllib.request.urlopen(
                    rq, timeout=12).status
                row["probe"] = "LIVE(%s)" % code
            except Exception as e2:
                row["probe"] = "FAIL:" + str(e2)[:40]
        intel.append(row)
    free_unused = [r for r in intel
                   if r["tier"] == "FREE"
                   and not r["already_integrated"]]
    cheap = sorted((r for r in intel
                    if r["tier"] in ("CHEAP", "FREEMIUM")
                    and not r["already_integrated"]),
                   key=lambda r: (r["usd_mo"] or 0))
    s3.put_object(Bucket=B, Key="data/source-intel.json",
                  Body=json.dumps({
                      "generated_at": kb["generated_at"],
                      "pricing_asof": ("curated knowledge "
                                       "~Jan-2026; verify "
                                       "before purchase"),
                      "already_integrated":
                          [r for r in intel
                           if r["already_integrated"]],
                      "free_unused": free_unused,
                      "cheap_worth_it": cheap,
                      "all": intel}, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "indicators": len(kb_ind),
                      "src_free_unused": len(free_unused),
                      "src_cheap": len(cheap),
                      "sources": len(kb_src),
                      "gaps": len(gaps),
                      "teachers_ok": sum(1 for t in tlog
                                         if "error" not in t)}))
    return {"ok": True}
