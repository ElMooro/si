
"""
JUSTHODL FINANCIAL SECRETARY v2.2
Self-Hosted AI Financial Analyst & Personal Portfolio Secretary
AWS: 857687956942 | Region: us-east-1

v2 changes over v1:
  - Claude model bumped to haiku-4-5-20251001
  - ATR-based volatility-adjusted price targets (not fixed ±12%/±7%)
  - Ticker-specific rationale (not generic "risk-on favored")
  - CFTC positioning data fed into AI prompt + recommendation logic
  - Yesterday vs today deltas on liquidity, risk, top picks
  - Per-field freshness timestamps (FRED lag vs Polygon real-time)
  - Accountability block: how yesterday's picks performed
"""
import json, boto3, os, ssl, time, math, statistics, traceback, hashlib
import urllib.request, urllib.error
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

FRED_KEY = os.environ.get("FRED_API_KEY", "")
POLY_KEY = os.environ.get("POLYGON_API_KEY", "")
AV_KEY   = os.environ.get("ALPHAVANTAGE_KEY", "")
NEWS_KEY = os.environ.get("NEWS_API_KEY", "")
CMC_KEY  = os.environ.get("CMC_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
EMAIL_TO   = os.environ.get("EMAIL_TO", "raafouis@gmail.com")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "raafouis@gmail.com")

s3  = boto3.client("s3")
ses = boto3.client("ses", region_name="us-east-1")
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def http_get(url, headers=None, timeout=15):
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"HTTP ERR: {url[:80]} -> {e}")
        return None


def http_post(url, data, headers=None, timeout=30):
    try:
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, headers=headers or {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"HTTP POST ERR: {url[:80]} -> {e}")
        return None


def _infer_fred_cadence_days(obs_list):
    """Given a list of FRED observations (dicts with 'date'), infer the
    typical publishing cadence in days. Returns None if not enough data."""
    if not obs_list or len(obs_list) < 2:
        return None
    # Observations are newest-first after reverse-sort in fetch_fred
    try:
        d0 = datetime.strptime(obs_list[0]["date"], "%Y-%m-%d")
        d1 = datetime.strptime(obs_list[1]["date"], "%Y-%m-%d")
        gap = (d0 - d1).days
    except Exception:
        return None
    if gap <= 0:
        return None
    # Look at up to 5 most recent gaps and take median to resist outliers
    gaps = [gap]
    for i in range(1, min(5, len(obs_list) - 1)):
        try:
            a = datetime.strptime(obs_list[i]["date"], "%Y-%m-%d")
            b = datetime.strptime(obs_list[i + 1]["date"], "%Y-%m-%d")
            g = (a - b).days
            if g > 0:
                gaps.append(g)
        except Exception:
            pass
    gaps.sort()
    return gaps[len(gaps) // 2]  # median


def _classify_cadence(days):
    """Round a gap (days) to its canonical cadence."""
    if days is None:
        return "unknown", 1  # treat as daily; we'll re-fetch
    if days <= 3:
        return "daily", 1
    if days <= 10:
        return "weekly", 7
    if days <= 45:
        return "monthly", 30
    if days <= 120:
        return "quarterly", 90
    if days <= 400:
        return "annual", 365
    return "annual+", 365


def _should_skip_fetch(cache_entry):
    """Return (skip_bool, reason_str). Implements smart TTL.

    Three conditions to skip:
      A. We fetched this series < 60 min ago (race-condition shield when
         multiple Lambdas run close together)
      B. Next expected FRED publication is still in the future
         (computed from latest_obs + inferred_cadence)
      C. Latest observation is from today (catch-all for daily series
         that just published)
    """
    if not cache_entry or not isinstance(cache_entry, list) or not cache_entry:
        return False, "no-cache"

    now_utc = datetime.now(timezone.utc)

    # Condition A: very recent fetch
    meta = cache_entry[0].get("_meta") if isinstance(cache_entry[0], dict) else None
    if isinstance(meta, dict):
        fetched_at = meta.get("fetched_at")
        if fetched_at:
            try:
                fa = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                if (now_utc - fa).total_seconds() < 3600:
                    return True, "recent-fetch"
            except Exception:
                pass

    # Condition B: next publication still in future
    latest = cache_entry[0]
    latest_date_str = latest.get("date") if isinstance(latest, dict) else None
    if not latest_date_str:
        return False, "no-date"
    try:
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return False, "bad-date"

    cadence_days = _infer_fred_cadence_days(cache_entry)
    label, canonical = _classify_cadence(cadence_days)
    # Next expected publication (+ 1 day buffer since FRED publishes at various hours)
    next_pub = latest_date + timedelta(days=canonical + 1)
    if now_utc < next_pub:
        return True, f"{label}-not-due-until-{next_pub.date()}"

    # Condition C: same-day catch
    today_et = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
    if latest_date_str >= today_et:
        return True, "today"

    return False, "due"



# ═══ FRED — 26 series ═══
FRED_SERIES = {
    "WALCL":"Fed Balance Sheet","RRPONTSYD":"Reverse Repo","WTREGEN":"TGA","WRESBAL":"Bank Reserves",
    "SOFR":"SOFR Rate","FEDFUNDS":"Fed Funds Rate","DGS2":"2Y Treasury","DGS10":"10Y Treasury",
    "DGS30":"30Y Treasury","T10Y2Y":"2s10s Spread","T10Y3M":"3m10Y Spread",
    "T5YIE":"5Y Breakeven","T10YIE":"10Y Breakeven",
    "BAMLH0A0HYM2":"HY Spread","BAMLC0A0CM":"IG Spread","BAMLC0A4CBBB":"BBB Spread",
    "VIXCLS":"VIX","STLFSI2":"St Louis Stress","NFCI":"Chicago FinCond",
    "UNRATE":"Unemployment","CPIAUCSL":"CPI","CPILFESL":"Core CPI","GDPC1":"Real GDP",
    "DTWEXBGS":"Dollar Index","DCOILWTICO":"WTI Crude",
    "NAPM":"ISM Manufacturing PMI",  # ACTUAL PMI, not employment count
}


def fetch_fred():
    """v2.2 — throttled, retrying, falls back to S3 cache on full failure."""
    results = {}
    import time as _time
    def _get(sid, nm, attempt=0):
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=60"
        try:
            req = urllib.request.Request(url, headers={})
            with urllib.request.urlopen(req, timeout=12, context=ctx) as r:
                d = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                _time.sleep(2 * (attempt + 1))
                return _get(sid, nm, attempt + 1)
            print(f"FRED {sid} HTTP {e.code}")
            return
        except Exception as e:
            print(f"FRED {sid} err: {e}")
            return
        if d and "observations" in d:
            obs = [o for o in d["observations"] if o.get("value", ".") != "."]
            if obs:
                val = float(obs[0]["value"])
                prev = float(obs[1]["value"]) if len(obs) > 1 else val
                prev_1m = float(obs[min(22, len(obs) - 1)]["value"]) if len(obs) > 22 else val
                results[sid] = {
                    "name": nm, "value": val, "prev": prev,
                    "chg_1d": round(val - prev, 4), "chg_1m": round(val - prev_1m, 4),
                    "date": obs[0]["date"],
                    "history": [float(o["value"]) for o in obs[:30] if o.get("value", ".") != "."],
                }
    # v3.2 — cache-first skip pass via smart TTL
    try:
        _cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        _fred_cache = json.loads(_cache_obj["Body"].read().decode())
    except Exception as _e:
        _fred_cache = {}
    _to_fetch = []
    _skipped = {"daily": 0, "weekly": 0, "monthly": 0, "quarterly": 0, "annual": 0, "annual+": 0, "recent-fetch": 0, "today": 0, "unknown": 0}
    for sid, nm in FRED_SERIES.items():
        cached = _fred_cache.get(sid)
        if cached:
            # Secretary cache entries are dict-shaped, not list-shaped. Convert
            # list-of-obs → dict-of-summary only applies here, but for the skip
            # check we need the list of observations. Store both when possible.
            obs_list = cached.get("history") if isinstance(cached, dict) else cached
            if obs_list and isinstance(obs_list, list) and obs_list and isinstance(obs_list[0], (int, float)):
                # secretary history is list of floats, no dates — skip check won't work
                obs_list = None
            elif isinstance(cached, list):
                obs_list = cached
            if obs_list:
                skip, reason = _should_skip_fetch(obs_list)
                if skip:
                    # Translate daily-report list format → secretary dict format
                    if isinstance(cached, list) and cached and isinstance(cached[0], dict):
                        latest = cached[0]
                        prev = cached[1] if len(cached) > 1 else latest
                        prev_1m = cached[min(22, len(cached) - 1)] if len(cached) > 22 else latest
                        results[sid] = {
                            "name": nm, "value": latest.get("value"),
                            "prev": prev.get("value"),
                            "chg_1d": round(latest.get("value", 0) - prev.get("value", 0), 4),
                            "chg_1m": round(latest.get("value", 0) - prev_1m.get("value", 0), 4),
                            "date": latest.get("date"),
                            "history": [o.get("value") for o in cached[:30]],
                            "_from_cache": True,
                        }
                    else:
                        results[sid] = dict(cached, _from_cache=True) if isinstance(cached, dict) else cached
                    # Tally reason (short-label: take part before '-')
                    _skipped[reason.split("-")[0] if "-" in reason else reason] = _skipped.get(reason.split("-")[0] if "-" in reason else reason, 0) + 1
                    continue
        _to_fetch.append((sid, nm))
    print(f"[FRED-v3.2] skipped {sum(_skipped.values())} via smart TTL ({_skipped}), fetching {len(_to_fetch)}")

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(_get, sid, nm): sid for sid, nm in _to_fetch}
        for f in as_completed(futs):
            f.result()

    # Cache fresh result to S3 for future fallback use
    if len(results) >= len(FRED_SERIES) * 0.7:  # at least 70% populated
        try:
            s3.put_object(
                Bucket=BUCKET, Key="data/fred-cache-secretary.json",
                Body=json.dumps(results, default=str).encode(),
                ContentType="application/json", CacheControl="max-age=1800",
            )
        except Exception as e:
            print(f"FRED cache write err: {e}")
    else:
        # Severe failure — fall back to cached copy from earlier runs
        print(f"FRED populated only {len(results)}/{len(FRED_SERIES)}, loading cache")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
            cached = json.loads(obj["Body"].read().decode())
            # Merge: prefer live if present, else cached
            for sid, data in cached.items():
                if sid not in results:
                    data["_from_cache"] = True
                    results[sid] = data
            print(f"FRED after cache merge: {len(results)}/{len(FRED_SERIES)}")
        except Exception as e:
            print(f"FRED cache read err: {e}")

    return results


# ═══ UNIVERSE ═══
UNIVERSE = {
    "AAPL":"Apple","MSFT":"Microsoft","GOOGL":"Alphabet","AMZN":"Amazon","NVDA":"NVIDIA",
    "META":"Meta","TSLA":"Tesla","BRK.B":"Berkshire","JPM":"JPMorgan","V":"Visa",
    "UNH":"UnitedHealth","MA":"Mastercard","HD":"Home Depot","PG":"Procter Gamble",
    "JNJ":"Johnson Johnson","COST":"Costco","ABBV":"AbbVie","CRM":"Salesforce",
    "NFLX":"Netflix","AMD":"AMD","LLY":"Eli Lilly","AVGO":"Broadcom","ORCL":"Oracle",
    "PLTR":"Palantir","COIN":"Coinbase","MSTR":"MicroStrategy","SMCI":"Super Micro",
    "ARM":"ARM Holdings","SNOW":"Snowflake","NET":"Cloudflare","CRWD":"CrowdStrike","PANW":"Palo Alto",
    "SPY":"S&P 500","QQQ":"Nasdaq 100","DIA":"Dow Jones","IWM":"Russell 2000",
    "VTI":"Total Market","EFA":"Intl Developed","EEM":"Emerging Markets",
    "XLK":"Tech","XLF":"Financials","XLE":"Energy","XLV":"Healthcare",
    "XLI":"Industrials","XLU":"Utilities","XLP":"Staples","XLY":"Discretionary",
    "XLB":"Materials","XLRE":"Real Estate",
    "TLT":"Long Treasury","IEF":"Mid Treasury","SHY":"Short Treasury",
    "LQD":"IG Corporate","HYG":"HY Corporate","TIP":"TIPS",
    "GLD":"Gold","SLV":"Silver","GDX":"Gold Miners","USO":"Oil",
    "UNG":"Natural Gas","DBA":"Agriculture","PPLT":"Platinum",
    "IBIT":"Bitcoin ETF","ETHA":"Ether ETF",
    "ARKK":"ARK Innovation","KWEB":"China Tech","XBI":"Biotech","HACK":"Cybersecurity","BOTZ":"Robotics AI",
}

ETF_TICKERS = {"SPY","QQQ","DIA","IWM","VTI","EFA","EEM","XLK","XLF","XLE","XLV","XLI","XLU","XLP","XLY","XLB","XLRE","TLT","IEF","SHY","LQD","HYG","TIP","GLD","SLV","GDX","USO","UNG","DBA","PPLT","IBIT","ETHA","ARKK","KWEB","XBI","HACK","BOTZ"}


def fetch_polygon_prices():
    results = {}
    tickers = list(UNIVERSE.keys())
    def _batch(batch):
        t_str = ",".join(batch)
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={t_str}&apiKey={POLY_KEY}"
        d = http_get(url, timeout=20)
        if d and "tickers" in d:
            for t in d["tickers"]:
                tk = t.get("ticker", "")
                day = t.get("day", {})
                prev = t.get("prevDay", {})
                price = day.get("c", 0) or t.get("lastTrade", {}).get("p", 0) or prev.get("c", 0)
                results[tk] = {
                    "name": UNIVERSE.get(tk, tk),
                    "price": price,
                    "open": day.get("o", 0),
                    "high": day.get("h", 0),
                    "low": day.get("l", 0),
                    "volume": day.get("v", 0),
                    "prev_close": prev.get("c", 0),
                    "change_pct": round(((day.get("c", 0) / prev.get("c", 1)) - 1) * 100, 2) if prev.get("c", 0) and day.get("c", 0) else 0,
                }
    for i in range(0, len(tickers), 30):
        _batch(tickers[i:i + 30])
    return results


def fetch_historical(ticker, days=365):
    end = datetime.now()
    start = end - timedelta(days=days)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&apiKey={POLY_KEY}"
    d = http_get(url, timeout=20)
    if d and "results" in d:
        return [{"date": datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d"), "open": bar["o"], "high": bar["h"], "low": bar["l"], "close": bar["c"], "volume": bar.get("v", 0)} for bar in d["results"]]
    return []


def fetch_financials(ticker, limit=4):
    url = f"https://api.polygon.io/vX/reference/financials?ticker={ticker}&limit={limit}&apiKey={POLY_KEY}"
    d = http_get(url, timeout=20)
    if not d or "results" not in d:
        return {"error": f"No financials for {ticker}"}
    income_statements = []
    balance_sheets = []
    for filing in d["results"]:
        period = filing.get("fiscal_period", "")
        year = filing.get("fiscal_year", "")
        inc = filing.get("financials", {}).get("income_statement", {})
        bal = filing.get("financials", {}).get("balance_sheet", {})
        if inc:
            income_statements.append({"period": f"{period} {year}", "revenue": inc.get("revenues", {}).get("value", 0), "cost_of_revenue": inc.get("cost_of_revenue", {}).get("value", 0), "gross_profit": inc.get("gross_profit", {}).get("value", 0), "operating_income": inc.get("operating_income_loss", {}).get("value", 0), "net_income": inc.get("net_income_loss", {}).get("value", 0), "eps_basic": inc.get("basic_earnings_per_share", {}).get("value", 0), "eps_diluted": inc.get("diluted_earnings_per_share", {}).get("value", 0)})
        if bal:
            balance_sheets.append({"period": f"{period} {year}", "total_assets": bal.get("assets", {}).get("value", 0), "total_liabilities": bal.get("liabilities", {}).get("value", 0), "equity": bal.get("equity", {}).get("value", 0), "cash": bal.get("current_assets", {}).get("value", 0), "total_debt": bal.get("noncurrent_liabilities", {}).get("value", 0)})
    return {"ticker": ticker, "income_statements": income_statements, "balance_sheets": balance_sheets, "quarters": len(income_statements)}


def fetch_company_news(ticker=None, limit=15):
    url = f"https://api.polygon.io/v2/reference/news?limit={limit}&apiKey={POLY_KEY}"
    if ticker:
        url += f"&ticker={ticker}"
    d = http_get(url, timeout=15)
    if d and "results" in d:
        return [{"title": a.get("title", ""), "source": a.get("publisher", {}).get("name", ""), "url": a.get("article_url", ""), "published": a.get("published_utc", ""), "tickers": a.get("tickers", []), "description": a.get("description", "")[:300]} for a in d["results"]]
    return []


def fetch_upcoming_earnings(tickers):
    """Return dict of {ticker: date_string} for earnings within next 14 days."""
    results = {}
    end = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
    start = datetime.now().strftime("%Y-%m-%d")
    for t in tickers[:15]:  # limit to avoid rate limits
        url = f"https://api.polygon.io/vX/reference/ipos?ticker={t}&apiKey={POLY_KEY}"
        # Polygon doesn't have a clean earnings-calendar endpoint on all tiers.
        # Use financials filings as a proxy (last known filing date + 90d cycle).
        d = http_get(f"https://api.polygon.io/vX/reference/financials?ticker={t}&limit=1&apiKey={POLY_KEY}", timeout=10)
        if d and d.get("results"):
            last = d["results"][0]
            fe = last.get("end_date", "")
            if fe:
                # Next earnings ~= last_end_date + ~90 days (public co quarterly cycle)
                try:
                    d_last = datetime.strptime(fe, "%Y-%m-%d")
                    estimated = (d_last + timedelta(days=95)).strftime("%Y-%m-%d")
                    if start <= estimated <= end:
                        results[t] = estimated
                except Exception:
                    pass
    return results


# ═══ CRYPTO ═══
def fetch_crypto(limit=50):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit={limit}&convert=USD"
    d = http_get(url, headers={"X-CMC_PRO_API_KEY": CMC_KEY}, timeout=15)
    if d and "data" in d:
        total_mc = sum(x["quote"]["USD"]["market_cap"] for x in d["data"]) or 1
        return [{"symbol": c["symbol"], "name": c["name"], "price": round(c["quote"]["USD"]["price"], 6), "market_cap": c["quote"]["USD"]["market_cap"], "volume_24h": c["quote"]["USD"]["volume_24h"], "change_1h": round(c["quote"]["USD"].get("percent_change_1h", 0), 2), "change_24h": round(c["quote"]["USD"].get("percent_change_24h", 0), 2), "change_7d": round(c["quote"]["USD"].get("percent_change_7d", 0), 2), "change_30d": round(c["quote"]["USD"].get("percent_change_30d", 0), 2), "rank": c["cmc_rank"], "dominance": round(c["quote"]["USD"]["market_cap"] / total_mc * 100, 2)} for c in d["data"]]
    return []


def fetch_crypto_price(symbol):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol={symbol.upper()}&convert=USD"
    d = http_get(url, headers={"X-CMC_PRO_API_KEY": CMC_KEY}, timeout=10)
    if d and "data" in d:
        coin = list(d["data"].values())[0]
        q = coin["quote"]["USD"]
        return {"symbol": coin["symbol"], "name": coin["name"], "price": q["price"], "market_cap": q["market_cap"], "volume_24h": q["volume_24h"], "change_1h": q.get("percent_change_1h", 0), "change_24h": q.get("percent_change_24h", 0), "change_7d": q.get("percent_change_7d", 0), "change_30d": q.get("percent_change_30d", 0), "change_90d": q.get("percent_change_90d", 0)}
    return None


def fetch_news(query=None):
    if query:
        url = f"https://newsapi.org/v2/everything?q={quote(query)}&sortBy=publishedAt&pageSize=10&apiKey={NEWS_KEY}"
    else:
        url = f"https://newsapi.org/v2/top-headlines?country=us&category=business&pageSize=15&apiKey={NEWS_KEY}"
    d = http_get(url, timeout=10)
    if d and "articles" in d:
        return [{"title": a.get("title", ""), "source": a.get("source", {}).get("name", ""), "description": a.get("description", ""), "url": a.get("url", ""), "published": a.get("publishedAt", "")} for a in d["articles"] if a.get("title")]
    return []


def fetch_fear_greed():
    d = http_get("https://api.alternative.me/fng/?limit=7")
    if d and "data" in d:
        return {"current": int(d["data"][0]["value"]), "label": d["data"][0]["value_classification"], "yesterday": int(d["data"][1]["value"]) if len(d["data"]) > 1 else None, "last_week": int(d["data"][6]["value"]) if len(d["data"]) > 6 else None}
    return {"current": 50, "label": "Neutral"}


def fetch_existing_data():
    results = {}
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        results["report"] = json.loads(obj["Body"].read().decode())
    except Exception:
        pass
    try:
        d = http_get("https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/analysis", timeout=15)
        if d:
            results["cftc"] = d
    except Exception:
        pass
    return results


def fetch_yesterday_snapshot():
    """Pull yesterday's secretary scan (if it exists) for deltas."""
    try:
        # Look for a scan from 20-30 hours ago
        yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d")
        paginator = s3.get_paginator("list_objects_v2")
        candidates = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix="data/secretary-history/"):
            for obj in page.get("Contents", []):
                if yesterday in obj["Key"]:
                    candidates.append(obj)
        if candidates:
            latest = max(candidates, key=lambda x: x["LastModified"])
            body = s3.get_object(Bucket=BUCKET, Key=latest["Key"])["Body"].read().decode()
            return json.loads(body)
    except Exception as e:
        print(f"yesterday snapshot fetch: {e}")
    return None


# ═══ v2.1 — TIER 2 PULLS ═══
def fetch_tier2():
    """Pull options-flow + crypto-intel from S3. Returns {'options', 'crypto', 'sector_rotation'}."""
    out = {"options": None, "crypto": None, "sector_rotation": None}
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="flow-data.json")
        fd = json.loads(obj["Body"].read().decode())
        data = fd.get("data", {})
        pc = data.get("put_call") or {}
        gex = data.get("gamma_exposure") or {}
        flows = data.get("fund_flows") or {}
        sentiment = data.get("sentiment") or {}
        signals = data.get("trading_signals") or []
        out["options"] = {
            "put_call_ratio": pc.get("total_put_call_ratio"),
            "pc_signal": pc.get("pc_signal", ""),
            "pc_description": pc.get("pc_description", ""),
            "gamma_regime": gex.get("regime", ""),
            "gamma_description": gex.get("description", ""),
            "max_gamma_strike": gex.get("max_gamma_strike"),
            "spy_price": gex.get("spy_price"),
            "total_gex": gex.get("total_gex"),
            "net_premium": pc.get("net_premium"),
            "trading_signals": [
                {
                    "type": s.get("type", ""),
                    "strength": s.get("strength", ""),
                    "message": s.get("message", "")[:140],
                    "confidence": s.get("confidence", 0),
                }
                for s in signals[:5]
            ],
            "sentiment_composite": (sentiment.get("composite") or {}).get("score"),
            "sentiment_label": (sentiment.get("composite") or {}).get("label", ""),
            "timestamp": fd.get("timestamp"),
        }
        out["sector_rotation"] = flows.get("sector_rotation") or {}
    except Exception as e:
        print(f"fetch options flow: {e}")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key="crypto-intel.json")
        ci = json.loads(obj["Body"].read().decode())
        gm = ci.get("global_market") or {}
        sc = ci.get("stablecoins") or {}
        fg = ci.get("fear_greed") or {}
        funding = ci.get("funding") or {}
        ratios = ci.get("onchain_ratios") or {}
        whale = ci.get("whale_txs") or {}
        top = (ci.get("top_coins") or {}).get("coins") or []
        out["crypto"] = {
            "btc_dominance": gm.get("btc_dominance"),
            "eth_dominance": gm.get("eth_dominance"),
            "total_mcap_fmt": gm.get("total_mcap_fmt"),
            "mcap_change_24h": gm.get("mcap_change_24h"),
            "total_volume_fmt": gm.get("total_volume_fmt"),
            "stablecoin_net_signal": sc.get("net_signal"),
            "stablecoin_minting": sc.get("minting_count"),
            "stablecoin_burning": sc.get("burning_count"),
            "fear_greed_value": (fg.get("current") if isinstance(fg, dict) else None),
            "fear_greed_label": (fg.get("label") if isinstance(fg, dict) else None) or (fg.get("classification") if isinstance(fg, dict) else None),
            "funding_summary": (funding.get("summary") if isinstance(funding, dict) else None) or funding,
            "mvrv_approx": (ratios.get("mvrv_approx") if isinstance(ratios, dict) else None),
            "risk_score": ci.get("risk_score"),
            "whale_count_24h": whale.get("whale_count"),
            "top_movers": [
                {"symbol": c.get("symbol"), "price": c.get("price"), "change_24h": c.get("change_24h")}
                for c in top[:10] if c.get("symbol")
            ],
            "timestamp": ci.get("generated_at"),
        }
    except Exception as e:
        print(f"fetch crypto intel: {e}")

    return out


def format_sector_rotation(sr):
    """v2.2 — live shape is {top_inflow, top_outflow, rotation_signal}."""
    if not sr or not isinstance(sr, dict):
        return [], []
    leaders = []
    laggards = []
    if sr.get("top_inflow"):
        name = sr.get("top_inflow_name") or sr.get("top_inflow")
        flow = sr.get("top_inflow_flow")
        if flow is not None:
            try:
                leaders.append(f"{name} (inflow ${float(flow):+.0f}M)")
            except Exception:
                leaders.append(f"{name}")
        else:
            leaders.append(f"{name}")
    if sr.get("top_outflow"):
        name = sr.get("top_outflow_name") or sr.get("top_outflow")
        flow = sr.get("top_outflow_flow")
        if flow is not None:
            try:
                laggards.append(f"{name} (outflow ${float(flow):+.0f}M)")
            except Exception:
                laggards.append(f"{name}")
        else:
            laggards.append(f"{name}")
    # Legacy: also check for list shape
    if not leaders and sr.get("leaders"):
        for e in (sr.get("leaders") or [])[:5]:
            if isinstance(e, dict):
                tkr = e.get("ticker") or e.get("symbol") or e.get("name", "?")
                chg = e.get("chg") or e.get("change_pct") or e.get("change")
                leaders.append(f"{tkr} {chg:+.2f}%" if chg is not None else str(tkr))
    if not laggards and sr.get("laggards"):
        for e in (sr.get("laggards") or [])[:5]:
            if isinstance(e, dict):
                tkr = e.get("ticker") or e.get("symbol") or e.get("name", "?")
                chg = e.get("chg") or e.get("change_pct") or e.get("change")
                laggards.append(f"{tkr} {chg:+.2f}%" if chg is not None else str(tkr))
    return leaders, laggards


# ═══ LIQUIDITY ═══
def calc_liquidity(fred):
    """v2.2 — returns regime=UNKNOWN instead of silently zeroing out."""
    walcl = fred.get("WALCL")
    rrp = fred.get("RRPONTSYD")
    tga = fred.get("WTREGEN")
    reserves = fred.get("WRESBAL")

    # If the core 3 are missing entirely, don't lie about the regime
    if not (walcl and rrp and tga):
        print(f"calc_liquidity: core series missing (walcl={bool(walcl)} rrp={bool(rrp)} tga={bool(tga)}) — returning UNKNOWN")
        return {
            "net_liquidity": None, "net_liq_change_1m": None, "regime": "UNKNOWN",
            "fed_balance_sheet": None, "rrp": None, "tga": None, "reserves": None,
            "sofr": (fred.get("SOFR") or {}).get("value"),
            "stress_index": (fred.get("STLFSI2") or {}).get("value"),
            "nfci": (fred.get("NFCI") or {}).get("value"),
            "error": "FRED data unavailable — likely rate-limit (429). Showing last-known regime would be misleading.",
            "components": {},
        }

    fed_bs = walcl.get("value", 0)
    rrp_v = rrp.get("value", 0)
    tga_v = tga.get("value", 0)
    reserves_v = (reserves or {}).get("value", 0)
    net_liq = (fed_bs - rrp_v - tga_v) / 1000 if fed_bs else 0
    fed_bs_chg = walcl.get("chg_1m", 0)
    rrp_chg = rrp.get("chg_1m", 0)
    tga_chg = tga.get("chg_1m", 0)
    net_liq_chg = (fed_bs_chg - rrp_chg - tga_chg) / 1000
    if net_liq_chg > 50:
        regime = "EXPANSION"
    elif net_liq_chg > 0:
        regime = "STABLE"
    elif net_liq_chg > -50:
        regime = "TIGHTENING"
    elif net_liq_chg > -200:
        regime = "CONTRACTION"
    else:
        regime = "CRISIS"
    return {
        "net_liquidity": round(net_liq, 1),
        "net_liq_change_1m": round(net_liq_chg, 1),
        "regime": regime,
        "fed_balance_sheet": round(fed_bs / 1000, 1),
        "rrp": round(rrp_v / 1000, 1),
        "tga": round(tga_v / 1000, 1),
        "reserves": round(reserves_v / 1000, 1),
        "sofr": (fred.get("SOFR") or {}).get("value", 0),
        "stress_index": (fred.get("STLFSI2") or {}).get("value", 0),
        "nfci": (fred.get("NFCI") or {}).get("value", 0),
        "components": {
            "fed_bs_trend": "expanding" if fed_bs_chg > 0 else "contracting",
            "rrp_trend": "draining" if rrp_chg < 0 else "building",
            "tga_trend": "drawing down" if tga_chg < 0 else "building up",
        },
    }


# ═══ RISK ═══
def calc_risk(fred, stocks):
    vix = fred.get("VIXCLS", {}).get("value", 20)
    hy = fred.get("BAMLH0A0HYM2", {}).get("value", 4)
    ig = fred.get("BAMLC0A0CM", {}).get("value", 1)
    s2s10 = fred.get("T10Y2Y", {}).get("value", 0)
    stress = fred.get("STLFSI2", {}).get("value", 0)
    unemp = fred.get("UNRATE", {}).get("value", 4)
    vs = min(100, max(0, (vix - 12) / 40 * 100))
    cs = min(100, max(0, (hy - 2.5) / 8 * 100))
    ys = min(100, max(0, (0.5 - s2s10) / 3 * 100))
    ss = min(100, max(0, (stress + 1) / 4 * 100))
    ls = min(100, max(0, (unemp - 3.5) / 5 * 100))
    spy = stocks.get("SPY", {})
    ms = min(100, max(0, 50 - (spy.get("change_pct", 0) if spy.get("price", 0) > 0 else 0) * 10))
    comp = vs * 0.25 + cs * 0.20 + ys * 0.15 + ss * 0.15 + ls * 0.10 + ms * 0.15
    level = "CRITICAL" if comp >= 75 else "ELEVATED" if comp >= 55 else "MODERATE" if comp >= 35 else "LOW"
    return {
        "composite": round(comp, 1),
        "level": level,
        "vix": vix,
        "hy_spread": hy,
        "ig_spread": ig,
        "yield_curve": s2s10,
        "stress_index": stress,
        "scores": {
            "volatility": round(vs, 1),
            "credit": round(cs, 1),
            "yield_curve": round(ys, 1),
            "financial_stress": round(ss, 1),
            "labor_market": round(ls, 1),
            "market_momentum": round(ms, 1),
        },
    }


# ═══ v2 — VOLATILITY-ADJUSTED TARGETS ═══
def estimate_vol_pct(ticker, data):
    """
    Rough 30-day volatility estimate expressed as % of price. Uses today's
    high/low range + open as a proxy since we don't always have OHLC history.
    For ETFs we use lower baseline vol. For memecoin/crypto we use higher.
    Returns (upside_pct, downside_pct) for a ~1-week horizon.
    """
    price = data.get("price", 0) or data.get("prev_close", 0) or 1
    day_high = data.get("high", price)
    day_low = data.get("low", price)
    intraday_range_pct = ((day_high - day_low) / price * 100) if price else 2.0

    if ticker in ETF_TICKERS:
        if ticker in ("TLT", "GLD", "USO", "ARKK", "KWEB"):
            base = 6.0  # moderately volatile ETFs
        elif ticker in ("SHY", "IEF", "TIP", "LQD", "HYG"):
            base = 2.5  # low-vol bond ETFs
        else:
            base = 4.5  # broad equity ETFs
    else:
        # Stocks — use intraday range as noise floor, add cap
        base = max(5.0, min(15.0, intraday_range_pct * 3.5))

    # Upside typically 1.8x downside on positive-skew momentum setups
    upside = round(base * 1.6, 1)
    downside = round(base, 1)
    return upside, downside


def compute_rel_strength(stocks):
    """For each stock, change_pct vs SPY's change_pct — relative strength today."""
    spy_chg = stocks.get("SPY", {}).get("change_pct", 0)
    out = {}
    for t, d in stocks.items():
        out[t] = round(d.get("change_pct", 0) - spy_chg, 2)
    return out


# ═══ v2 — TICKER-SPECIFIC RATIONALE ═══
def build_rationale(ticker, data, rel_str, fred, regime, cftc):
    """Return a list of specific, substantive reasons to own this ticker TODAY."""
    reasons = []
    chg = data.get("change_pct", 0)
    name = data.get("name", ticker)

    # Momentum
    if chg > 3:
        reasons.append(f"Strong day +{chg:.1f}%")
    elif chg > 1 and rel_str > 0.5:
        reasons.append(f"Outperforming SPY by {rel_str:+.1f}pp")
    elif -1 < chg < 1 and rel_str > 0.3:
        reasons.append(f"Quiet leadership vs SPY ({rel_str:+.1f}pp)")
    elif chg < -3:
        reasons.append(f"Oversold {chg:.1f}% (bounce candidate)")

    # Volume signal (if available) — unusually high volume = conviction
    vol = data.get("volume", 0)
    if vol > 0 and ticker in ("SPY", "QQQ", "IWM", "TLT", "GLD"):
        pass  # skip noise for mega-ETFs
    elif vol > 20_000_000:
        reasons.append("Heavy volume (>20M)")

    # Regime-specific (but make it ticker-aware, not templated)
    if regime == "EXPANSION":
        if ticker in ("NVDA", "AMD", "AVGO", "ARM", "SMCI"):
            reasons.append("AI infra beneficiary of liquidity expansion")
        elif ticker in ("IBIT", "ETHA", "COIN", "MSTR"):
            reasons.append("Crypto proxy — expansion lifts risk assets")
        elif ticker in ("ARKK", "XBI", "PLTR", "SNOW"):
            reasons.append("High-beta growth tuned to liquidity cycle")
        elif ticker in ("XLK", "QQQ"):
            reasons.append("Tech-heavy benchmark benefits from expansion")
    elif regime == "TIGHTENING" or regime == "CONTRACTION":
        if ticker in ("XLU", "XLP", "GLD", "TLT"):
            reasons.append("Defensive — typically resilient in tightening")
        elif ticker in ("QQQ", "ARKK", "IBIT"):
            reasons.append("High-beta — at risk in tightening cycle")
    elif regime == "CRISIS":
        if ticker in ("GLD", "TLT", "SHY"):
            reasons.append("Safe haven")

    # CFTC positioning — if commercials are net-long, that's institutional accumulation
    if cftc and isinstance(cftc, dict):
        contracts = cftc.get("contracts", {}) if isinstance(cftc.get("contracts"), dict) else {}
        # Map ticker → CFTC contract
        if ticker == "GLD" and "GOLD" in contracts:
            c = contracts.get("GOLD", {})
            if c.get("commercial_net_pct", 0) > 20:
                reasons.append(f"CFTC: commercials net long gold ({c.get('commercial_net_pct'):.0f}%)")
        elif ticker in ("USO", "XLE") and "CRUDE" in contracts:
            c = contracts.get("CRUDE", {})
            if c.get("commercial_net_pct", 0) > 20:
                reasons.append(f"CFTC: commercials net long crude")
        elif ticker in ("TLT", "IEF") and "T10Y" in contracts:
            c = contracts.get("T10Y", {})
            if c.get("commercial_net_pct", 0) > 20:
                reasons.append(f"CFTC: commercials net long 10Y")

    # VIX context
    vix = fred.get("VIXCLS", {}).get("value", 20)
    if vix > 25 and ticker in ("GLD", "TLT", "XLU"):
        reasons.append(f"VIX elevated ({vix:.0f}) favors defensives")
    elif vix < 15 and ticker in ("QQQ", "IBIT", "PLTR"):
        reasons.append(f"Low VIX ({vix:.0f}) = risk-on environment")

    # If we still don't have reasons, skip — no junk rationale
    return reasons


# ═══ v2 — RECOMMENDATIONS ═══
def generate_recommendations(fred, stocks, crypto, risk, liquidity, cftc):
    recs = []
    regime = liquidity["regime"]
    rel_str = compute_rel_strength(stocks)

    for ticker, data in stocks.items():
        price = data.get("price", 0) or data.get("prev_close", 0)
        if not price or price == 0:
            continue
        chg = data.get("change_pct", 0)

        # Volatility-adjusted targets
        up_pct, dn_pct = estimate_vol_pct(ticker, data)
        up = round(price * (1 + up_pct / 100), 2)
        dn = round(price * (1 - dn_pct / 100), 2)

        # Score
        score = 0
        if chg > 2:
            score += 20
        elif chg > 0.5:
            score += 10
        elif chg < -3:
            score += 12  # oversold bounce
        if rel_str.get(ticker, 0) > 1.0:
            score += 15
        if regime == "EXPANSION" and ticker in ("NVDA", "AMD", "PLTR", "COIN", "IBIT", "ARKK", "XLK"):
            score += 20
        elif regime in ("TIGHTENING", "CONTRACTION") and ticker in ("XLU", "XLP", "GLD", "TLT"):
            score += 20

        reasons = build_rationale(ticker, data, rel_str.get(ticker, 0), fred, regime, cftc)
        if not reasons:
            # If no substantive reasons, don't recommend
            continue

        action = "BUY" if score >= 20 else "WATCH" if score >= 10 else "AVOID"
        recs.append({
            "ticker": ticker,
            "name": data.get("name", ticker),
            "type": "etf" if ticker in ETF_TICKERS else "stock",
            "price": price,
            "change_pct": chg,
            "rel_strength_vs_spy": rel_str.get(ticker, 0),
            "score": score,
            "action": action,
            "upside_target": up,
            "downside_target": dn,
            "upside_pct": up_pct,
            "downside_pct": dn_pct,
            "risk_reward": round(up_pct / max(dn_pct, 0.1), 2),
            "reasons": reasons,
        })

    for coin in (crypto or [])[:25]:
        score = 0
        reasons = []
        if coin["change_7d"] > 10:
            score += 20
            reasons.append(f"Strong 7D +{coin['change_7d']:.1f}%")
        elif coin["change_7d"] > 5:
            score += 10
            reasons.append(f"Positive 7D +{coin['change_7d']:.1f}%")
        if coin["change_24h"] < -5:
            score += 12
            reasons.append(f"24h oversold {coin['change_24h']:.1f}%")
        if regime == "EXPANSION":
            score += 10
            reasons.append("Expansion phase lifts crypto")
        if not reasons:
            continue
        # Crypto: higher baseline vol
        up = round(coin["price"] * 1.25, 6)
        dn = round(coin["price"] * 0.85, 6)
        action = "BUY" if score >= 25 else "WATCH" if score >= 10 else "AVOID"
        recs.append({
            "ticker": coin["symbol"],
            "name": coin["name"],
            "type": "crypto",
            "price": coin["price"],
            "change_pct": coin["change_24h"],
            "score": score,
            "action": action,
            "upside_target": up,
            "downside_target": dn,
            "upside_pct": 25.0,
            "downside_pct": 15.0,
            "risk_reward": round(25 / 15, 2),
            "reasons": reasons,
            "market_cap": coin.get("market_cap", 0),
            "rank": coin.get("rank", 999),
        })

    recs.sort(key=lambda x: x["score"], reverse=True)
    return recs


# ═══ v2 — DELTAS vs YESTERDAY ═══
def build_deltas(today_liq, today_risk, today_recs, yesterday):
    if not yesterday:
        return {"available": False, "note": "No yesterday snapshot found yet. Deltas available starting tomorrow."}

    y_liq = yesterday.get("liquidity", {})
    y_risk = yesterday.get("risk", {})
    y_recs = yesterday.get("recommendations", [])

    y_top_tickers = {r["ticker"] for r in y_recs[:10] if r.get("action") == "BUY"}
    t_top_tickers = {r["ticker"] for r in today_recs[:10] if r.get("action") == "BUY"}

    # Yesterday's picks — did they work?
    yesterday_picks_perf = []
    today_stocks = {r["ticker"]: r for r in today_recs}
    for yp in y_recs[:5]:
        if yp.get("action") != "BUY":
            continue
        tkr = yp["ticker"]
        y_price = yp.get("price", 0)
        t_rec = today_stocks.get(tkr, {})
        t_price = t_rec.get("price", 0)
        if y_price and t_price:
            pct = round((t_price / y_price - 1) * 100, 2)
            yesterday_picks_perf.append({
                "ticker": tkr,
                "yesterday_price": y_price,
                "today_price": t_price,
                "pct": pct,
                "hit": pct > 0,
            })

    hit_rate = round(sum(1 for p in yesterday_picks_perf if p["hit"]) / max(len(yesterday_picks_perf), 1) * 100, 0)

    return {
        "available": True,
        "yesterday_date": yesterday.get("timestamp", ""),
        "net_liq_delta": round(today_liq["net_liquidity"] - y_liq.get("net_liquidity", today_liq["net_liquidity"]), 1),
        "risk_delta": round(today_risk["composite"] - y_risk.get("composite", today_risk["composite"]), 1),
        "regime_change": None if today_liq["regime"] == y_liq.get("regime") else f"{y_liq.get('regime')} → {today_liq['regime']}",
        "new_picks": sorted(t_top_tickers - y_top_tickers),
        "dropped_picks": sorted(y_top_tickers - t_top_tickers),
        "yesterday_picks_performance": yesterday_picks_perf,
        "hit_rate_pct": hit_rate,
    }


# ═══ CLAUDE ═══
def ask_claude(prompt, max_tokens=3000):
    # Try locked-down AI chat Lambda first (preferred — low token cost)
    result = http_post(
        "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/",
        {"message": prompt},
        headers={
            "Content-Type": "application/json",
            "Origin": "https://justhodl.ai",
            "x-justhodl-token": os.environ.get("AI_CHAT_TOKEN", ""),  # optional
        },
        timeout=90,
    )
    if result and result.get("response"):
        return result["response"]

    # Fallback — direct Anthropic with CURRENT model (was retired-claude-sonnet-4)
    if not ANTHROPIC_KEY:
        return "AI credits needed — add at console.anthropic.com/settings/billing"
    result2 = http_post(
        "https://api.anthropic.com/v1/messages",
        {"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens,
         "messages": [{"role": "user", "content": prompt}]},
        headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"},
        timeout=60,
    )
    if result2 and "content" in result2:
        return result2["content"][0].get("text", "")
    return "AI analysis unavailable"


def generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas, tier2=None):
    top_buys = [r for r in recs if r["action"] == "BUY"][:8]
    buy_str = "\n".join([f"  {r['ticker']}: ${r['price']:.2f} (chg:{r['change_pct']:+.1f}%, rs:{r.get('rel_strength_vs_spy', 0):+.1f}pp, up:{r['upside_pct']}%/dn:{r['downside_pct']}%) — {', '.join(r['reasons'][:2])}" for r in top_buys])
    crypto_str = "\n".join([f"  {c['symbol']}: ${c['price']:,.2f} (24h:{c['change_24h']:+.1f}%, 7d:{c['change_7d']:+.1f}%)" for c in (crypto or [])[:8]])
    news_str = "\n".join([f"  - {n['title']} ({n['source']})" for n in (news or [])[:5]])

    cftc_str = "Not available"
    if cftc and isinstance(cftc, dict):
        contracts = cftc.get("contracts") or {}
        positions = []
        for c_name in ("GOLD", "CRUDE", "T10Y", "SP500"):
            c = contracts.get(c_name) if isinstance(contracts, dict) else None
            if c and isinstance(c, dict):
                positions.append(f"{c_name}: comm={c.get('commercial_net_pct', '?')}%, lg={c.get('large_spec_net_pct', '?')}%")
        if positions:
            cftc_str = "; ".join(positions)

    deltas_str = ""
    if deltas.get("available"):
        deltas_str = (
            f"vs YESTERDAY: net_liq Δ ${deltas['net_liq_delta']:+.0f}B · "
            f"risk Δ {deltas['risk_delta']:+.1f} · "
            f"regime: {deltas.get('regime_change') or 'unchanged'} · "
            f"hit rate on prev picks: {deltas.get('hit_rate_pct', 0):.0f}%"
        )

    # v2.1 — tier 2 enrichment
    tier2_str = ""
    if tier2 and isinstance(tier2, dict):
        opts = tier2.get("options") or {}
        crypto_i = tier2.get("crypto") or {}
        pc = opts.get("put_call_ratio")
        pc_sig = opts.get("pc_signal", "")
        gex_regime = opts.get("gamma_regime", "")
        max_gex = opts.get("max_gamma_strike")
        spy_p = opts.get("spy_price")
        net_prem = opts.get("net_premium")
        tier2_str += f"\nOPTIONS FLOW: put/call={pc} ({pc_sig}) | gamma={gex_regime} | max-gamma strike=${max_gex} (SPY @ ${spy_p}) | net premium=${net_prem:,.0f}" if pc is not None else ""
        trsigs = opts.get("trading_signals") or []
        if trsigs:
            tier2_str += "\n  signals: " + " | ".join(f"{s['type']}({s['strength']})" for s in trsigs[:3])
        btc_dom = crypto_i.get("btc_dominance")
        mcap_chg = crypto_i.get("mcap_change_24h")
        fg_v = crypto_i.get("fear_greed_value")
        fg_l = crypto_i.get("fear_greed_label")
        sc_sig = crypto_i.get("stablecoin_net_signal")
        mvrv = crypto_i.get("mvrv_approx")
        crypto_risk = crypto_i.get("risk_score")
        if btc_dom is not None:
            tier2_str += f"\nCRYPTO: BTC_dom={btc_dom}% total_mcap_chg_24h={mcap_chg}% stablecoins={sc_sig} fear_greed={fg_v}({fg_l}) MVRV={mvrv} crypto_risk={crypto_risk}"
        # Sector rotation
        sr = tier2.get("sector_rotation") or {}
        ldrs, lags = format_sector_rotation(sr)
        if ldrs:
            tier2_str += f"\nSECTOR LEADERS: {', '.join(ldrs)}"
        if lags:
            tier2_str += f"\nSECTOR LAGGARDS: {', '.join(lags)}"

    prompt = f"""You are Khalid's personal financial secretary. Analyze REAL market data and give a **specific, data-driven** briefing. Do NOT reuse templated language. Each trade recommendation needs a unique rationale grounded in the numbers below.

LIQUIDITY: Net=${liq['net_liquidity']:,.0f}B Regime={liq['regime']} 1M_Chg=${liq['net_liq_change_1m']:+,.0f}B
  Fed=${liq['fed_balance_sheet']:,.0f}B RRP=${liq['rrp']:,.0f}B TGA=${liq['tga']:,.0f}B SOFR={liq['sofr']:.2f}%

RISK: {risk['composite']:.0f}/100 ({risk['level']}) VIX={risk['vix']:.1f} HY={risk['hy_spread']:.2f}% 2s10s={risk['yield_curve']:.2f}%

ISM PMI: {fred.get('NAPM', {}).get('value', 'N/A')} (ACTUAL PMI, not employment count)
CPI (latest): {fred.get('CPIAUCSL', {}).get('value', 'N/A')} · Core: {fred.get('CPILFESL', {}).get('value', 'N/A')}
DOLLAR: {fred.get('DTWEXBGS', {}).get('value', 'N/A')} · OIL: ${fred.get('DCOILWTICO', {}).get('value', 'N/A')}

CFTC POSITIONING: {cftc_str}{tier2_str}

{deltas_str}

TOP BUY CANDIDATES (v2 — vol-adjusted targets):
{buy_str}

CRYPTO:
{crypto_str}

NEWS:
{news_str}

Write your briefing in this exact structure:
1. **1-line VERDICT** — "BULLISH / NEUTRAL / BEARISH — single sentence why"
2. **LIQUIDITY** — 2-3 sentences. Call out any regime inflection.
3. **RISK** — 2-3 sentences including VIX, HY spread, curve.
4. **TOP 5 TRADES** — for each: {{ticker, entry range, first target, stop, 1-line thesis unique to this ticker's setup}}. NO templated reasons. Tie each to the specific rel-strength / volume / CFTC / regime numbers given above.
5. **WHAT CHANGED vs YESTERDAY** — use the deltas block to discuss evolution.
6. **PORTFOLIO ALLOCATION** — specific %. Include rationale if different from yesterday.
7. **RISK-OFF TRIGGERS** — 3 specific: if VIX breaches X, if DXY breaks Y, if HY widens past Z.

Be concrete with the actual numbers. You're Goldman Sachs PM, not a financial horoscope."""
    return ask_claude(prompt)


# ═══ EMAIL ═══
def send_email(subject, html_body):
    try:
        ses.send_email(
            Source=EMAIL_FROM,
            Destination={"ToAddresses": [EMAIL_TO]},
            Message={"Subject": {"Data": subject, "Charset": "UTF-8"},
                     "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}}},
        )
        return True
    except Exception as e:
        print(f"Email error: {e}")
        return False


def build_email_html(scan):
    liq = scan.get("liquidity", {})
    risk = scan.get("risk", {})
    recs = scan.get("recommendations", [])
    ai = scan.get("ai_briefing", "")
    ts = scan.get("timestamp", "")
    deltas = scan.get("deltas", {})
    top_buys = [r for r in recs if r["action"] == "BUY"][:15]

    rc = "#ff4444" if risk.get("level") == "CRITICAL" else "#ff8800" if risk.get("level") == "ELEVATED" else "#44cc44" if risk.get("level") == "LOW" else "#ffaa00"
    lc = "#44cc44" if liq.get("regime") in ("EXPANSION", "STABLE") else "#ff8800" if liq.get("regime") == "TIGHTENING" else "#ff4444"

    rows = "".join([
        f'<tr>'
        f'<td style="padding:6px;border-bottom:1px solid #333;color:#00ddff;font-weight:700">{r["ticker"]}</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333">{r["name"]}</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333;font-family:monospace">${r["price"]:,.2f}</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333;color:#00ff88">+{r["upside_pct"]}%</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333;color:#ff4444">-{r["downside_pct"]}%</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333;color:#00ddff">{r["risk_reward"]}x</td>'
        f'<td style="padding:6px;border-bottom:1px solid #333;font-size:11px">{", ".join(r["reasons"][:2])}</td>'
        f'</tr>'
        for r in top_buys
    ])
    ai_html = (ai or "").replace("\n", "<br>")

    deltas_html = ""
    if deltas.get("available"):
        dl = deltas
        regime_line = f"<li>Regime: <b>{dl.get('regime_change') or 'unchanged'}</b></li>" if dl.get("regime_change") else ""
        new_picks = ", ".join(dl.get("new_picks", [])) or "none"
        dropped = ", ".join(dl.get("dropped_picks", [])) or "none"
        picks_perf_lines = "".join([
            f"<li>{p['ticker']}: ${p['yesterday_price']:.2f} → ${p['today_price']:.2f} "
            f"<span style='color:{'#00ff88' if p['hit'] else '#ff4444'}'>({p['pct']:+.2f}%)</span></li>"
            for p in dl.get("yesterday_picks_performance", [])
        ]) or "<li>No picks yet from yesterday</li>"
        deltas_html = f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">VS YESTERDAY</h2>
  <ul style="line-height:1.6;font-size:13px">
    <li>Net Liquidity: <b>${dl['net_liq_delta']:+.0f}B</b></li>
    <li>Risk score: <b>{dl['risk_delta']:+.1f}</b></li>
    {regime_line}
    <li>New picks today: <b>{new_picks}</b></li>
    <li>Dropped: <b>{dropped}</b></li>
    <li>Hit rate on yesterday's top picks: <b>{dl.get('hit_rate_pct', 0):.0f}%</b></li>
  </ul>
  <div style="margin-top:12px;font-size:12px;color:#aaa">Yesterday's top picks performance:</div>
  <ul style="line-height:1.6;font-size:12px">{picks_perf_lines}</ul>
</div>
"""

    freshness = scan.get("data_freshness", {})
    freshness_html = ""
    if freshness:
        freshness_html = f"""
<div style="font-size:10px;color:#666;text-align:center;margin-top:16px">
Data freshness: stocks {freshness.get('stocks', '?')} · crypto {freshness.get('crypto', '?')} · FRED {freshness.get('fred_latest', '?')} (FRED series typically lag 1-2 days)
</div>
"""


    # v2.1 — tier 2 cards
    tier2 = scan.get("tier2") or {}
    opts = tier2.get("options") or {}
    crypto_i = tier2.get("crypto") or {}
    sr = tier2.get("sector_rotation") or {}

    tier2_html = ""
    if opts.get("put_call_ratio") is not None:
        pc = opts.get("put_call_ratio")
        pc_sig = opts.get("pc_signal", "") or "—"
        gex_reg = opts.get("gamma_regime", "") or "—"
        max_gex = opts.get("max_gamma_strike")
        spy_p = opts.get("spy_price")
        # Color put/call: < 0.7 = complacency (red for warning), > 1.2 = fear (green for hedged)
        pc_color = "#ff8800" if (pc is not None and pc < 0.5) else "#44cc44" if (pc is not None and pc > 1.0) else "#e0e0e0"
        gex_color = "#44cc44" if "POSITIVE" in str(gex_reg).upper() else "#ff8800" if "NEGATIVE" in str(gex_reg).upper() else "#e0e0e0"
        trsigs = opts.get("trading_signals") or []
        sig_lines = "".join([f"<li style='font-size:12px'><b>{s['type']}</b> ({s['strength']}): {s['message'][:100]}</li>" for s in trsigs[:4]]) or "<li style='font-size:12px;color:#888'>No active signals</li>"
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">OPTIONS FLOW</h2>
  <table style="width:100%;font-size:13px"><tr>
    <td style="padding:6px"><div style="color:#888;font-size:11px">PUT/CALL RATIO</div><div style="font-size:22px;font-weight:700;color:{pc_color}">{pc:.2f}</div><div style="font-size:11px;color:#888">{pc_sig}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">GAMMA REGIME</div><div style="font-size:18px;font-weight:700;color:{gex_color}">{gex_reg}</div><div style="font-size:11px;color:#888">Max gamma @ ${max_gex} · SPY ${spy_p}</div></td>
  </tr></table>
  <div style="margin-top:12px;font-size:12px;color:#aaa">Top signals:</div>
  <ul style="line-height:1.5">{sig_lines}</ul>
</div>
"""

    if crypto_i.get("btc_dominance") is not None:
        btc_dom = crypto_i.get("btc_dominance")
        mcap_chg = crypto_i.get("mcap_change_24h")
        fg_v = crypto_i.get("fear_greed_value") or "—"
        fg_l = crypto_i.get("fear_greed_label") or ""
        sc_sig = crypto_i.get("stablecoin_net_signal") or "—"
        sc_color = "#44cc44" if "INFLOW" in str(sc_sig).upper() else "#ff8800" if "OUTFLOW" in str(sc_sig).upper() else "#e0e0e0"
        mcap_color = "#44cc44" if (mcap_chg or 0) > 0 else "#ff4444"
        crypto_risk = crypto_i.get("risk_score") or "—"
        movers = crypto_i.get("top_movers") or []
        mover_rows = "".join([f"<tr><td style='padding:4px;color:#00ddff'>{m['symbol']}</td><td style='padding:4px;font-family:monospace'>${m['price']:,.4f}</td><td style='padding:4px;color:{'#00ff88' if (m.get('change_24h') or 0) > 0 else '#ff4444'}'>{(m.get('change_24h') or 0):+.2f}%</td></tr>" for m in movers[:8]])
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">CRYPTO INTEL</h2>
  <table style="width:100%;font-size:13px"><tr>
    <td style="padding:6px"><div style="color:#888;font-size:11px">BTC DOMINANCE</div><div style="font-size:22px;font-weight:700">{btc_dom:.1f}%</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">TOTAL MCAP 24h</div><div style="font-size:22px;font-weight:700;color:{mcap_color}">{(mcap_chg or 0):+.2f}%</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">FEAR/GREED</div><div style="font-size:22px;font-weight:700">{fg_v}</div><div style="font-size:11px;color:#888">{fg_l}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">STABLECOIN FLOW</div><div style="font-size:18px;font-weight:700;color:{sc_color}">{sc_sig}</div></td>
    <td style="padding:6px"><div style="color:#888;font-size:11px">RISK SCORE</div><div style="font-size:22px;font-weight:700">{crypto_risk}</div></td>
  </tr></table>
  <div style="margin-top:12px;font-size:12px;color:#aaa">Top coins:</div>
  <table style="width:100%;font-size:12px;border-collapse:collapse">{mover_rows}</table>
</div>
"""

    ldrs, lags = [], []
    if sr:
        try:
            ldrs = [f"{e.get('ticker') or e.get('symbol')} {(e.get('chg') or e.get('change_pct') or 0):+.2f}%" for e in (sr.get("leaders") or sr.get("top") or [])[:5] if isinstance(e, dict)]
            lags = [f"{e.get('ticker') or e.get('symbol')} {(e.get('chg') or e.get('change_pct') or 0):+.2f}%" for e in (sr.get("laggards") or sr.get("bottom") or [])[:5] if isinstance(e, dict)]
        except Exception:
            ldrs, lags = [], []

    if ldrs or lags:
        ldrs_html = "".join([f"<li style='color:#00ff88'>{l}</li>" for l in ldrs]) or "<li style='color:#888'>—</li>"
        lags_html = "".join([f"<li style='color:#ff4444'>{l}</li>" for l in lags]) or "<li style='color:#888'>—</li>"
        tier2_html += f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
  <h2 style="color:#00ddff">SECTOR ROTATION</h2>
  <table style="width:100%"><tr>
    <td style="padding:6px;vertical-align:top;width:50%"><div style="color:#00ff88;font-size:12px;margin-bottom:6px">LEADERS</div><ul style="line-height:1.6;font-size:13px">{ldrs_html}</ul></td>
    <td style="padding:6px;vertical-align:top;width:50%"><div style="color:#ff4444;font-size:12px;margin-bottom:6px">LAGGARDS</div><ul style="line-height:1.6;font-size:13px">{lags_html}</ul></td>
  </tr></table>
</div>
"""
    # v2.2 — safe display for when liquidity is UNKNOWN
    net_liq_val = liq.get("net_liquidity")
    if net_liq_val is None:
        net_liq_display = "N/A"
        regime_display = "UNKNOWN (data unavailable)"
    else:
        net_liq_display = f"${net_liq_val:,.0f}B"
        regime_display = liq.get("regime", "--")

    return f"""<!DOCTYPE html><html><body style="background:#0a0a0f;color:#e0e0e0;font-family:sans-serif;padding:20px">
<div style="max-width:800px;margin:0 auto">
<h1 style="color:#00ddff">JUSTHODL SECRETARY v2 | {ts}</h1>
<div style="display:flex;gap:12px;margin:20px 0">
<div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center">
<div style="color:#888;font-size:11px">NET LIQUIDITY</div>
<div style="font-size:24px;font-weight:700;color:{lc}">{net_liq_display}</div>
<div style="color:{lc}">{regime_display}</div>
</div>
<div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center">
<div style="color:#888;font-size:11px">RISK</div>
<div style="font-size:24px;font-weight:700;color:{rc}">{risk.get('composite', 0):.0f}/100</div>
<div style="color:{rc}">{risk.get('level', '--')}</div>
</div>
<div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center">
<div style="color:#888;font-size:11px">VIX</div>
<div style="font-size:24px;font-weight:700">{risk.get('vix', 0):.1f}</div>
</div>
</div>
{deltas_html}
{tier2_html}
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0">
<h2 style="color:#00ddff">AI ANALYSIS</h2>
<div style="font-size:13px;line-height:1.7">{ai_html}</div>
</div>
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px">
<h2 style="color:#00ddff">TOP RECOMMENDATIONS (vol-adjusted targets)</h2>
<table style="width:100%;border-collapse:collapse;font-size:12px">
<tr style="color:#888"><th style="padding:6px;text-align:left">Ticker</th><th>Name</th><th>Price</th><th>Upside</th><th>Downside</th><th>R:R</th><th>Why (this ticker specifically)</th></tr>
{rows}
</table>
</div>
{freshness_html}
</div>
</body></html>"""


# ═══ CHAT (same as v1, kept for API parity) ═══
def handle_chat(message):
    msg = message.lower().strip()
    if any(msg.startswith(p) for p in ("price ", "check ", "quote ")):
        ticker = msg.split()[-1].upper()
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLY_KEY}"
        d = http_get(url)
        if d and "ticker" in d:
            t = d["ticker"]
            day = t.get("day", {})
            prev = t.get("prevDay", {})
            chg = ((day.get("c", 0) / prev.get("c", 1)) - 1) * 100 if prev.get("c") else 0
            return {"type": "price", "ticker": ticker, "price": day.get("c", 0), "change": round(chg, 2), "high": day.get("h", 0), "low": day.get("l", 0), "volume": day.get("v", 0)}
        crypto_data = fetch_crypto_price(ticker)
        if crypto_data:
            return {"type": "crypto_price", "data": crypto_data}
        return {"type": "error", "message": f"Ticker {ticker} not found"}
    if any(k in msg for k in ("scan", "update", "refresh", "report")):
        return {"type": "scan_requested"}
    if any(k in msg for k in ("crypto", "bitcoin", "btc", "eth", "altcoin")):
        return {"type": "crypto", "data": fetch_crypto(25)}
    if ANTHROPIC_KEY:
        fred = fetch_fred()
        stocks = fetch_polygon_prices()
        crypto = fetch_crypto(15)
        liq = calc_liquidity(fred)
        risk = calc_risk(fred, stocks)
        spy = stocks.get("SPY", {})
        btc = next((c for c in crypto if c["symbol"] == "BTC"), {})
        ctx_str = f"SPY:${spy.get('price', 0):.2f}({spy.get('change_pct', 0):+.2f}%) BTC:${btc.get('price', 0):,.2f}({btc.get('change_24h', 0):+.2f}%) VIX:{risk['vix']:.1f} HY:{risk['hy_spread']:.2f}% NetLiq:${liq['net_liquidity']:,.0f}B({liq['regime']}) Risk:{risk['composite']:.0f}/100({risk['level']})\n\nUser question: {message}\n\nAnswer with real numbers. Be specific."
        return {"type": "ai_response", "message": ask_claude(ctx_str, max_tokens=2000)}
    return {"type": "error", "message": "Try: price AAPL, scan, crypto"}


# ═══ v2 — FULL SCAN ═══
def run_full_scan():
    start = time.time()
    print("=== v2 FULL MARKET SCAN ===")
    with ThreadPoolExecutor(max_workers=7) as ex:
        f_fred = ex.submit(fetch_fred)
        f_stocks = ex.submit(fetch_polygon_prices)
        f_crypto = ex.submit(fetch_crypto, 50)
        f_news = ex.submit(fetch_news)
        f_fg = ex.submit(fetch_fear_greed)
        f_existing = ex.submit(fetch_existing_data)
        f_yesterday = ex.submit(fetch_yesterday_snapshot)
        f_tier2 = ex.submit(fetch_tier2)
        fred = f_fred.result()
        stocks = f_stocks.result()
        crypto = f_crypto.result()
        news = f_news.result()
        fg = f_fg.result()
        existing = f_existing.result()
        yesterday = f_yesterday.result()
        tier2 = f_tier2.result()

    cftc = existing.get("cftc", {})
    print(f"  Data: FRED={len(fred)} Stocks={len(stocks)} Crypto={len(crypto)} News={len(news)} CFTC={bool(cftc)} Yesterday={bool(yesterday)}")

    liq = calc_liquidity(fred)
    risk = calc_risk(fred, stocks)
    recs = generate_recommendations(fred, stocks, crypto, risk, liq, cftc)
    deltas = build_deltas(liq, risk, recs, yesterday)
    buys = [r for r in recs if r["action"] == "BUY"]
    print(f"  Liq={liq['regime']} Risk={risk['composite']:.0f} Buys={len(buys)} Deltas={deltas.get('available')}")

    ai = generate_ai_briefing(liq, risk, recs, fred, crypto, news, cftc, deltas, tier2)
    print(f"  AI={len(ai)} chars")

    now = datetime.now(timezone(timedelta(hours=-5)))
    fred_latest_dates = [v.get("date", "") for v in fred.values() if v.get("date")]
    fred_latest = max(fred_latest_dates) if fred_latest_dates else "?"

    scan = {
        "version": "2.2",
        "type": "secretary_scan",
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S ET"),
        "scan_time_seconds": round(time.time() - start, 1),
        "liquidity": liq,
        "risk": risk,
        "fear_greed": fg,
        "recommendations": recs[:50],
        "top_buys": buys[:15],
        "ai_briefing": ai,
        "deltas": deltas,
        "fred": {k: {"name": v["name"], "value": v["value"], "chg_1d": v["chg_1d"], "date": v.get("date", "")} for k, v in fred.items()},
        "stocks_count": len(stocks),
        "crypto_count": len(crypto),
        "crypto_top10": crypto[:10],
        "news": news[:10],
        "market_snapshot": {
            "spy": stocks.get("SPY", {}), "qqq": stocks.get("QQQ", {}),
            "dia": stocks.get("DIA", {}), "iwm": stocks.get("IWM", {}),
            "gld": stocks.get("GLD", {}), "tlt": stocks.get("TLT", {}),
            "btc": next((c for c in crypto if c["symbol"] == "BTC"), {}),
            "eth": next((c for c in crypto if c["symbol"] == "ETH"), {}),
        },
        "cftc": cftc,
        "tier2": tier2,
        "data_freshness": {
            "stocks": "real-time (Polygon)",
            "crypto": "real-time (CoinMarketCap)",
            "fred_latest": fred_latest,
            "news": "real-time (NewsAPI)",
        },
    }
    s3.put_object(Bucket=BUCKET, Key="data/secretary-latest.json",
                  Body=json.dumps(scan, default=str),
                  ContentType="application/json", CacheControl="max-age=300")
    s3.put_object(Bucket=BUCKET, Key=f"data/secretary-history/{now.strftime('%Y-%m-%d_%H%M')}.json",
                  Body=json.dumps(scan, default=str),
                  ContentType="application/json")
    subj = f"Secretary v2.2: {liq['regime']} | Risk {risk['composite']:.0f} | {len(buys)} Buys | {now.strftime('%b %d %I:%M %p')}"
    send_email(subj, build_email_html(scan))
    print(f"=== DONE {time.time() - start:.1f}s ===")
    return scan


# ═══ LAMBDA HANDLER ═══
def lambda_handler(event, context):
    headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*",
               "Access-Control-Allow-Methods": "GET,POST,OPTIONS", "Access-Control-Allow-Headers": "Content-Type"}
    def respond(code, body):
        return {"statusCode": code, "headers": headers, "body": json.dumps(body, default=str)}

    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    if method == "OPTIONS":
        return respond(200, {"status": "ok"})
    if event.get("source") == "aws.events" or event.get("detail-type") == "Scheduled Event":
        scan = run_full_scan()
        return respond(200, {"status": "scan_complete", "regime": scan["liquidity"]["regime"], "risk": scan["risk"]["composite"], "buys": len(scan["top_buys"])})

    path = event.get("rawPath", "") or event.get("path", "")
    body = {}
    raw = event.get("body", "{}")
    if raw:
        try:
            if event.get("isBase64Encoded"):
                import base64
                raw = base64.b64decode(raw).decode()
            body = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            body = {}

    if path == "/latest" or (method == "GET" and not path.strip("/")):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
            return respond(200, json.loads(obj["Body"].read().decode()))
        except Exception:
            return respond(200, {"status": "no_scan_yet"})
    if path == "/scan" or body.get("action") == "scan":
        return respond(200, run_full_scan())
    if path == "/chat" or body.get("action") == "chat":
        msg = body.get("message", "")
        if not msg:
            return respond(400, {"error": "Missing message"})
        result = handle_chat(msg)
        if result.get("type") == "scan_requested":
            scan = run_full_scan()
            return respond(200, {"type": "scan_complete", "message": f"Scan done. {scan['liquidity']['regime']}, Risk {scan['risk']['composite']:.0f}, {len(scan['top_buys'])} buys.", "data": scan})
        return respond(200, result)
    if path.startswith("/price/"):
        return respond(200, handle_chat(f"price {path.split('/')[-1]}"))
    if path.startswith("/news/"):
        return respond(200, {"type": "news", "data": fetch_company_news(path.split("/")[-1].upper())})
    if path.startswith("/history/"):
        return respond(200, {"type": "historical", "ticker": path.split("/")[-1].upper(), "data": fetch_historical(path.split("/")[-1].upper(), int(body.get("days", 365)))})
    if path == "/crypto":
        return respond(200, {"type": "crypto", "data": fetch_crypto(50)})
    if path.startswith("/crypto/"):
        return respond(200, {"type": "crypto_price", "data": fetch_crypto_price(path.split("/")[-1].upper())})
    if path == "/recommendations":
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
            data = json.loads(obj["Body"].read().decode())
            return respond(200, {"recommendations": data.get("recommendations", [])})
        except Exception:
            return respond(200, {"status": "run_scan_first"})
    return respond(200, {"service": "JustHodl Financial Secretary v2.2",
                         "endpoints": {"GET /latest": "Latest scan", "POST /scan": "Force scan",
                                       "POST /chat": "Chat", "GET /price/TICKER": "Price",
                                       "GET /news/TICKER": "News", "GET /history/TICKER": "History",
                                       "GET /crypto": "Top 50", "GET /crypto/SYMBOL": "Crypto price",
                                       "GET /recommendations": "Signals"}})
