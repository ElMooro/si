import json, os, boto3, urllib.request, math, traceback
from datetime import datetime, timezone, timedelta

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_BASE = "https://financialmodelingprep.com/stable"
POLY_BASE = "https://api.polygon.io"
S3_BUCKET = "justhodl-dashboard-live"

def http_get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"error": str(e)}

def fmp(endpoint, params=""):
    if params:
        return http_get(f"{FMP_BASE}/{endpoint}?{params}&apikey={FMP_KEY}")
    return http_get(f"{FMP_BASE}/{endpoint}?apikey={FMP_KEY}")

def poly(endpoint, params=""):
    sep = "&" if "?" in params else "?"
    return http_get(f"{POLY_BASE}/{endpoint}{sep}apiKey={POLYGON_KEY}")

def compute_sma(prices, period):
    if len(prices) < period: return None
    return round(sum(prices[:period]) / period, 2)

def compute_ema(prices, period):
    if len(prices) < period: return None
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in reversed(prices[:period]):
        ema = p * k + ema * (1 - k)
    return round(ema, 2)

def compute_rsi(prices, period=14):
    if len(prices) < period + 1: return None
    deltas = [prices[i] - prices[i+1] for i in range(len(prices)-1)]
    gains = [d for d in deltas[:period] if d > 0]
    losses = [-d for d in deltas[:period] if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)

def compute_macd(prices):
    if len(prices) < 26: return None, None, None
    ema12 = compute_ema(prices, 12)
    ema26 = compute_ema(prices, 26)
    if not ema12 or not ema26: return None, None, None
    return round(ema12 - ema26, 3), ema12, ema26

def compute_bollinger(prices, period=20):
    if len(prices) < period: return None, None, None
    sma = sum(prices[:period]) / period
    std = math.sqrt(sum((p - sma)**2 for p in prices[:period]) / period)
    return round(sma + 2*std, 2), round(sma, 2), round(sma - 2*std, 2)

def compute_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = []
    for i in range(period):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i+1]), abs(lows[i] - closes[i+1]))
        trs.append(tr)
    return round(sum(trs) / period, 2)

def compute_stoch_rsi(prices, period=14):
    if len(prices) < period * 2: return None
    rsi_vals = []
    for i in range(period):
        v = compute_rsi(prices[i:i+period+2], period)
        if v is not None: rsi_vals.append(v)
    if not rsi_vals: return None
    min_r = min(rsi_vals); max_r = max(rsi_vals)
    if max_r == min_r: return 50.0
    return round((rsi_vals[0] - min_r) / (max_r - min_r) * 100, 1)

def get_price_history(ticker, days=300):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days+60)
    data = poly(f"v2/aggs/ticker/{ticker}/range/1/day/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}",
                "adjusted=true&sort=desc&limit=400")
    if "error" in data or not data.get("results"):
        return [], [], [], []
    results = data["results"]
    return [r["c"] for r in results], [r["h"] for r in results], [r["l"] for r in results], [r["v"] for r in results]

def get_technicals(ticker):
    closes, highs, lows, vols = get_price_history(ticker)
    if not closes: return {"error": "No price data"}
    price  = closes[0]
    # Try to get live price from FMP quote
    try:
        q = fmp("quote", f"symbol={ticker}")
        if isinstance(q, list) and q and q[0].get("price"):
            price = float(q[0]["price"])
    except: pass
    sma20  = compute_sma(closes, 20)
    sma50  = compute_sma(closes, 50)
    sma200 = compute_sma(closes, 200)
    ema12  = compute_ema(closes, 12)
    ema26  = compute_ema(closes, 26)
    rsi    = compute_rsi(closes)
    macd, _, _ = compute_macd(closes)
    bb_upper, bb_mid, bb_lower = compute_bollinger(closes)
    atr    = compute_atr(highs, lows, closes) if len(highs) > 14 else None
    stoch  = compute_stoch_rsi(closes)
    wk52_high = round(max(highs[:252]), 2) if len(highs) >= 252 else round(max(highs), 2)
    wk52_low  = round(min(lows[:252]),  2) if len(lows)  >= 252 else round(min(lows),  2)
    avg_vol   = int(sum(vols[:20]) / 20) if len(vols) >= 20 else None
    vol_ratio = round(vols[0] / avg_vol, 2) if avg_vol and vols else None
    dist_200  = round((price / sma200 - 1) * 100, 1) if sma200 else None
    dist_50   = round((price / sma50  - 1) * 100, 1) if sma50  else None
    pct_1m  = round((price / closes[21]  - 1) * 100, 1) if len(closes) > 21  else None
    pct_3m  = round((price / closes[63]  - 1) * 100, 1) if len(closes) > 63  else None
    pct_6m  = round((price / closes[126] - 1) * 100, 1) if len(closes) > 126 else None
    pct_1y  = round((price / closes[252] - 1) * 100, 1) if len(closes) > 252 else None
    signals = []
    if rsi:
        if rsi < 30: signals.append("OVERSOLD")
        elif rsi > 70: signals.append("OVERBOUGHT")
    if sma200: signals.append("ABOVE_200MA" if price > sma200 else "BELOW_200MA")
    if sma50 and sma200: signals.append("GOLDEN_CROSS" if sma50 > sma200 else "DEATH_CROSS")
    if macd: signals.append("MACD_BULL" if macd > 0 else "MACD_BEAR")
    if bb_upper and bb_lower:
        if price > bb_upper: signals.append("BB_OVERBOUGHT")
        elif price < bb_lower: signals.append("BB_OVERSOLD")
    return {
        "price": price, "sma20": sma20, "sma50": sma50, "sma200": sma200,
        "ema12": ema12, "ema26": ema26, "rsi": rsi, "macd": macd,
        "stoch_rsi": stoch, "atr": atr,
        "bb_upper": bb_upper, "bb_mid": bb_mid, "bb_lower": bb_lower,
        "wk52_high": wk52_high, "wk52_low": wk52_low,
        "dist_200ma_pct": dist_200, "dist_50ma_pct": dist_50,
        "avg_vol_20d": avg_vol, "vol_ratio": vol_ratio,
        "pct_1m": pct_1m, "pct_3m": pct_3m, "pct_6m": pct_6m, "pct_1y": pct_1y,
        "signals": " | ".join(signals) if signals else "NEUTRAL",
        "above_200ma": price > sma200 if sma200 else None,
    }

def get_fundamentals(ticker):
    quote   = fmp("quote",                  f"symbol={ticker}")
    ratios  = fmp("ratios-ttm",             f"symbol={ticker}")
    metrics = fmp("key-metrics-ttm",        f"symbol={ticker}")
    profile = fmp("profile",               f"symbol={ticker}")
    pt      = fmp("price-target-consensus", f"symbol={ticker}")
    out = {}
    if isinstance(quote, list) and quote:
        q = quote[0]
        out.update({
            "name":       q.get("name"),
            "market_cap": q.get("marketCap"),
            "avg_volume": q.get("averageVolume") or q.get("avgVolume"),
            "exchange":   q.get("exchange"),
            "year_high":  q.get("yearHigh"),
            "year_low":   q.get("yearLow"),
            "eps":        q.get("eps"),
            "pe":         q.get("pe"),
        })
    if isinstance(ratios, list) and ratios:
        r = ratios[0]
        out.update({
            "pe_ttm":        r.get("priceToEarningsRatioTTM"),
            "pb_ttm":        r.get("priceToBookRatioTTM"),
            "ps_ttm":        r.get("priceToSalesRatioTTM"),
            "peg":           r.get("priceToEarningsGrowthRatioTTM"),
            "ev_ebitda":     r.get("evToEBITDATTM"),
            "roe":           round(r["returnOnEquityTTM"]*100,1)        if r.get("returnOnEquityTTM")  else None,
            "roa":           round(r["returnOnAssetsTTM"]*100,1)        if r.get("returnOnAssetsTTM")  else None,
            "profit_margin": round(r["netProfitMarginTTM"]*100,1)       if r.get("netProfitMarginTTM") else None,
            "gross_margin":  round(r["grossProfitMarginTTM"]*100,1)     if r.get("grossProfitMarginTTM") else None,
            "debt_equity":   r.get("debtToEquityRatioTTM"),
            "current_ratio": r.get("currentRatioTTM"),
            "fcf_yield":     round(r["priceToFreeCashFlowRatioTTM"],2)  if r.get("priceToFreeCashFlowRatioTTM") else None,
        })
    if isinstance(metrics, list) and metrics:
        m = metrics[0]
        out.update({
            "ev":              m.get("enterpriseValueTTM"),
            "ev_ebitda":       m.get("evToEBITDATTM"),
            "roe":             round(m["returnOnEquityTTM"]*100,1)      if m.get("returnOnEquityTTM")  else None,
            "roa":             round(m["returnOnAssetsTTM"]*100,1)      if m.get("returnOnAssetsTTM")  else None,
            "fcf_yield":       round(m["freeCashFlowYieldTTM"]*100,2)   if m.get("freeCashFlowYieldTTM") else None,
            "roic":            round(m["returnOnInvestedCapitalTTM"]*100,1) if m.get("returnOnInvestedCapitalTTM") else None,
            "earnings_yield":  round(m["earningsYieldTTM"]*100,2)       if m.get("earningsYieldTTM") else None,
        })
    if isinstance(profile, list) and profile:
        p = profile[0]
        out.update({
            "name":        p.get("companyName") or out.get("name"),
            "sector":      p.get("sector"),
            "industry":    p.get("industry"),
            "employees":   p.get("fullTimeEmployees"),
            "ceo":         p.get("ceo"),
            "beta":        p.get("beta"),
            "description": (p.get("description") or "")[:300],
            "market_cap":  p.get("marketCap") or out.get("market_cap"),
            "last_div":    p.get("lastDividend"),
            "website":     p.get("website"),
            "country":     p.get("country"),
        })
    if isinstance(pt, list) and pt:
        p = pt[0]
        out.update({
            "pt_high":      p.get("targetHigh"),
            "pt_low":       p.get("targetLow"),
            "pt_mean":      p.get("targetConsensus"),
            "pt_median":    p.get("targetMedian"),
            "pt_consensus": p.get("targetConsensus"),
        })
    return out

def get_earnings(ticker):
    data = fmp("earnings", f"symbol={ticker}&limit=8")
    if not isinstance(data, list): return []
    out = []
    for e in data[:8]:
        eps_est = e.get("epsEstimated")
        eps_act = e.get("epsActual")
        surprise = None
        if eps_est and eps_act:
            try: surprise = round((float(eps_act) - float(eps_est)) / abs(float(eps_est)) * 100, 1)
            except: pass
        out.append({"date": e.get("date"), "eps_est": eps_est, "eps_act": eps_act,
                    "surprise_pct": surprise, "revenue": e.get("revenueActual"),
                    "revenue_est": e.get("revenueEstimated")})
    return out

def get_dividends(ticker):
    data = fmp("dividends", f"symbol={ticker}&limit=8")
    if not isinstance(data, list): return []
    return [{"date": d.get("date"), "dividend": d.get("dividend")} for d in data[:8]]

def get_income_trend(ticker):
    data = fmp("income-statement", f"symbol={ticker}&limit=4")
    if not isinstance(data, list): return []
    return [{"date": d.get("date"), "revenue": d.get("revenue"),
             "net_income": d.get("netIncome") or d.get("netIncomeFromContinuingOperations"),
             "ebitda": d.get("ebitda"), "gross_profit": d.get("grossProfit"),
             "operating_income": d.get("operatingIncome"), "eps": d.get("eps")} for d in data[:4]]

def get_balance_sheet(ticker):
    data = fmp("balance-sheet-statement", f"symbol={ticker}&limit=2")
    if not isinstance(data, list) or not data: return {}
    b = data[0]
    total_debt = (b.get("longTermDebt") or 0) + (b.get("shortTermDebt") or b.get("shortTermNetDebtIssuance") or 0)
    return {
        "total_assets":  b.get("totalAssets"),
        "total_debt":    b.get("totalDebt") or total_debt,
        "cash":          b.get("cashAndCashEquivalents"),
        "cash_and_st":   b.get("cashAndShortTermInvestments"),
        "total_equity":  b.get("totalStockholdersEquity") or b.get("totalEquity"),
        "net_debt":      b.get("netDebt"),
        "total_current_assets": b.get("totalCurrentAssets"),
        "total_liabilities": b.get("totalLiabilities"),
    }

def get_cash_flow(ticker):
    data = fmp("cash-flow-statement", f"symbol={ticker}&limit=2")
    if not isinstance(data, list) or not data: return {}
    c = data[0]
    return {
        "operating_cf":  c.get("netCashProvidedByOperatingActivities") or c.get("operatingCashFlow"),
        "free_cf":       c.get("freeCashFlow") or (
            (c.get("netCashProvidedByOperatingActivities") or 0) +
            (c.get("investmentsInPropertyPlantAndEquipment") or 0)
        ),
        "capex":         c.get("investmentsInPropertyPlantAndEquipment") or c.get("capitalExpenditure"),
        "dividends_paid":c.get("dividendsPaid"),
        "buybacks":      c.get("netCommonStockIssuance") or c.get("commonStockRepurchased"),
        "net_income":    c.get("netIncome"),
    }

def get_historical_annual(ticker):
    """Fetch 25 years of annual fundamentals for charting"""
    income  = fmp("income-statement", f"symbol={ticker}&period=annual&limit=25")
    ratios  = fmp("ratios",           f"symbol={ticker}&period=annual&limit=25")
    metrics = fmp("key-metrics",      f"symbol={ticker}&period=annual&limit=25")
    divs    = fmp("dividends",        f"symbol={ticker}&limit=100")

    out = {"labels":[], "revenue":[], "net_income":[], "eps":[],
           "gross_margin":[], "net_margin":[], "pe":[], "pb":[],
           "roe":[], "fcf":[], "div_annual":[], "div_labels":[], "div_per_share":[]}

    # Income statement - reverse to oldest first
    if isinstance(income, list) and income:
        for row in reversed(income):
            yr = (row.get("date") or "")[:4]
            if yr not in out["labels"]:
                out["labels"].append(yr)
                out["revenue"].append(round(row["revenue"]/1e9,2) if row.get("revenue") else None)
                out["net_income"].append(round((row.get("netIncome") or row.get("netIncomeFromContinuingOperations") or 0)/1e9,2) if row.get("netIncome") or row.get("netIncomeFromContinuingOperations") else None)
                out["eps"].append(round(row["eps"],2) if row.get("eps") else None)

    # Ratios - map by year
    if isinstance(ratios, list) and ratios:
        rmap = {(r.get("date") or "")[:4]: r for r in ratios}
        for yr in out["labels"]:
            r = rmap.get(yr, {})
            out["gross_margin"].append(round(r["grossProfitMargin"]*100,1) if r.get("grossProfitMargin") else None)
            out["net_margin"].append(round(r["netProfitMargin"]*100,1) if r.get("netProfitMargin") else None)
            out["pe"].append(round(r["priceToEarningsRatio"],1) if r.get("priceToEarningsRatio") else None)
            out["pb"].append(round(r["priceToBookRatio"],1) if r.get("priceToBookRatio") else None)
            out["fcf"].append(round(r["freeCashFlowPerShare"],2) if r.get("freeCashFlowPerShare") else None)
            out["div_per_share"].append(round(r["dividendPerShare"],3) if r.get("dividendPerShare") else None)

    # Key metrics ROE
    if isinstance(metrics, list) and metrics:
        mmap = {(m.get("date") or "")[:4]: m for m in metrics}
        for i, yr in enumerate(out["labels"]):
            m = mmap.get(yr, {})
            out["roe"].append(round(m["returnOnEquity"]*100,1) if m.get("returnOnEquity") else None)

    # Annual dividends aggregated by year
    if isinstance(divs, list) and divs:
        div_by_year = {}
        for d in divs:
            yr = (d.get("date") or "")[:4]
            if yr:
                div_by_year[yr] = round(div_by_year.get(yr,0) + (d.get("adjDividend") or d.get("dividend") or 0), 3)
        sorted_yrs = sorted(div_by_year.keys())
        out["div_labels"] = sorted_yrs
        out["div_annual"] = [div_by_year[y] for y in sorted_yrs]

    return out

def get_ohlcv(ticker):
    """Fetch weekly OHLCV from Alpha Vantage back to 1999 for candlestick charting"""
    try:
        AV_KEY = "EOLGKSGAYZUXKPUL"
        av_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={ticker}&apikey={AV_KEY}"
        req = urllib.request.Request(av_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        ts = data.get("Weekly Adjusted Time Series", {})
        if not ts:
            return {"error": "no data from Alpha Vantage"}
        # Sort oldest first
        sorted_dates = sorted(ts.keys())
        dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
        for d in sorted_dates:
            row = ts[d]
            dates.append(d)
            opens.append(float(row.get("1. open", 0)))
            highs.append(float(row.get("2. high", 0)))
            lows.append(float(row.get("3. low", 0)))
            closes.append(float(row.get("5. adjusted close", row.get("4. close", 0))))
            volumes.append(int(row.get("6. volume", 0)))
        def sma(arr, n):
            out = [None] * len(arr)
            for i in range(n-1, len(arr)):
                out[i] = round(sum(arr[i-n+1:i+1]) / n, 2)
            return out
        return {
            "dates":   dates,
            "opens":   opens,
            "highs":   highs,
            "lows":    lows,
            "closes":  closes,
            "volumes": volumes,
            "sma50":   sma(closes, 50),
            "sma100":  sma(closes, 100),
            "sma200":  sma(closes, 200),
            "count":   len(dates)
        }
    except Exception as e:
        return {"error": str(e)}

def get_news(ticker):
    import urllib.parse
    data = poly("v2/reference/news", f"ticker={ticker}&limit=8&order=desc&sort=published_utc")
    if not isinstance(data, dict) or not data.get("results"): return []
    # Filter to only news that actually mentions our ticker
    results = [n for n in data["results"] if ticker in (n.get("tickers") or [])]
    if not results: results = data["results"]  # fallback to all if none match
    return [{"title": n.get("title"), "published": n.get("published_utc", "")[:10],
             "source": n.get("publisher", {}).get("name", ""), "url": n.get("article_url","")} for n in results[:5]]

def get_market_breadth():
    out = {}
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        report = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/report.json")["Body"].read())
        ath = report.get("ath_breakouts", {})
        out["at_ath"]        = ath.get("total_at_ath")
        out["near_ath"]      = ath.get("total_near_ath")
        out["ath_breakouts"] = len(ath.get("breakouts", []))
        stocks = report.get("stocks", {})
        out["above_200ma"]   = sum(1 for v in stocks.values() if isinstance(v, dict) and v.get("above_200ma"))
        out["total_tracked"] = len(stocks)
        ki = report.get("khalid_index", {})
        out["market_regime"] = ki.get("regime")
        out["khalid_score"]  = ki.get("score")
    except Exception as e:
        out["report_error"] = str(e)
    gainers = fmp("biggest-gainers")
    losers  = fmp("biggest-losers")
    if isinstance(gainers, list):
        out["top_gainers"] = [{"symbol": g.get("symbol"), "chg_pct": g.get("changesPercentage"), "price": g.get("price")} for g in gainers[:5]]
    if isinstance(losers, list):
        out["top_losers"]  = [{"symbol": l.get("symbol"), "chg_pct": l.get("changesPercentage"), "price": l.get("price")} for l in losers[:5]]
    return out

def full_stock_analysis(ticker):
    ticker = ticker.upper().strip()
    return {
        "ticker": ticker,
        "name": "",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "technicals":   get_technicals(ticker),
        "fundamentals": get_fundamentals(ticker),
        "earnings":     get_earnings(ticker),
        "dividends":    get_dividends(ticker),
        "income_trend": get_income_trend(ticker),
        "balance_sheet":get_balance_sheet(ticker),
        "cash_flow":    get_cash_flow(ticker),
        "news":         get_news(ticker),
        "historical":   get_historical_annual(ticker),
        "ohlcv":        get_ohlcv(ticker),
    }

def lambda_handler(event, context):
    CORS_HEADERS = {
        "Content-Type": "application/json"
    }
    # Handle CORS preflight
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS" or event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}
    try:
        qs = event.get("queryStringParameters") or {}
        body = event.get("body", "{}")
        if isinstance(body, str):
            try: body = json.loads(body)
            except: body = {}
        if not isinstance(body, dict): body = {}
        ticker = qs.get("ticker") or qs.get("symbol") or body.get("ticker") or body.get("symbol")
        if qs.get("breadth") == "true" or body.get("breadth"):
            return {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps(get_market_breadth())}
        if not ticker:
            return {"statusCode": 400, "headers": CORS_HEADERS, "body": json.dumps({"error": "ticker required. Use ?ticker=AAPL"})}
        # Serve from S3 cache if < 4 hours old
        force = qs.get("force") == "true" or body.get("force")
        if not force:
            try:
                s3c = boto3.client("s3", region_name="us-east-1")
                obj = s3c.get_object(Bucket=S3_BUCKET, Key=f"stock-analysis/{ticker.upper()}.json")
                from datetime import datetime, timezone, timedelta
                last_mod = obj["LastModified"]
                age = datetime.now(timezone.utc) - last_mod
                if age < timedelta(hours=4):
                    cached = json.loads(obj["Body"].read())
                    cached["_cached"] = True
                    cached["_age_minutes"] = int(age.total_seconds()/60)
                    return {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps(cached)}
            except: pass
        result = full_stock_analysis(ticker)
        result["name"] = result["fundamentals"].get("name", ticker)
        try:
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.put_object(Bucket=S3_BUCKET, Key=f"stock-analysis/{ticker}.json",
                Body=json.dumps(result), ContentType="application/json")
        except: pass
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps(result)}
    except Exception as e:
        return {"statusCode": 500, "headers": CORS_HEADERS,
                "body": json.dumps({"error": str(e), "trace": traceback.format_exc()[-500:]})}
