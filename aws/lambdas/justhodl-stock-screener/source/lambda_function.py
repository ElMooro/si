import json, time, boto3, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

FMP      = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE     = "https://financialmodelingprep.com/stable"
S3_BUCKET= "justhodl-dashboard-live"
CACHE_KEY= "screener/data.json"
CACHE_TTL= 4 * 3600
WORKERS  = 5

s3 = boto3.client("s3", region_name="us-east-1")

def sanitize_pe(v):
    try:
        f = float(v)
        if f <= 0 or f > 500:
            return None
        return round(f, 2)
    except:
        return None

def sanitize_ratio(v, lo=-50, hi=500):
    try:
        f = float(v)
        if f < lo or f > hi:
            return None
        return round(f, 4)
    except:
        return None


def fmp(path, params=""):
    url = f"{BASE}/{path}?apikey={FMP}{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        r   = urllib.request.urlopen(req, timeout=25)
        return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"  ERR {path}: {e}")
        return None

def sf(v):
    try: f=float(v); return round(f,4) if f==f else None
    except: return None

def sp(v):
    try: f=float(v); return round(f*100,2) if f==f else None
    except: return None

def sp2(v):
    try: f=float(v); return round(f,2) if f==f else None
    except: return None

# ── TECHNICAL INDICATORS ──────────────────────────
def get_price_history(symbol, days=300):
    """Fetch ~300 days of daily closes (most recent first)."""
    data = fmp("historical-price-eod/full", f"&symbol={symbol}")
    if not data or not isinstance(data, dict) or "historical" not in data:
        # FMP /stable returns a list directly for some endpoints; try alternate
        if isinstance(data, list):
            history = data
        else:
            return []
    else:
        history = data.get("historical", [])
    # Take just the closes
    closes = []
    for row in history[:days]:
        c = row.get("close") or row.get("adjClose")
        if c is not None:
            try:
                closes.append(float(c))
            except (ValueError, TypeError):
                pass
    return closes

def compute_sma(closes, period):
    """Simple moving average ending at index 0 (most recent)."""
    if len(closes) < period:
        return None
    return round(sum(closes[:period]) / period, 2)

def detect_cross(closes, lookback_days=60):
    """
    Detect golden/death cross within the last `lookback_days`.

    A golden cross = SMA50 crosses ABOVE SMA200.
    A death cross  = SMA50 crosses BELOW SMA200.

    Returns (signal, days_ago) where:
      signal: 'GOLDEN' | 'DEATH' | None
      days_ago: int days since cross, or None
    """
    if len(closes) < 200 + lookback_days:
        return None, None

    # closes[0] = most recent. To compute SMA at day d (d=0 most recent),
    # we need closes[d:d+period].
    sma50_today  = compute_sma(closes[0:],   50)
    sma200_today = compute_sma(closes[0:],  200)
    if sma50_today is None or sma200_today is None:
        return None, None

    # Walk backwards through history checking for the crossing event
    for d in range(1, lookback_days + 1):
        # SMA values d days ago
        s50_then  = compute_sma(closes[d:],   50)
        s200_then = compute_sma(closes[d:],  200)
        # SMA values d-1 days ago (one day later than `then`)
        s50_now   = compute_sma(closes[d-1:],  50)
        s200_now  = compute_sma(closes[d-1:], 200)
        if None in (s50_then, s200_then, s50_now, s200_now):
            continue
        # Golden cross: was below or equal, now above
        if s50_then <= s200_then and s50_now > s200_now:
            return 'GOLDEN', d
        # Death cross: was above or equal, now below
        if s50_then >= s200_then and s50_now < s200_now:
            return 'DEATH', d

    return None, None

# ── BULK ──────────────────────────────────────────
def get_sp500():
    for attempt in range(5):
        data = fmp("sp500-constituent")
        if isinstance(data, list) and len(data) > 0:
            return data
        print(f"  SP500 attempt {attempt+1} failed, waiting 15s...")
        time.sleep(15)
    return []

def get_bulk_price_changes(symbols):
    result = {}
    for i in range(0, len(symbols), 100):
        chunk = ",".join(symbols[i:i+100])
        data  = fmp("stock-price-change", f"&symbol={chunk}")
        if isinstance(data, list):
            for item in data:
                result[item["symbol"]] = item
    return result

# ── PER STOCK ─────────────────────────────────────
def get_stock_data(symbol):
    profile = fmp("profile",          f"&symbol={symbol}")
    km      = fmp("key-metrics-ttm",  f"&symbol={symbol}")
    ratios  = fmp("ratios-ttm",       f"&symbol={symbol}")
    growth  = fmp("financial-growth", f"&symbol={symbol}&limit=1")

    # Historical prices for SMA + cross detection.
    # Need >=260 days to detect crosses across the last ~60 days.
    closes = get_price_history(symbol, days=300)

    p = profile[0] if isinstance(profile, list) and profile else {}
    k = km[0]      if isinstance(km, list)      and km      else {}
    r = ratios[0]  if isinstance(ratios, list)  and ratios  else {}
    g = growth[0]  if isinstance(growth, list)  and growth  else {}

    # Compute simplified Piotroski from available data
    score = 0
    roa = sf(k.get("returnOnAssetsTTM"))
    roe = sf(k.get("returnOnEquityTTM"))
    roic= sf(k.get("returnOnInvestedCapitalTTM"))
    cr  = sf(k.get("currentRatioTTM"))
    de  = sf(r.get("debtToEquityRatioTTM"))
    npm = sf(r.get("netProfitMarginTTM"))
    gpm = sf(r.get("grossProfitMarginTTM"))
    opm = sf(r.get("operatingProfitMarginTTM"))
    fcfy= sf(k.get("freeCashFlowYieldTTM"))
    at  = sf(r.get("assetTurnoverTTM"))
    rg  = sf(g.get("revenueGrowth"))
    nig = sf(g.get("netIncomeGrowth"))
    fcfg= sf(g.get("freeCashFlowGrowth"))

    if roa  is not None and roa  > 0: score += 1
    if roe  is not None and roe  > 0: score += 1
    if fcfy is not None and fcfy > 0: score += 1
    if npm  is not None and npm  > 0: score += 1
    if gpm  is not None and gpm  > 0.2: score += 1
    if cr   is not None and cr   > 1.0: score += 1
    if de   is not None and de   < 1.0: score += 1
    if rg   is not None and rg   > 0:   score += 1
    if nig  is not None and nig  > 0:   score += 1

    # Institutional signal from financial health
    if score >= 7 and rg is not None and rg > 0.05:
        inst_signal = "buying"
    elif score <= 3 or (de is not None and de > 3):
        inst_signal = "selling"
    else:
        inst_signal = "holding"

    # Cross detection — uses fetched price history
    cross_signal, cross_days_ago = detect_cross(closes, lookback_days=60)

    return {
        "symbol":          symbol,
        "name":            p.get("companyName",""),
        "sector":          p.get("sector",""),
        "industry":        p.get("industry",""),
        "price":           sf(p.get("price")),
        "beta":            sf(p.get("beta")),
        "volume":          int(p.get("volume",0) or 0),
        "marketCap":       sf(p.get("marketCap")),
        # Valuation
        "peRatio":         sanitize_pe(r.get("priceToEarningsRatioTTM")),
        "pbRatio":         sf(r.get("priceToBookRatioTTM")),
        "psRatio":         sf(r.get("priceToSalesRatioTTM")),
        "evEbitda":        sf(k.get("evToEBITDATTM")),
        # Quality
        "roe":             sp2(k.get("returnOnEquityTTM")),
        "roa":             sp2(k.get("returnOnAssetsTTM")),
        "roic":            sp2(k.get("returnOnInvestedCapitalTTM")),
        "grossMargin":     sp2(r.get("grossProfitMarginTTM")),
        "operatingMargin": sp2(r.get("operatingProfitMarginTTM")),
        "netMargin":       sp2(r.get("netProfitMarginTTM")),
        "revenueGrowth":   sp2(g.get("revenueGrowth")),
        "epsGrowth":       sp2(g.get("epsgrowth")),
        "fcfGrowth":       sp2(g.get("freeCashFlowGrowth")),
        # Balance sheet
        "debtToEquity":    sf(r.get("debtToEquityRatioTTM")),
        "currentRatio":    sf(r.get("currentRatioTTM")),
        "dividendYield":   sp2(r.get("dividendYieldTTM")),
        "interestCoverage":sf(r.get("interestCoverageRatioTTM")),
        # Scores
        "piotroski":       score,
        "altmanZ":         None,
        # Institutional (derived)
        "instSignal":      inst_signal,
        "instHolders":     None,
        "instChgPct":      None,
        # Technical — SMAs + cross detection (added 2026-04-25)
        "sma50":           compute_sma(closes, 50),
        "sma200":          compute_sma(closes, 200),
        "crossSignal":     cross_signal,   # 'GOLDEN' | 'DEATH' | None
        "crossDaysAgo":    cross_days_ago, # int days since cross | None
    }

def process(args):
    symbol, price_changes = args
    try:
        d  = get_stock_data(symbol)
        pc = price_changes.get(symbol, {})
        d["chg1d"] = sf(pc.get("1D"))
        d["chg1w"] = sf(pc.get("5D"))
        d["chg1m"] = sf(pc.get("1M"))
        d["chg3m"] = sf(pc.get("3M"))
        d["chg6m"] = sf(pc.get("6M"))
        d["chg1y"] = sf(pc.get("1Y"))
        return d
    except Exception as e:
        print(f"  FAIL {symbol}: {e}")
        return None

# ── HANDLER ───────────────────────────────────────
def lambda_handler(event, context):
    hdrs = {"Content-Type":"application/json","Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"GET,POST,OPTIONS"}
    if isinstance(event,dict) and event.get("requestContext",{}).get("http",{}).get("method")=="OPTIONS":
        return {"statusCode":200,"headers":hdrs,"body":""}

    force = isinstance(event,dict) and (
        event.get("force") or
        (event.get("queryStringParameters") or {}).get("force") == "true"
    )

    if not force:
        try:
            obj    = s3.get_object(Bucket=S3_BUCKET, Key=CACHE_KEY)
            cached = json.loads(obj["Body"].read())
            age    = time.time() - cached.get("generated_at_unix", 0)
            if age < CACHE_TTL:
                return {"statusCode":200,"headers":hdrs,"body":json.dumps({
                    "from_cache":True,"age_hours":round(age/3600,2),
                    "count":cached.get("count",0),"generated_at":cached.get("generated_at"),
                    "stocks": cached.get("stocks",[])
                })}
        except: pass

    print("=== SCREENER START ===")
    t0 = time.time()

    sp500 = get_sp500()
    if not sp500:
        return {"statusCode":500,"headers":hdrs,"body":json.dumps({"error":"S&P 500 fetch failed"})}

    symbols = [s["symbol"] for s in sp500]
    print(f"  {len(symbols)} symbols | {time.time()-t0:.1f}s")

    print("Fetching bulk price changes...")
    price_changes = get_bulk_price_changes(symbols)
    print(f"  {len(price_changes)} price-change records | {time.time()-t0:.1f}s")

    print(f"Processing {len(symbols)} stocks ({WORKERS} workers)...")
    args_list = [(sym, price_changes) for sym in symbols]
    stocks = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process, a): a[0] for a in args_list}
        done = 0
        for fut in as_completed(futs):
            try:
                r = fut.result(timeout=60)
                if r: stocks.append(r)
            except Exception as e:
                print(f"  ERR {futs[fut]}: {e}")
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(symbols)} | {time.time()-t0:.1f}s")

    elapsed = time.time() - t0
    print(f"=== DONE: {len(stocks)} stocks in {elapsed:.1f}s ===")

    payload = {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds":   round(elapsed,1),
        "count":             len(stocks),
        "stocks":            stocks,
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=CACHE_KEY,
        Body=json.dumps(payload, separators=(",",":")),
        ContentType="application/json", CacheControl="max-age=14400"
    )
    return {"statusCode":200,"headers":hdrs,"body":json.dumps({
        "success":True,"count":len(stocks),
        "elapsed_seconds":round(elapsed,1),"generated_at":payload["generated_at"]
    })}
