import json, time, boto3, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

FMP      = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE     = "https://financialmodelingprep.com/stable"
S3_BUCKET= "justhodl-dashboard-live"
CACHE_KEY= "screener/data.json"
CACHE_TTL= 4 * 3600
WORKERS  = 3  # Ultimate tier (3000/min). 19 endpoints/stock × 503 = ~9.5k calls. At 3000/min ≈ 3.2 min.

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


def fmp(path, params="", max_retries=3):
    """
    FMP request with exponential backoff on 429s.
    Premium plan = 750 req/min ≈ 12.5 req/sec. With 2 workers × 5 endpoints
    × 503 stocks we issue ~25 req/sec peak, which spikes over budget.
    Retry with backoff smooths out the burst.
    """
    url = f"{BASE}/{path}?apikey={FMP}{params}"
    last_err = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            r   = urllib.request.urlopen(req, timeout=25)
            return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                # Exponential backoff: 1s, 3s, 9s
                wait = 1 + (attempt * 2) + (attempt ** 2)
                time.sleep(wait)
                continue
            # Non-429: don't retry, fail fast
            print(f"  ERR {path}: {e}")
            return None
        except Exception as e:
            last_err = e
            print(f"  ERR {path}: {e}")
            return None
    # All retries exhausted
    print(f"  ERR {path}: exhausted retries ({last_err})")
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


# ── STAGE 10: Find the latest available 13F quarter — cached per Lambda invoke
# 13Fs filed quarterly with 45-day lag. As of May 2026: latest filed is Q4 2025
# (filed by Feb 14, 2026), Q1 2026 starts filing by May 15. We try most recent
# 4 quarters and stop at first one returning data.
_LATEST_13F_QUARTER = None
def get_latest_13f_quarter():
    """Pick the most recent FULLY-FILED 13F quarter. 13Fs have a 45-day filing
    lag after quarter end. To avoid using a partial-filed quarter (where the
    QoQ change looks artificially negative because most institutions haven't
    filed yet), we require ≥60 days since quarter end."""
    global _LATEST_13F_QUARTER
    if _LATEST_13F_QUARTER is not None:
        return _LATEST_13F_QUARTER
    from datetime import datetime as _dt, date as _date
    now = _dt.now(timezone.utc).date()
    # Quarter-end dates we'll check (newest first)
    qends = []
    y = now.year
    for q in (4, 3, 2, 1):
        # Build q-end date for current year, then iterate backwards
        qend_month = q * 3
        qend_day = 30 if qend_month in (6, 9) else 31
        try:
            qends.append((y, q, _date(y, qend_month, qend_day)))
        except ValueError:
            pass
    # Add prior year quarters
    for q in (4, 3, 2, 1):
        qend_month = q * 3
        qend_day = 30 if qend_month in (6, 9) else 31
        try:
            qends.append((y - 1, q, _date(y - 1, qend_month, qend_day)))
        except ValueError:
            pass
    # Filter to quarter-ends that are ≥60 days in the past (allows full filing)
    # then sort newest-first
    eligible = [(yy, qq, qd) for yy, qq, qd in qends if (now - qd).days >= 60]
    eligible.sort(key=lambda x: -x[2].toordinal())

    # Probe each eligible quarter with AAPL until one returns data
    for yy, qq, qd in eligible[:4]:
        try:
            test = fmp("institutional-ownership/symbol-positions-summary",
                          f"&symbol=AAPL&year={yy}&quarter={qq}", max_retries=1)
            if isinstance(test, list) and test:
                _LATEST_13F_QUARTER = (yy, qq)
                print(f"[inst] using fully-filed 13F quarter Q{qq} {yy} "
                       f"({(now - qd).days}d since quarter end)")
                return _LATEST_13F_QUARTER
        except Exception:
            continue
    _LATEST_13F_QUARTER = (now.year - 1, 4)
    return _LATEST_13F_QUARTER

# ── PER STOCK ─────────────────────────────────────
def get_stock_data(symbol):
    profile = fmp("profile",          f"&symbol={symbol}")
    km      = fmp("key-metrics-ttm",  f"&symbol={symbol}")
    ratios  = fmp("ratios-ttm",       f"&symbol={symbol}")
    growth  = fmp("financial-growth", f"&symbol={symbol}&limit=1")
    scores  = fmp("financial-scores", f"&symbol={symbol}")  # confirmed Apr 26 — has altmanZScore + piotroskiScore
    # ── PHASE 1 ADDS (2026-05-11) ─────────────────────────────
    # income-statement + cash-flow-statement give us ABSOLUTE
    # dollar values for Money Machines / Cash Generators / Rev
    # Kings tabs (we previously only had margins, not totals).
    income  = fmp("income-statement",    f"&symbol={symbol}&limit=3&period=annual")
    cashflow= fmp("cash-flow-statement", f"&symbol={symbol}&limit=3&period=annual")
    # ── STAGE 2 RE-WIRED 2026-05-11 ─────────────────────────────
    # STAGE 10 — Ultimate plan ACTIVATED 2026-05-11
    # institutional-ownership now works with year+quarter params (Ultimate).
    year_q, q_q = get_latest_13f_quarter()
    inst = fmp("institutional-ownership/symbol-positions-summary",
                  f"&symbol={symbol}&year={year_q}&quarter={q_q}")
    # insider-trading/search: confirmed working on Premium (probe step 411)
    insider = fmp("insider-trading/search", f"&symbol={symbol}&limit=50")
    # earnings: provides epsActual + epsEstimated; compute surprise % locally
    surprises = fmp("earnings", f"&symbol={symbol}&limit=8")

    # ── STAGE 9 ADDS (2026-05-11) — FMP Ultimate Power-Up ───────────
    # All endpoints confirmed working on Premium tier (probe step 423/424).
    # 6 new endpoints/stock that add political alpha, analyst intelligence,
    # DCF valuation, ESG ratings, and news momentum.
    senate_tr   = fmp("senate-trades",            f"&symbol={symbol}")
    house_tr    = fmp("house-trades",             f"&symbol={symbol}")
    pt_consensus= fmp("price-target-consensus",   f"&symbol={symbol}")
    pt_summary  = fmp("price-target-summary",     f"&symbol={symbol}")
    grades_cons = fmp("grades-consensus",         f"&symbol={symbol}")
    grades_recent= fmp("grades",                  f"&symbol={symbol}")
    dcf_data    = fmp("discounted-cash-flow",     f"&symbol={symbol}")
    esg_data    = fmp("esg-ratings",              f"&symbol={symbol}")

    # ── STAGE 10 ADDS — FMP Ultimate Activation ─────────────────
    # Forward analyst estimates (annual + quarterly) for forward growth metrics
    forward_est = fmp("analyst-estimates",       f"&symbol={symbol}&period=annual&limit=3")
    # Share float — liquidity classification, free float pct
    share_float = fmp("shares-float",             f"&symbol={symbol}")
    # Key executives — for CEO pay + tenure tracking
    exec_list   = fmp("key-executives",            f"&symbol={symbol}")

    # ── STAGE 11 ADDS — News momentum (sudden catalysts) ──
    # 10 latest articles per symbol (was 20 — cut to halve payload size since
    # each article includes full body text, was 11.6min runtime).
    # We compute count-30d + heuristic sentiment from headline keywords.
    news_data   = fmp("news/stock",                f"&symbols={symbol}&limit=10")

    # Historical prices for SMA + cross detection.
    # Need >=260 days to detect crosses across the last ~60 days.
    closes = get_price_history(symbol, days=300)

    p = profile[0] if isinstance(profile, list) and profile else {}
    k = km[0]      if isinstance(km, list)      and km      else {}
    r = ratios[0]  if isinstance(ratios, list)  and ratios  else {}
    g = growth[0]  if isinstance(growth, list)  and growth  else {}
    sc = scores[0] if isinstance(scores, list)  and scores  else {}
    inc = income[0]  if isinstance(income, list)   and income   else {}
    inc_prev = income[1] if isinstance(income, list) and len(income) >= 2 else {}
    inc_prev2= income[2] if isinstance(income, list) and len(income) >= 3 else {}
    cf  = cashflow[0]if isinstance(cashflow, list) and cashflow else {}

    # ── PHASE 1 NEW FIELDS — absolute dollar values + multi-year trend ──
    # FMP-computed Altman Z-Score from /stable/financial-scores
    altman_z = sf(sc.get("altmanZScore"))
    # Revenue & profitability (latest annual)
    revenue_ttm        = sf(inc.get("revenue"))
    net_income_ttm     = sf(inc.get("netIncome"))
    operating_income   = sf(inc.get("operatingIncome"))
    ebitda             = sf(inc.get("ebitda"))
    gross_profit       = sf(inc.get("grossProfit"))
    # Cash flow
    free_cash_flow     = sf(cf.get("freeCashFlow"))
    operating_cf       = sf(cf.get("operatingCashFlow"))
    capex              = sf(cf.get("capitalExpenditure"))
    # Buyback detection: stockRepurchased is the cash outflow on buybacks
    # (negative number when actively buying back).
    stock_repurchased  = sf(cf.get("commonStockRepurchased") or cf.get("stockRepurchased"))
    # FCF Yield calculated (FCF / market cap)
    mcap_val = sf(p.get("marketCap"))
    fcf_yield_calc = None
    if free_cash_flow is not None and mcap_val and mcap_val > 0:
        fcf_yield_calc = round((free_cash_flow / mcap_val) * 100, 2)
    # 3-year revenue trend (latest vs 2 years ago) — flags accelerating growth
    rev_2y_ago = sf(inc_prev2.get("revenue")) if inc_prev2 else None
    rev_3y_cagr = None
    if revenue_ttm and rev_2y_ago and rev_2y_ago > 0:
        rev_3y_cagr = round(((revenue_ttm / rev_2y_ago) ** (1.0 / 2.0) - 1.0) * 100, 2)
    # Sustainable profitability flag — positive net income in all 3 yrs
    ni_curr = sf(inc.get("netIncome"))
    ni_prev = sf(inc_prev.get("netIncome")) if inc_prev else None
    ni_prev2= sf(inc_prev2.get("netIncome")) if inc_prev2 else None
    sustainable_3y = bool(ni_curr and ni_curr > 0 and
                            ni_prev and ni_prev > 0 and
                            ni_prev2 and ni_prev2 > 0)
    # ROE consistency — check sustainability of profitability
    roe_val = sf(k.get("returnOnEquityTTM"))
    _npm_check = sf(r.get("netProfitMarginTTM"))  # used here + redefined below as `npm` for piotroski
    sustainable_quality = bool(sustainable_3y and roe_val and roe_val > 0.15 and
                                  _npm_check and _npm_check > 0.10)
    # Buyback signal — material repurchases relative to mcap
    buyback_yield = None
    if stock_repurchased and mcap_val and mcap_val > 0:
        # stockRepurchased is typically negative — flip sign so positive = buying back
        buyback_yield = round((abs(stock_repurchased) / mcap_val) * 100, 2)

    # ── STAGE 2 — institutional + insider + earnings-surprise processing ──
    # Institutional ownership (13F)
    inst_curr = inst[0] if isinstance(inst, list) and len(inst) >= 1 else {}
    inst_prev = inst[1] if isinstance(inst, list) and len(inst) >= 2 else {}
    inst_ownership_pct = sf(inst_curr.get("ownershipPercent"))
    inst_holders_n = sf(inst_curr.get("investorsHolding"))
    inst_total_shares = sf(inst_curr.get("totalInvested"))
    inst_qoq_chg_pct = None
    inst_holders_chg = None
    if inst_curr and inst_prev:
        sh_curr = sf(inst_curr.get("totalInvested"))
        sh_prev = sf(inst_prev.get("totalInvested"))
        if sh_curr and sh_prev and sh_prev > 0:
            inst_qoq_chg_pct = round((sh_curr / sh_prev - 1.0) * 100, 2)
        h_curr = sf(inst_curr.get("investorsHolding"))
        h_prev = sf(inst_prev.get("investorsHolding"))
        if h_curr is not None and h_prev is not None:
            inst_holders_chg = int(h_curr - h_prev)

    # Insider trading — last 90 days, separate buys vs sells
    # Endpoint: /stable/insider-trading/search returns transactions with these key fields:
    #   transactionType — "P-Purchase", "S-Sale", "A-Award", etc.
    #   acquisitionOrDisposition — "A" (acquired) or "D" (disposed)
    #   securitiesTransacted — share count (may be present)
    #   securitiesOwned — total holdings after transaction
    #   price — per-share transaction price
    #   reportingName — insider's name
    #   transactionDate / filingDate
    insider_buys_90d_usd = 0.0
    insider_sells_90d_usd = 0.0
    insider_buyers_90d = set()
    insider_sellers_90d = set()
    insider_net_signal = "neutral"
    insider_recent_buys_count = 0
    if isinstance(insider, list):
        from datetime import datetime as _dt
        cutoff_ts = (datetime.now(timezone.utc).timestamp() - 90 * 86400)
        for t in insider:
            try:
                tx_date_str = t.get("transactionDate") or t.get("filingDate") or ""
                if not tx_date_str:
                    continue
                tx_ts = _dt.strptime(tx_date_str[:10], "%Y-%m-%d").timestamp()
                if tx_ts < cutoff_ts:
                    continue
                # Try multiple share-count field names
                shares = float(t.get("securitiesTransacted")
                                 or t.get("transactionShares")
                                 or 0)
                price = float(t.get("price") or t.get("transactionPrice") or 0)
                amount = abs(shares * price)
                txt = (t.get("transactionType") or "").upper()
                acq = (t.get("acquisitionOrDisposition")
                          or t.get("acquistionOrDisposition")
                          or "").upper()
                name = t.get("reportingName") or t.get("name") or "unknown"
                # P-Purchase or A-Award with acquisition counts as buying.
                # Note: A-Award includes restricted-stock grants which are NOT
                # discretionary purchases — but they DO show insider conviction
                # to accept compensation as stock vs cash, so we include them.
                is_buy = (("P-PURCHASE" in txt) or
                            ("P/PURCHASE" in txt) or
                            ("A-AWARD" in txt and acq == "A") or
                            (acq == "A" and "P" in txt))
                is_sell = (("S-SALE" in txt) or
                              ("S/SALE" in txt) or
                              (acq == "D" and "S" in txt))
                if is_buy and not is_sell:
                    if amount > 0:
                        insider_buys_90d_usd += amount
                    insider_buyers_90d.add(name)
                    insider_recent_buys_count += 1
                elif is_sell:
                    if amount > 0:
                        insider_sells_90d_usd += amount
                    insider_sellers_90d.add(name)
            except Exception:
                continue
    # Cluster buying = 3+ distinct insiders making purchases in 90d
    insider_cluster_buying = len(insider_buyers_90d) >= 3
    insider_net_usd = round(insider_buys_90d_usd - insider_sells_90d_usd, 0)
    if insider_buys_90d_usd > 0 and insider_buys_90d_usd > insider_sells_90d_usd * 2:
        insider_net_signal = "buying"
    elif insider_sells_90d_usd > insider_buys_90d_usd * 5 and insider_sells_90d_usd > 1e6:
        insider_net_signal = "selling"

    # Earnings surprise streak — from /stable/earnings (epsActual + epsEstimated)
    beat_streak = 0
    last_surprise_pct = None
    avg_surprise_pct = None
    if isinstance(surprises, list) and surprises:
        # The endpoint returns FUTURE + past dates mixed. Filter to past (epsActual not null)
        # and sort by date descending so most-recent reported quarter is first.
        past = [s for s in surprises if s.get("epsActual") is not None]
        try:
            past.sort(key=lambda x: x.get("date", ""), reverse=True)
        except Exception:
            pass
        surprise_pcts = []
        streak_active = True
        for s in past:
            act = sf(s.get("epsActual"))
            est = sf(s.get("epsEstimated"))
            if act is None or est is None:
                streak_active = False
                continue
            beat = act > est
            if streak_active and beat:
                beat_streak += 1
            else:
                streak_active = False
            if est != 0:
                surprise_pcts.append((act - est) / abs(est) * 100)
        if surprise_pcts:
            last_surprise_pct = round(surprise_pcts[0], 2)
            avg_surprise_pct = round(sum(surprise_pcts) / len(surprise_pcts), 2)

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

    # ════════════════════════════════════════════════════════════════════
    # STAGE 9 PARSERS — Political + Analyst + DCF + ESG
    # ════════════════════════════════════════════════════════════════════
    from datetime import datetime as _dt2

    # ── Helper: parse FMP's "$1,001 - $15,000" range into midpoint USD ──
    def parse_amount_range(raw):
        if not raw or not isinstance(raw, str):
            return 0.0
        # Strip $, comma, spaces; split on -
        cleaned = raw.replace("$", "").replace(",", "").strip()
        parts = cleaned.split("-")
        try:
            if len(parts) == 2:
                lo = float(parts[0].strip())
                hi = float(parts[1].strip())
                return (lo + hi) / 2.0
            return float(parts[0].strip())
        except Exception:
            return 0.0

    # ── Aggregate Senate + House trading over last 90 days ──
    political_buys_90d_usd = 0.0
    political_sells_90d_usd = 0.0
    political_buyers_90d = set()
    political_sellers_90d = set()
    political_recent_count = 0
    senate_buys_90d = 0
    house_buys_90d = 0
    cutoff_ts_90 = (datetime.now(timezone.utc).timestamp() - 90 * 86400)
    cutoff_ts_30 = (datetime.now(timezone.utc).timestamp() - 30 * 86400)
    political_buys_30d_n = 0

    def _parse_political(trades, chamber):
        """Accumulate to outer scope variables."""
        nonlocal political_buys_90d_usd, political_sells_90d_usd
        nonlocal political_recent_count, political_buys_30d_n
        nonlocal senate_buys_90d, house_buys_90d
        if not isinstance(trades, list):
            return
        for t in trades:
            try:
                date_str = t.get("transactionDate") or t.get("disclosureDate") or ""
                if not date_str:
                    continue
                tx_ts = _dt2.strptime(date_str[:10], "%Y-%m-%d").timestamp()
                if tx_ts < cutoff_ts_90:
                    continue
                amount_usd = parse_amount_range(t.get("amount", ""))
                txn_type = (t.get("type") or "").lower()
                full_name = (t.get("firstName", "") + " " + t.get("lastName", "")).strip()
                if "purchase" in txn_type or "buy" in txn_type:
                    political_buys_90d_usd += amount_usd
                    political_buyers_90d.add(full_name)
                    political_recent_count += 1
                    if chamber == "senate":
                        senate_buys_90d += 1
                    else:
                        house_buys_90d += 1
                    if tx_ts >= cutoff_ts_30:
                        political_buys_30d_n += 1
                elif "sale" in txn_type or "sell" in txn_type:
                    political_sells_90d_usd += amount_usd
                    political_sellers_90d.add(full_name)
            except Exception:
                continue

    _parse_political(senate_tr, "senate")
    _parse_political(house_tr, "house")
    political_net_usd = round(political_buys_90d_usd - political_sells_90d_usd, 0)
    # Signal: 3+ distinct buyers in 90d = cluster political buying
    political_cluster_buying = len(political_buyers_90d) >= 3
    political_signal = "buying" if (political_buys_90d_usd > political_sells_90d_usd * 1.5
                                       and political_buys_90d_usd > 5000) \
                          else ("selling" if political_sells_90d_usd > political_buys_90d_usd * 3
                                  and political_sells_90d_usd > 25000 else "neutral")

    # ── Analyst price targets ──
    target_consensus = None
    target_high = None
    target_low = None
    target_median = None
    target_upside_pct = None
    pt_count_30d = None
    pt_count_90d = None
    pt_avg_30d = None
    pt_avg_90d = None
    if isinstance(pt_consensus, list) and pt_consensus:
        ptc = pt_consensus[0]
        target_consensus = sf(ptc.get("targetConsensus"))
        target_high = sf(ptc.get("targetHigh"))
        target_low = sf(ptc.get("targetLow"))
        target_median = sf(ptc.get("targetMedian"))
        price_now = sf(p.get("price"))
        if target_consensus is not None and price_now and price_now > 0:
            target_upside_pct = round((target_consensus - price_now) / price_now * 100, 1)
    if isinstance(pt_summary, list) and pt_summary:
        pts = pt_summary[0]
        pt_count_30d = pts.get("lastMonthCount")
        pt_avg_30d = sf(pts.get("lastMonthAvgPriceTarget"))
        pt_count_90d = pts.get("lastQuarterCount")
        pt_avg_90d = sf(pts.get("lastQuarterAvgPriceTarget"))

    # ── Analyst grades / upgrades ──
    grades_strong_buy = 0
    grades_buy = 0
    grades_hold = 0
    grades_sell = 0
    grades_strong_sell = 0
    grades_consensus_label = None
    grades_consensus_score = None  # -100 to +100
    if isinstance(grades_cons, list) and grades_cons:
        gc = grades_cons[0]
        grades_strong_buy = int(gc.get("strongBuy") or 0)
        grades_buy = int(gc.get("buy") or 0)
        grades_hold = int(gc.get("hold") or 0)
        grades_sell = int(gc.get("sell") or 0)
        grades_strong_sell = int(gc.get("strongSell") or 0)
        grades_consensus_label = gc.get("consensus")
        total = grades_strong_buy + grades_buy + grades_hold + grades_sell + grades_strong_sell
        if total > 0:
            weighted = (grades_strong_buy * 2 + grades_buy * 1
                          + grades_hold * 0
                          - grades_sell * 1 - grades_strong_sell * 2)
            grades_consensus_score = round((weighted / (total * 2)) * 100, 1)

    # Recent upgrade/downgrade tally — last 30/90 days
    upgrades_30d = 0
    downgrades_30d = 0
    upgrades_90d = 0
    downgrades_90d = 0
    if isinstance(grades_recent, list):
        for gr in grades_recent[:200]:  # cap how far we look back
            try:
                date_str = gr.get("date", "")
                if not date_str:
                    continue
                gr_ts = _dt2.strptime(date_str[:10], "%Y-%m-%d").timestamp()
                action = (gr.get("action") or "").lower()
                if gr_ts >= cutoff_ts_90:
                    if "upgrade" in action:
                        upgrades_90d += 1
                        if gr_ts >= cutoff_ts_30:
                            upgrades_30d += 1
                    elif "downgrade" in action:
                        downgrades_90d += 1
                        if gr_ts >= cutoff_ts_30:
                            downgrades_30d += 1
            except Exception:
                continue
    upgrade_net_30d = upgrades_30d - downgrades_30d
    upgrade_net_90d = upgrades_90d - downgrades_90d

    # ── DCF Valuation ──
    dcf_fair_value = None
    dcf_upside_pct = None
    if isinstance(dcf_data, list) and dcf_data:
        dcf_v = sf(dcf_data[0].get("dcf"))
        price_now = sf(p.get("price"))
        if dcf_v is not None and price_now and price_now > 0:
            dcf_fair_value = round(dcf_v, 2)
            dcf_upside_pct = round((dcf_v - price_now) / price_now * 100, 1)

    # ── ESG Ratings (most recent fiscal year) ──
    esg_rating = None
    esg_score_numeric = None
    esg_industry_rank = None
    esg_industry = None
    if isinstance(esg_data, list) and esg_data:
        # Sort by fiscalYear desc to get most recent
        latest_esg = sorted(esg_data, key=lambda x: x.get("fiscalYear", 0), reverse=True)[0]
        esg_rating = latest_esg.get("ESGRiskRating")
        esg_industry_rank = latest_esg.get("industryRank")
        esg_industry = latest_esg.get("industry")
        # Convert letter rating to 0-100 numeric for sorting
        # AAA=100, AA=90, A=80, BBB=70, BB=60, B=50, CCC=40, CC=30, C=20, D=10
        esg_map = {"AAA": 100, "AA": 90, "A": 80, "BBB": 70, "BB": 60,
                     "B": 50, "CCC": 40, "CC": 30, "C": 20, "D": 10}
        esg_score_numeric = esg_map.get(esg_rating)

    # ════════════════════════════════════════════════════════════════════
    # STAGE 10 PARSERS — Institutional + Forward + Float + Executives
    # ════════════════════════════════════════════════════════════════════

    # ── INSTITUTIONAL OWNERSHIP (13F summary) ──
    inst_investors_holding = None
    inst_last_investors_holding = None
    inst_investors_change = None
    inst_investors_chg_pct = None
    inst_13f_shares = None
    inst_last_13f_shares = None
    inst_shares_change_pct = None
    inst_signal_real = None
    inst_quarter_label = None
    if isinstance(inst, list) and inst:
        inst_rec = inst[0]
        inst_investors_holding = inst_rec.get("investorsHolding")
        inst_last_investors_holding = inst_rec.get("lastInvestorsHolding")
        inst_investors_change = inst_rec.get("investorsHoldingChange")
        if (inst_investors_holding and inst_last_investors_holding
                and inst_last_investors_holding > 0):
            inst_investors_chg_pct = round(
                ((inst_investors_holding - inst_last_investors_holding) /
                 inst_last_investors_holding) * 100, 2)
        inst_13f_shares = inst_rec.get("numberOf13Fshares")
        inst_last_13f_shares = inst_rec.get("lastNumberOf13Fshares")
        if (inst_13f_shares and inst_last_13f_shares
                and inst_last_13f_shares > 0):
            inst_shares_change_pct = round(
                ((inst_13f_shares - inst_last_13f_shares) /
                 inst_last_13f_shares) * 100, 2)
        # Real institutional signal — based on QoQ changes
        chg_pct = inst_shares_change_pct or 0
        inv_chg_pct = inst_investors_chg_pct or 0
        if chg_pct > 2 or inv_chg_pct > 2:
            inst_signal_real = "buying"
        elif chg_pct < -2 or inv_chg_pct < -2:
            inst_signal_real = "selling"
        else:
            inst_signal_real = "holding"
        inst_quarter_label = f"Q{q_q} {year_q}"

    # ── FORWARD ANALYST ESTIMATES (next-year revenue + EBITDA growth) ──
    forward_revenue = None
    forward_revenue_growth = None
    forward_ebitda = None
    forward_ebitda_growth = None
    forward_pe = None
    forward_year = None
    if isinstance(forward_est, list) and forward_est:
        # Estimates often span multiple years; find the next forward year vs today
        from datetime import datetime as _dt3
        cur_year = _dt3.now(timezone.utc).year
        # Sort by date ascending; first record with date.year > cur_year is "next year"
        sorted_est = sorted(forward_est, key=lambda x: x.get("date", ""))
        forward_rec = None
        for e in sorted_est:
            ed = e.get("date", "")[:4]
            if ed.isdigit() and int(ed) > cur_year:
                forward_rec = e
                break
        if not forward_rec and sorted_est:
            forward_rec = sorted_est[0]  # fallback to soonest
        if forward_rec:
            forward_revenue = sf(forward_rec.get("revenueAvg"))
            forward_ebitda = sf(forward_rec.get("ebitdaAvg"))
            forward_year = forward_rec.get("date", "")[:4]
            # Forward growth vs current trailing revenue
            current_rev = sf(p.get("revenue")) or sf(inc.get("revenue"))
            if forward_revenue and current_rev and current_rev > 0:
                forward_revenue_growth = round(
                    ((forward_revenue - current_rev) / current_rev) * 100, 2)
            # Forward P/E estimate using consensus EPS
            forward_eps = sf(forward_rec.get("epsAvg"))
            price_now = sf(p.get("price"))
            if forward_eps and forward_eps > 0 and price_now:
                forward_pe = round(price_now / forward_eps, 2)

    # ── SHARE FLOAT ──
    free_float_pct = None
    float_shares = None
    outstanding_shares = None
    if isinstance(share_float, list) and share_float:
        sfr = share_float[0]
        free_float_pct = sf(sfr.get("freeFloat"))
        float_shares = sf(sfr.get("floatShares"))
        outstanding_shares = sf(sfr.get("outstandingShares"))

    # ── KEY EXECUTIVES (CEO + tenure) ──
    ceo_name = None
    ceo_pay = None
    n_executives = None
    if isinstance(exec_list, list) and exec_list:
        n_executives = len(exec_list)
        # Find the CEO record
        for e in exec_list:
            title = (e.get("title") or "").lower()
            if "chief executive officer" in title or "ceo" in title.split():
                ceo_name = e.get("name")
                ceo_pay = sf(e.get("pay"))
                break

    # ────────── STAGE 11: NEWS MOMENTUM ──────────
    # Cheap heuristic sentiment scoring — no LLM call. Keywords were tuned
    # from finance-specific language. Score = (pos_matches - neg_matches) /
    # total_articles, scaled to -100 to +100.
    POS_WORDS = {
        "beat","beats","exceed","exceeds","exceeded","record","records",
        "growth","strong","surge","surges","surged","rally","rallies","rallied",
        "upgrade","upgrades","upgraded","buy","raises","outperform",
        "breakthrough","milestone","partnership","approval","approved",
        "acquires","acquisition","raises","boost","boosts","soars","soared",
        "bullish","top","tops","highest","gains","gained","rises","rose",
        "profit","profitable","launches","wins","awarded","contract",
        "expansion","expand","expanding","innovate","innovative","success",
    }
    NEG_WORDS = {
        "miss","misses","missed","decline","declined","declines","weak",
        "weakens","fall","falls","fell","crash","crashes","downgrade",
        "downgrades","downgraded","sell","sells","investigation","lawsuit",
        "sued","layoff","layoffs","recall","recalls","warn","warns","warned",
        "warning","disappoint","disappoints","disappointed","disappointing",
        "loss","losses","bearish","crash","tumble","tumbles","tumbled",
        "plunge","plunges","plunged","drops","dropped","slumps","slumped",
        "halt","halts","halted","fraud","scandal","fine","fined","penalty",
        "concern","concerns","concerning","cuts","cut","reduces","reduced",
        "risks","risky","threat","threatens","decline","declining",
    }

    news_count_30d = 0
    news_count_7d = 0
    news_sentiment_30d = None
    latest_headline = None
    latest_news_date = None
    cutoff_news_30 = (datetime.now(timezone.utc).timestamp() - 30 * 86400)
    cutoff_news_7 = (datetime.now(timezone.utc).timestamp() - 7 * 86400)
    if isinstance(news_data, list) and news_data:
        sent_pos = 0
        sent_neg = 0
        for a in news_data:
            try:
                pub_date = a.get("publishedDate", "")[:19]
                if not pub_date:
                    continue
                # publishedDate format: "2026-05-11 10:31:28"
                pub_ts = datetime.strptime(pub_date, "%Y-%m-%d %H:%M:%S").timestamp()
                if pub_ts < cutoff_news_30:
                    continue
                news_count_30d += 1
                if pub_ts >= cutoff_news_7:
                    news_count_7d += 1
                # Keyword sentiment scoring (case-insensitive)
                title = (a.get("title") or "").lower()
                words = set(title.replace(",", " ").replace(":", " ").split())
                sent_pos += len(words & POS_WORDS)
                sent_neg += len(words & NEG_WORDS)
                # Track latest headline (news is returned in desc order)
                if latest_headline is None:
                    latest_headline = a.get("title")
                    latest_news_date = a.get("publishedDate")
            except Exception:
                continue
        # Sentiment: net positive matches per article × 50 (so ±2 per article = ±100)
        if news_count_30d > 0:
            net = sent_pos - sent_neg
            news_sentiment_30d = round(max(-100, min(100, net / news_count_30d * 50)), 1)

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
        # Quality — FMP returns decimals (0.25 for 25%). Multiply ×100 here so
        # the screener page displays the values as true percentages.
        "roe":             sp(k.get("returnOnEquityTTM")),
        "roa":             sp(k.get("returnOnAssetsTTM")),
        "roic":            sp(k.get("returnOnInvestedCapitalTTM")),
        "grossMargin":     sp(r.get("grossProfitMarginTTM")),
        "operatingMargin": sp(r.get("operatingProfitMarginTTM")),
        "netMargin":       sp(r.get("netProfitMarginTTM")),
        "revenueGrowth":   sp(g.get("revenueGrowth")),
        "epsGrowth":       sp(g.get("epsgrowth")),
        "fcfGrowth":       sp(g.get("freeCashFlowGrowth")),
        # Balance sheet
        "debtToEquity":    sf(r.get("debtToEquityRatioTTM")),
        "currentRatio":    sf(r.get("currentRatioTTM")),
        "dividendYield":   sp(r.get("dividendYieldTTM")),
        "interestCoverage":sf(r.get("interestCoverageRatioTTM")),
        # Scores
        "piotroski":       score,
        "altmanZ":         altman_z,
        # Institutional (Stage 10: real 13F data)
        "instSignal":               inst_signal_real or inst_signal,
        "instInvestorsHolding":     inst_investors_holding,
        "instLastInvestorsHolding": inst_last_investors_holding,
        "instInvestorsChange":      inst_investors_change,
        "instInvestorsChgPct":      inst_investors_chg_pct,
        "inst13fShares":            inst_13f_shares,
        "instSharesChangePct":      inst_shares_change_pct,
        # QoQ change in institutional shares is the KEY metric for
        # the 🐋 Hedge Fund Bought + 🏦 Institution-Accumulated tabs
        "instQoQChgPct":            inst_shares_change_pct,
        "instHolders":              inst_investors_holding,  # alias for page
        "instChgPct":               inst_investors_chg_pct,  # alias for page
        "instQuarter":              inst_quarter_label,
        # Technical — SMAs + cross detection (added 2026-04-25)
        "sma50":           compute_sma(closes, 50),
        "sma200":          compute_sma(closes, 200),
        "crossSignal":     cross_signal,   # 'GOLDEN' | 'DEATH' | None
        "crossDaysAgo":    cross_days_ago, # int days since cross | None
        # ── PHASE 1 NEW FIELDS (2026-05-11) ──
        # Absolute-dollar fundamentals (latest annual filing)
        "revenue":           revenue_ttm,           # absolute $ revenue (latest annual)
        "netIncome":         net_income_ttm,        # absolute $ net income
        "operatingIncome":   operating_income,      # absolute $ operating income
        "ebitda":            ebitda,                # absolute $ EBITDA
        "grossProfit":       gross_profit,
        # Cash flow
        "freeCashFlow":      free_cash_flow,        # absolute $ FCF
        "operatingCashFlow": operating_cf,
        "capex":             capex,
        "fcfYieldCalc":      fcf_yield_calc,        # FCF / marketCap % (calc'd here)
        # Buyback signals
        "stockRepurchased":  stock_repurchased,
        "buybackYield":      buyback_yield,         # |repurchases|/mcap %, positive
        # Multi-year trends
        "rev3yCAGR":         rev_3y_cagr,           # 3y revenue CAGR %
        "sustainable3y":     sustainable_3y,        # 3 consecutive years of positive NI
        "sustainableQuality":sustainable_quality,   # 3y profit + ROE>15% + margin>10%
        # ── STAGE 2 FIELDS (2026-05-11) ──
        # Institutional 13F ownership
        "instOwnershipPct":  inst_ownership_pct,    # % of float held by 13F institutions
        "instHoldersN":      inst_holders_n,        # # of 13F institutions holding
        "instQoQChgPct":     inst_qoq_chg_pct,      # QoQ % change in 13F shares held
        "instHoldersChg":    inst_holders_chg,      # change in # of holders QoQ
        # Insider activity (Form 4) last 90 days
        "insiderBuys90dUsd": round(insider_buys_90d_usd, 0),
        "insiderSells90dUsd":round(insider_sells_90d_usd, 0),
        "insiderNet90dUsd":  insider_net_usd,       # buys - sells, $ value
        "insiderBuyersN90d": len(insider_buyers_90d),
        "insiderClusterBuy": insider_cluster_buying,# 3+ distinct insiders buying
        "insiderSignal":     insider_net_signal,    # 'buying' | 'selling' | 'neutral'
        # Earnings surprise streak
        "beatStreak":        beat_streak,           # # of consecutive recent EPS beats
        "lastSurprisePct":   last_surprise_pct,     # most recent EPS surprise %
        "avgSurprisePct":    avg_surprise_pct,      # avg surprise % over fetched window
        # ── STAGE 9 ─────────────────────────────────────────────
        # Political/Senate/House trading (combined)
        "politicalBuys90dUsd":   round(political_buys_90d_usd, 0),
        "politicalSells90dUsd":  round(political_sells_90d_usd, 0),
        "politicalNet90dUsd":    political_net_usd,
        "politicalBuyersN90d":   len(political_buyers_90d),
        "politicalSellersN90d":  len(political_sellers_90d),
        "politicalClusterBuy":   political_cluster_buying,
        "politicalSignal":       political_signal,     # 'buying'|'selling'|'neutral'
        "senateBuysN90d":        senate_buys_90d,
        "houseBuysN90d":         house_buys_90d,
        "politicalBuys30dN":     political_buys_30d_n,  # most-recent surge indicator
        # Analyst price targets
        "priceTargetMean":       target_consensus,
        "priceTargetHigh":       target_high,
        "priceTargetLow":        target_low,
        "priceTargetMedian":     target_median,
        "priceTargetUpsidePct":  target_upside_pct,    # KEY field for "target upside" tab
        "priceTargetCount30d":   pt_count_30d,
        "priceTargetCount90d":   pt_count_90d,
        "priceTargetAvg30d":     pt_avg_30d,
        # Analyst grades & upgrades
        "gradesStrongBuy":       grades_strong_buy,
        "gradesBuy":             grades_buy,
        "gradesHold":            grades_hold,
        "gradesSell":            grades_sell,
        "gradesStrongSell":      grades_strong_sell,
        "gradesConsensus":       grades_consensus_label,
        "gradesScore":           grades_consensus_score,   # -100 to +100
        "upgrades30d":           upgrades_30d,
        "downgrades30d":         downgrades_30d,
        "upgrades90d":           upgrades_90d,
        "downgrades90d":         downgrades_90d,
        "upgradeNet30d":         upgrade_net_30d,        # net upgrades - downgrades
        "upgradeNet90d":         upgrade_net_90d,
        # DCF valuation
        "dcfFairValue":          dcf_fair_value,
        "dcfUpsidePct":          dcf_upside_pct,         # KEY field for "DCF deep value" tab
        # ESG ratings
        "esgRating":             esg_rating,             # letter grade (AAA, AA, A, BBB, ...)
        "esgScoreNumeric":       esg_score_numeric,      # 10-100 for sorting/filtering
        "esgIndustryRank":       esg_industry_rank,      # "4 out of 6" style string
        # ── STAGE 10 ─────────────────────────────────────────────
        # Forward analyst estimates (1-3 years out)
        "forwardRevenue":        forward_revenue,
        "forwardEbitda":         forward_ebitda,
        "forwardRevenueGrowth":  forward_revenue_growth,   # %
        "forwardPE":             forward_pe,
        "forwardYear":           forward_year,
        # Share float / liquidity
        "freeFloatPct":          free_float_pct,
        "floatShares":           float_shares,
        "outstandingShares":     outstanding_shares,
        # Key executives
        "ceoName":               ceo_name,
        "ceoPay":                ceo_pay,                  # last reported CEO total comp
        "nExecutives":           n_executives,
        # ── STAGE 11: News momentum ──
        "newsCount30d":          news_count_30d,
        "newsCount7d":           news_count_7d,            # surge indicator
        "newsSentiment30d":      news_sentiment_30d,        # -100 to +100, keyword-based
        "latestHeadline":        latest_headline,
        "latestNewsDate":        latest_news_date,
    }

# ════════════════════════════════════════════════════════════════════════
# STEAL SCORE — 9-factor composite percentile-rank ranking
# Goal: a single 0-100 score that captures "all the right factors a value
# investor would weigh." Each factor is converted to a percentile rank
# vs. the universe (higher = better), with sign-inverted metrics where
# 'lower is better' (P/E, EV/EBITDA, D/E). Weighted sum → composite.
#
# Buckets:
#   90+      🔥 STEAL    pulsing gold border on the page
#   80-89    💎 PREMIUM
#   70-79    ✓ Quality
#   <70      neutral
# ════════════════════════════════════════════════════════════════════════
def _vmap_quality(stocks):
    """Earnings quality factor: FCF / |NI| ratio. ≥1.0 = real cash backing earnings."""
    out = {}
    for i, s in enumerate(stocks):
        fcf = s.get("freeCashFlow")
        ni  = s.get("netIncome")
        if fcf is not None and ni is not None and abs(ni) > 0:
            out[i] = round(fcf / abs(ni), 3)
        else:
            out[i] = None
    return out


def _vmap(stocks, field):
    return {i: s.get(field) for i, s in enumerate(stocks)}


def _percentile_ranks(values_by_idx, invert=False):
    """Returns dict {idx: percentile_rank 0-100} where higher = better.
    invert=True for 'lower is better' fields (P/E, D/E, etc)."""
    valid_items = [(idx, v) for idx, v in values_by_idx.items()
                     if v is not None and v == v]
    if not valid_items:
        return {}
    # Sort: ascending for 'higher=better' (best at end); descending for invert
    valid_items.sort(key=lambda kv: kv[1], reverse=invert)
    n = len(valid_items)
    ranks = {}
    for rank_pos, (idx, _v) in enumerate(valid_items):
        ranks[idx] = round(rank_pos / max(1, n - 1) * 100, 1)
    return ranks


def _vmap_capped(stocks, field, lo=None, hi=None):
    """Like _vmap but caps extreme values so outliers don't distort percentile
    ranking. Used for instSharesChangePct (NFLX +892% is a real data outlier
    that would otherwise dominate the factor's top percentile)."""
    out = {}
    for i, s in enumerate(stocks):
        v = s.get(field)
        if v is None:
            out[i] = None
            continue
        try:
            f = float(v)
            if hi is not None and f > hi: f = hi
            if lo is not None and f < lo: f = lo
            out[i] = f
        except Exception:
            out[i] = None
    return out


def compute_steal_score(stocks):
    """Computes stealScore (0-100), stealRank (1=highest), stealBucket label
    in-place for each stock. Mutates the list."""
    if not stocks:
        return

    # ── 12 factor weights — sum = 100 (Stage 9 added 3 factors) ──
    factors = [
        # (label, weight, value_map, invert_lower_is_better)
        ("valuation_pe",       8, _vmap(stocks, "peRatio"),       True),
        ("valuation_evebitda", 8, _vmap(stocks, "evEbitda"),      True),
        ("growth_revenue",     12, _vmap(stocks, "revenueGrowth"), False),
        ("profit_opmargin",     7, _vmap(stocks, "operatingMargin"), False),
        ("profit_roic",         6, _vmap(stocks, "roic"),          False),
        ("earnings_quality",    8, _vmap_quality(stocks),          False),
        ("balance_de",          4, _vmap(stocks, "debtToEquity"),  True),
        ("balance_currratio",   3, _vmap(stocks, "currentRatio"),  False),
        ("momentum_6m",         8, _vmap(stocks, "chg6m"),         False),
        ("inst_flow",           7, _vmap_capped(stocks, "instQoQChgPct", lo=-50, hi=100), False),
        ("insider_flow",        4, _vmap(stocks, "insiderNet90dUsd"), False),
        ("estimate_revisions",  4, _vmap(stocks, "beatStreak"),    False),
        # ── STAGE 9: 3 new factors ──
        # Analyst grades consensus — sentiment of professional analysts
        ("analyst_grades",      7, _vmap(stocks, "gradesScore"),   False),
        # DCF undervaluation — the value-investing flagship signal
        ("dcf_upside",          8, _vmap_capped(stocks, "dcfUpsidePct", lo=-100, hi=300), False),
        # Political buying — alpha from informed insiders (Sen/House trades)
        ("political_buying",    6, _vmap(stocks, "politicalBuyersN90d"), False),
    ]
    # Total weight: 8+8+12+7+6+8+4+3+8+7+4+4+7+8+6 = 100

    factor_ranks = {}
    for label, weight, val_map, invert in factors:
        factor_ranks[label] = _percentile_ranks(val_map, invert=invert)

    for i, s in enumerate(stocks):
        total_weight = 0
        weighted_sum = 0.0
        contributions = {}
        for label, weight, val_map, invert in factors:
            rk = factor_ranks[label].get(i)
            if rk is None:
                continue
            weighted_sum += rk * weight
            total_weight += weight
            contributions[label] = {"rank": rk, "weight": weight}

        # Need ≥50% of total weight (i.e. factor coverage) for a meaningful score
        if total_weight >= 50:
            score = round(weighted_sum / total_weight, 1)
        else:
            score = None
        s["stealScore"]   = score
        s["stealFactors"] = contributions
        s["stealBucket"]  = (
            "STEAL"   if score is not None and score >= 90 else
            "PREMIUM" if score is not None and score >= 80 else
            "QUALITY" if score is not None and score >= 70 else
            None
        )

    # stealRank: 1 = highest score among stocks WITH a score
    scored = [(i, s["stealScore"]) for i, s in enumerate(stocks)
                if s.get("stealScore") is not None]
    if not scored:
        return

    # ── MIN-MAX RESCALING ──
    # Raw scores are percentile-rank averages, which naturally cluster around
    # 50 with bell-curve ceiling around 75-80. Rescale so the OBSERVED top
    # stock gets 100 — makes thresholds (90+ STEAL, 80+ PREMIUM) meaningful.
    raw_scores = [sc for _, sc in scored]
    raw_min = min(raw_scores)
    raw_max = max(raw_scores)
    raw_range = raw_max - raw_min
    if raw_range < 1e-6:
        rescale = lambda s: 50.0
    else:
        # Stretch so [raw_min, raw_max] maps to [0, 100]
        rescale = lambda s: round((s - raw_min) / raw_range * 100.0, 1)
    for i, s in enumerate(stocks):
        if s.get("stealScore") is not None:
            raw = s["stealScore"]
            s["stealScoreRaw"] = raw   # keep raw for transparency
            s["stealScore"] = rescale(raw)
            # Re-bucket on the rescaled score
            score = s["stealScore"]
            s["stealBucket"] = (
                "STEAL"   if score >= 90 else
                "PREMIUM" if score >= 80 else
                "QUALITY" if score >= 70 else
                None
            )

    # Re-sort + assign rank using rescaled scores
    rescored = [(i, s["stealScore"]) for i, s in enumerate(stocks)
                  if s.get("stealScore") is not None]
    rescored.sort(key=lambda x: -x[1])
    for rank_pos, (i, _sc) in enumerate(rescored):
        stocks[i]["stealRank"] = rank_pos + 1


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

    # ── STAGE 3: STEAL SCORE post-processing ────────────────────────────
    # 9-factor weighted composite score, percentile-ranked vs the universe.
    print("Computing Steal Score across universe...")
    compute_steal_score(stocks)
    steals_90 = sum(1 for s in stocks if (s.get("stealScore") or 0) >= 90)
    steals_80 = sum(1 for s in stocks if (s.get("stealScore") or 0) >= 80)
    print(f"  Steal Score: {steals_90} stocks ≥90 (🔥 STEAL), {steals_80} stocks ≥80 (💎 PREMIUM)")

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

    # ── STAGE 7: Daily snapshot + just-crossed diff ────────────────────
    # Save today's payload to snapshots/YYYY-MM-DD.json (latest-wins on
    # multi-runs/day). Try to find yesterday's snapshot (or oldest within
    # the last 7 days) and compute a diff to surface stocks that newly
    # entered key categories. Write to screener/just-crossed.json.
    try:
        write_snapshot_and_diff(stocks, payload)
    except Exception as e:
        print(f"[just-crossed] WARN write failed: {e}")

    # ── STAGE 8: History aggregation across all stored snapshots ──────
    # Aggregates the last 30 snapshots into screener/history.json so the
    # page can plot stealScore trajectories per stock + surface rising/
    # fading names ahead of tier changes.
    try:
        build_history()
    except Exception as e:
        print(f"[history] WARN build failed: {e}")

    # ── STAGE 11: Fire Telegram alerts asynchronously ──
    # After just-crossed.json is fresh, ping the alerts Lambda so any
    # high-conviction events flow to @Justhodl_bot. Async (Event invocation)
    # so this Lambda's response isn't blocked.
    try:
        import boto3 as _boto3
        _lam = _boto3.client("lambda", region_name="us-east-1")
        _lam.invoke(
            FunctionName="justhodl-screener-alerts",
            InvocationType="Event",
            Payload=b"{}")
        print("[alerts] async-fired justhodl-screener-alerts")
    except Exception as e:
        print(f"[alerts] WARN fire failed: {e}")

    return {"statusCode":200,"headers":hdrs,"body":json.dumps({
        "success":True,"count":len(stocks),
        "elapsed_seconds":round(elapsed,1),"generated_at":payload["generated_at"]
    })}


def write_snapshot_and_diff(today_stocks, today_payload):
    """Write today's snapshot, try to load yesterday's, compute crossings."""
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_key = f"screener/snapshots/{today_iso}.json"

    # Slim snapshot — strip large fields we don't need for diffs
    slim_stocks = []
    for s in today_stocks:
        slim_stocks.append({
            "symbol": s.get("symbol"),
            "name": s.get("name"),
            "sector": s.get("sector"),
            "marketCap": s.get("marketCap"),
            "price": s.get("price"),
            "stealScore": s.get("stealScore"),
            "stealBucket": s.get("stealBucket"),
            "stealRank": s.get("stealRank"),
            "insiderSignal": s.get("insiderSignal"),
            "insiderNet90dUsd": s.get("insiderNet90dUsd"),
            "insiderBuyersN90d": s.get("insiderBuyersN90d"),
            "beatStreak": s.get("beatStreak"),
            "crossSignal": s.get("crossSignal"),
            "sustainable3y": s.get("sustainable3y"),
            "sustainableQuality": s.get("sustainableQuality"),
            "revenueGrowth": s.get("revenueGrowth"),
            "fcfYieldCalc": s.get("fcfYieldCalc"),
            "buybackYield": s.get("buybackYield"),
            "operatingMargin": s.get("operatingMargin"),
            "chg1m": s.get("chg1m"),
            "chg6m": s.get("chg6m"),
            # Stage 9 fields tracked for just-crossed events
            "politicalSignal": s.get("politicalSignal"),
            "politicalBuyersN90d": s.get("politicalBuyersN90d"),
            "politicalNet90dUsd": s.get("politicalNet90dUsd"),
            "priceTargetUpsidePct": s.get("priceTargetUpsidePct"),
            "gradesScore": s.get("gradesScore"),
            "upgradeNet30d": s.get("upgradeNet30d"),
            "dcfUpsidePct": s.get("dcfUpsidePct"),
            "esgScoreNumeric": s.get("esgScoreNumeric"),
            # Stage 10 fields
            "instSignal": s.get("instSignal"),
            "instInvestorsHolding": s.get("instInvestorsHolding"),
            "instInvestorsChgPct": s.get("instInvestorsChgPct"),
            "instSharesChangePct": s.get("instSharesChangePct"),
            "forwardRevenueGrowth": s.get("forwardRevenueGrowth"),
            "forwardPE": s.get("forwardPE"),
            # Stage 11 fields
            "newsCount30d": s.get("newsCount30d"),
            "newsCount7d": s.get("newsCount7d"),
            "newsSentiment30d": s.get("newsSentiment30d"),
        })
    snap_payload = {
        "snapshot_date": today_iso,
        "generated_at": today_payload["generated_at"],
        "count": len(slim_stocks),
        "stocks": slim_stocks,
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=snap_key,
        Body=json.dumps(snap_payload, separators=(",", ":")),
        ContentType="application/json", CacheControl="max-age=86400")
    print(f"[just-crossed] snapshot saved: {snap_key}")

    # Find the most recent prior snapshot (walk back up to 7 days)
    from datetime import timedelta as _td
    today_dt = datetime.now(timezone.utc).date()
    prior_snap = None
    prior_date = None
    for back in range(1, 8):
        check_date = (today_dt - _td(days=back)).isoformat()
        check_key = f"screener/snapshots/{check_date}.json"
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=check_key)
            prior_snap = json.loads(obj["Body"].read())
            prior_date = check_date
            print(f"[just-crossed] comparing today ({today_iso}) vs {prior_date}")
            break
        except Exception:
            continue

    if not prior_snap:
        print("[just-crossed] no prior snapshot found within 7 days — skipping diff")
        return

    # Build symbol-keyed lookups
    today_by_sym = {s["symbol"]: s for s in slim_stocks}
    prior_by_sym = {s["symbol"]: s for s in (prior_snap.get("stocks") or [])}

    events = []

    for symbol, today_s in today_by_sym.items():
        prior_s = prior_by_sym.get(symbol)
        if not prior_s:
            continue  # New symbol in S&P 500 — skip (no comparison)

        common = {
            "symbol": symbol,
            "name": today_s.get("name"),
            "sector": today_s.get("sector"),
            "marketCap": today_s.get("marketCap"),
            "price": today_s.get("price"),
            "stealScore": today_s.get("stealScore"),
            "stealBucket": today_s.get("stealBucket"),
            "stealRank": today_s.get("stealRank"),
            "chg1m": today_s.get("chg1m"),
        }

        # ── 1. STEAL TIER changes ──
        prior_tier = prior_s.get("stealBucket")
        today_tier = today_s.get("stealBucket")
        TIER_ORDER = {None: 0, "QUALITY": 1, "PREMIUM": 2, "STEAL": 3}
        prior_rank = TIER_ORDER.get(prior_tier, 0)
        today_rank = TIER_ORDER.get(today_tier, 0)
        if today_rank > prior_rank:
            events.append({**common, "type": "ENTERED_TIER",
                "from": prior_tier, "to": today_tier,
                "from_score": prior_s.get("stealScore"),
                "to_score": today_s.get("stealScore"),
                "significance": 80 + (today_rank * 10),  # 90/100/110
            })
        elif today_rank < prior_rank and prior_rank > 0:
            events.append({**common, "type": "EXITED_TIER",
                "from": prior_tier, "to": today_tier,
                "from_score": prior_s.get("stealScore"),
                "to_score": today_s.get("stealScore"),
                "significance": 50 + (prior_rank * 5),
            })

        # ── 2. STEAL SCORE jumps (≥5 points up) ──
        prior_score = prior_s.get("stealScore")
        today_score = today_s.get("stealScore")
        if prior_score is not None and today_score is not None:
            delta = today_score - prior_score
            if delta >= 5:
                events.append({**common, "type": "SCORE_JUMP",
                    "from": round(prior_score, 1),
                    "to": round(today_score, 1),
                    "delta": round(delta, 1),
                    "significance": min(95, 50 + delta * 2),
                })
            elif delta <= -5:
                events.append({**common, "type": "SCORE_DROP",
                    "from": round(prior_score, 1),
                    "to": round(today_score, 1),
                    "delta": round(delta, 1),
                    "significance": min(70, 40 + abs(delta) * 1.5),
                })

        # ── 3. INSIDER SIGNAL flips ──
        prior_ins = prior_s.get("insiderSignal")
        today_ins = today_s.get("insiderSignal")
        if prior_ins != today_ins and today_ins:
            if today_ins == "buying":
                events.append({**common, "type": "INSIDER_TURNED_BUYING",
                    "from": prior_ins, "to": today_ins,
                    "insider_net": today_s.get("insiderNet90dUsd"),
                    "significance": 75,
                })
            elif today_ins == "selling":
                events.append({**common, "type": "INSIDER_TURNED_SELLING",
                    "from": prior_ins, "to": today_ins,
                    "insider_net": today_s.get("insiderNet90dUsd"),
                    "significance": 60,
                })

        # ── 4. BEAT STREAK milestones (newly hit 3, 5, 7+) ──
        prior_streak = prior_s.get("beatStreak") or 0
        today_streak = today_s.get("beatStreak") or 0
        for milestone in (3, 5, 7, 10):
            if today_streak >= milestone and prior_streak < milestone:
                events.append({**common, "type": f"BEAT_STREAK_{milestone}",
                    "from": prior_streak, "to": today_streak,
                    "significance": 50 + milestone * 3,
                })
                break

        # ── 5. GOLDEN / DEATH CROSS — newly detected ──
        prior_cross = prior_s.get("crossSignal")
        today_cross = today_s.get("crossSignal")
        if today_cross and today_cross != prior_cross:
            if today_cross == "GOLDEN":
                events.append({**common, "type": "GOLDEN_CROSS",
                    "from": prior_cross, "to": today_cross,
                    "significance": 70,
                })
            elif today_cross == "DEATH":
                events.append({**common, "type": "DEATH_CROSS",
                    "from": prior_cross, "to": today_cross,
                    "significance": 55,
                })

        # ── 6. SUSTAINABLE QUALITY flag flipped ──
        if today_s.get("sustainableQuality") and not prior_s.get("sustainableQuality"):
            events.append({**common, "type": "BECAME_SUSTAINABLE_QUALITY",
                "to": True, "significance": 65})

        # ── 7. FCF YIELD threshold crossings ──
        prior_fcfy = prior_s.get("fcfYieldCalc") or 0
        today_fcfy = today_s.get("fcfYieldCalc") or 0
        for threshold, label in [(10, "FCF_YIELD_10"), (5, "FCF_YIELD_5")]:
            if today_fcfy >= threshold and prior_fcfy < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_fcfy, 1),
                    "to": round(today_fcfy, 1),
                    "significance": 50 + threshold * 2,
                })
                break

        # ── 8. REVENUE GROWTH acceleration ──
        prior_rg = prior_s.get("revenueGrowth") or 0
        today_rg = today_s.get("revenueGrowth") or 0
        for threshold, label in [(25, "REV_GROWTH_25"), (15, "REV_GROWTH_15")]:
            if today_rg >= threshold and prior_rg < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_rg, 1),
                    "to": round(today_rg, 1),
                    "significance": 50 + threshold * 1.5,
                })
                break

        # ── 9. BUYBACK YIELD threshold ──
        prior_bb = prior_s.get("buybackYield") or 0
        today_bb = today_s.get("buybackYield") or 0
        for threshold, label in [(5, "BUYBACK_5"), (2, "BUYBACK_2")]:
            if today_bb >= threshold and prior_bb < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_bb, 1),
                    "to": round(today_bb, 1),
                    "significance": 50 + threshold * 4,
                })
                break

        # ════════════════════════════════════════════════════
        # STAGE 9 event types — Political + Analyst + DCF
        # ════════════════════════════════════════════════════

        # ── 10. POLITICAL signal flipped (Senate/House started buying) ──
        prior_pol = prior_s.get("politicalSignal")
        today_pol = today_s.get("politicalSignal")
        if prior_pol != today_pol and today_pol == "buying":
            events.append({**common, "type": "POLITICIANS_TURNED_BUYING",
                "from": prior_pol, "to": today_pol,
                "buyers": today_s.get("politicalBuyersN90d"),
                "net_usd": today_s.get("politicalNet90dUsd"),
                "significance": 85,
            })
        elif prior_pol != today_pol and today_pol == "selling":
            events.append({**common, "type": "POLITICIANS_TURNED_SELLING",
                "from": prior_pol, "to": today_pol,
                "significance": 60,
            })

        # ── 11. New political buyers added (cluster buying detected) ──
        prior_pol_buyers = prior_s.get("politicalBuyersN90d") or 0
        today_pol_buyers = today_s.get("politicalBuyersN90d") or 0
        if today_pol_buyers >= 3 and prior_pol_buyers < 3:
            events.append({**common, "type": "POLITICAL_CLUSTER_BUYING",
                "from": prior_pol_buyers, "to": today_pol_buyers,
                "significance": 90,
            })

        # ── 12. Analyst PRICE TARGET upside crossed thresholds ──
        prior_pt = prior_s.get("priceTargetUpsidePct") or 0
        today_pt = today_s.get("priceTargetUpsidePct") or 0
        for threshold, label in [(50, "TARGET_UPSIDE_50"), (25, "TARGET_UPSIDE_25"),
                                    (15, "TARGET_UPSIDE_15")]:
            if today_pt >= threshold and prior_pt < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_pt, 1),
                    "to": round(today_pt, 1),
                    "significance": 60 + threshold * 0.4,
                })
                break

        # ── 13. ANALYST UPGRADE SURGE (net upgrades crossed thresholds) ──
        prior_up_net = prior_s.get("upgradeNet30d") or 0
        today_up_net = today_s.get("upgradeNet30d") or 0
        if today_up_net >= 3 and prior_up_net < 3:
            events.append({**common, "type": "ANALYST_UPGRADE_SURGE",
                "from": prior_up_net, "to": today_up_net,
                "significance": 70,
            })

        # ── 14. ANALYST CONSENSUS strengthened ──
        prior_gs = prior_s.get("gradesScore")
        today_gs = today_s.get("gradesScore")
        if prior_gs is not None and today_gs is not None:
            gs_delta = today_gs - prior_gs
            if gs_delta >= 10:
                events.append({**common, "type": "GRADES_IMPROVED",
                    "from": round(prior_gs, 1),
                    "to": round(today_gs, 1),
                    "delta": round(gs_delta, 1),
                    "significance": 55 + gs_delta,
                })

        # ── 15. DCF UPSIDE crossed thresholds ──
        prior_dcf = prior_s.get("dcfUpsidePct") or 0
        today_dcf = today_s.get("dcfUpsidePct") or 0
        for threshold, label in [(100, "DCF_UPSIDE_100"), (50, "DCF_UPSIDE_50"),
                                    (25, "DCF_UPSIDE_25")]:
            if today_dcf >= threshold and prior_dcf < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_dcf, 1),
                    "to": round(today_dcf, 1),
                    "significance": 55 + threshold * 0.3,
                })
                break

        # ── 16. ESG rating improved ──
        prior_esg = prior_s.get("esgScoreNumeric") or 0
        today_esg = today_s.get("esgScoreNumeric") or 0
        if today_esg >= prior_esg + 10 and today_esg >= 70:
            events.append({**common, "type": "ESG_RATING_IMPROVED",
                "from": prior_esg, "to": today_esg,
                "significance": 50,
            })

        # ════════════════════════════════════════════════════
        # STAGE 10 event types — Institutional + Forward
        # ════════════════════════════════════════════════════

        # ── 17. HEDGE FUNDS started accumulating (instSharesChangePct flipped positive ≥5%) ──
        prior_inst_chg = prior_s.get("instSharesChangePct") or 0
        today_inst_chg = today_s.get("instSharesChangePct") or 0
        if today_inst_chg >= 5 and prior_inst_chg < 5:
            events.append({**common, "type": "HEDGE_FUND_ACCUMULATING",
                "from": round(prior_inst_chg, 1),
                "to": round(today_inst_chg, 1),
                "significance": 75,
            })
        elif today_inst_chg <= -5 and prior_inst_chg > -5:
            events.append({**common, "type": "HEDGE_FUND_EXITING",
                "from": round(prior_inst_chg, 1),
                "to": round(today_inst_chg, 1),
                "significance": 60,
            })

        # ── 18. INSTITUTIONAL HOLDER COUNT crossed threshold ──
        prior_holders = prior_s.get("instInvestorsHolding") or 0
        today_holders = today_s.get("instInvestorsHolding") or 0
        prior_holders_chg = prior_s.get("instInvestorsChgPct") or 0
        today_holders_chg = today_s.get("instInvestorsChgPct") or 0
        if today_holders_chg >= 10 and prior_holders_chg < 10:
            events.append({**common, "type": "INST_HOLDERS_SURGE",
                "from": round(prior_holders_chg, 1),
                "to": round(today_holders_chg, 1),
                "holders": today_holders,
                "significance": 75,
            })

        # ── 19. FORWARD revenue growth acceleration ──
        prior_fwd = prior_s.get("forwardRevenueGrowth") or 0
        today_fwd = today_s.get("forwardRevenueGrowth") or 0
        for threshold, label in [(50, "FORWARD_GROWTH_50"),
                                    (25, "FORWARD_GROWTH_25"),
                                    (15, "FORWARD_GROWTH_15")]:
            if today_fwd >= threshold and prior_fwd < threshold:
                events.append({**common, "type": label,
                    "from": round(prior_fwd, 1),
                    "to": round(today_fwd, 1),
                    "significance": 50 + threshold * 0.5,
                })
                break

        # ── 20. CHEAP FORWARD P/E (newly under 15) ──
        prior_fpe = prior_s.get("forwardPE")
        today_fpe = today_s.get("forwardPE")
        if today_fpe is not None and today_fpe > 0 and today_fpe < 15:
            if prior_fpe is None or prior_fpe >= 15:
                events.append({**common, "type": "CHEAP_FORWARD_PE",
                    "from": prior_fpe, "to": today_fpe,
                    "significance": 70,
                })

        # ════════════════════════════════════════════════════
        # STAGE 11 event types — News momentum
        # ════════════════════════════════════════════════════

        # ── 21. NEWS SURGE — 7-day count doubled & ≥5 articles ──
        prior_n7 = prior_s.get("newsCount7d") or 0
        today_n7 = today_s.get("newsCount7d") or 0
        if today_n7 >= 5 and today_n7 >= prior_n7 * 2:
            events.append({**common, "type": "NEWS_SURGE",
                "from": prior_n7, "to": today_n7,
                "significance": 65,
            })

        # ── 22. NEWS SENTIMENT shift (became positive ≥+30) ──
        prior_sent = prior_s.get("newsSentiment30d")
        today_sent = today_s.get("newsSentiment30d")
        if (prior_sent is not None and today_sent is not None
                and today_sent >= 30 and prior_sent < 30):
            events.append({**common, "type": "NEWS_SENTIMENT_POSITIVE",
                "from": prior_sent, "to": today_sent,
                "significance": 55,
            })
        elif (prior_sent is not None and today_sent is not None
                and today_sent <= -30 and prior_sent > -30):
            events.append({**common, "type": "NEWS_SENTIMENT_NEGATIVE",
                "from": prior_sent, "to": today_sent,
                "significance": 50,
            })

    # Sort by significance descending (most newsworthy first)
    events.sort(key=lambda e: -e.get("significance", 0))

    # Bucket counts for the summary
    type_counts = {}
    for e in events:
        type_counts[e["type"]] = type_counts.get(e["type"], 0) + 1

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "comparison": {
            "today": today_iso,
            "previous": prior_date,
            "days_back": (today_dt - datetime.strptime(prior_date, "%Y-%m-%d").date()).days,
        },
        "n_events": len(events),
        "type_counts": type_counts,
        "events": events[:200],   # cap at 200 most significant
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="screener/just-crossed.json",
        Body=json.dumps(output, separators=(",", ":")),
        ContentType="application/json", CacheControl="max-age=3600")
    print(f"[just-crossed] {len(events)} events written")
    for typ, cnt in sorted(type_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {typ:<30} {cnt}")


# ════════════════════════════════════════════════════════════════════════
# STAGE 8 — History aggregation
# Walks the last 30 snapshots in screener/snapshots/, builds a per-stock
# time series + trend metrics, writes screener/history.json. Used by the
# screener page for sparklines, rising/fading tabs, and a click-to-open
# trajectory chart.
# ════════════════════════════════════════════════════════════════════════
def build_history(max_days=30):
    """Aggregate snapshots → screener/history.json with per-stock time series."""
    from datetime import datetime as _dt

    # 1. List all snapshots
    listing = s3.list_objects_v2(Bucket=S3_BUCKET,
                                   Prefix="screener/snapshots/",
                                   MaxKeys=200)
    objs = listing.get("Contents") or []
    if not objs:
        print("[history] no snapshots in S3 — skipping")
        return

    # Extract date from key: screener/snapshots/YYYY-MM-DD.json
    dated = []
    for o in objs:
        key = o["Key"]
        # Pull date out of the filename
        base = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        try:
            d = _dt.strptime(base, "%Y-%m-%d").date()
            dated.append((d, key))
        except ValueError:
            continue
    # Sort newest-first, take last N days
    dated.sort(key=lambda x: x[0], reverse=True)
    dated = dated[:max_days]
    # Reverse to chronological order for the time series
    dated.sort(key=lambda x: x[0])

    if len(dated) < 1:
        print("[history] no parsable snapshot dates")
        return

    print(f"[history] aggregating {len(dated)} snapshots: "
          f"{dated[0][0]} → {dated[-1][0]}")

    # 2. Load each snapshot + build per-symbol time series
    dates_iso = [d.isoformat() for d, _ in dated]
    # Pre-init per-symbol arrays — null where the symbol was missing that day
    n = len(dated)
    by_symbol = {}

    for date_idx, (date, key) in enumerate(dated):
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            snap = json.loads(obj["Body"].read())
        except Exception as e:
            print(f"[history]   skip {key}: {e}")
            continue
        snap_stocks = snap.get("stocks") or []
        for s in snap_stocks:
            sym = s.get("symbol")
            if not sym:
                continue
            if sym not in by_symbol:
                by_symbol[sym] = {
                    "name": s.get("name"),
                    "sector": s.get("sector"),
                    "scores":   [None] * n,
                    "buckets":  [None] * n,
                    "ranks":    [None] * n,
                    "insider":  [None] * n,
                    "beats":    [None] * n,
                    "crosses":  [None] * n,
                }
            # Always refresh name/sector to the most-recent observation
            if s.get("name"): by_symbol[sym]["name"] = s.get("name")
            if s.get("sector"): by_symbol[sym]["sector"] = s.get("sector")
            by_symbol[sym]["scores"][date_idx]  = s.get("stealScore")
            by_symbol[sym]["buckets"][date_idx] = s.get("stealBucket")
            by_symbol[sym]["ranks"][date_idx]   = s.get("stealRank")
            by_symbol[sym]["insider"][date_idx] = s.get("insiderSignal")
            by_symbol[sym]["beats"][date_idx]   = s.get("beatStreak")
            by_symbol[sym]["crosses"][date_idx] = s.get("crossSignal")

    # 3. Compute trend metrics per stock
    for sym, d in by_symbol.items():
        scores = d["scores"]
        # Find first and last non-null indices for valid trend windows
        valid_idx = [i for i, v in enumerate(scores) if v is not None]
        if not valid_idx:
            d["score_now"] = None
            d["score_7d_chg"] = None
            d["score_14d_chg"] = None
            d["score_30d_chg"] = None
            d["score_trend"] = "unknown"
            d["score_slope"] = None
            continue

        last_idx = valid_idx[-1]
        score_now = scores[last_idx]
        d["score_now"] = score_now

        # Helper: find latest score at or before idx (lookback window)
        def score_at_or_before(target_idx):
            for j in range(min(target_idx, last_idx), -1, -1):
                if scores[j] is not None:
                    return scores[j]
            return None

        # 7-day, 14-day, 30-day lookbacks (in trading-day approximations)
        d["score_7d_chg"]  = (round(score_now - (score_at_or_before(last_idx - 7) or score_now), 1)
                                if last_idx >= 7 else None)
        d["score_14d_chg"] = (round(score_now - (score_at_or_before(last_idx - 14) or score_now), 1)
                                if last_idx >= 14 else None)
        d["score_30d_chg"] = (round(score_now - (score_at_or_before(last_idx - 29) or score_now), 1)
                                if last_idx >= 29 else None)

        # Linear regression slope over the valid window — points/day
        # using least-squares on (x=days, y=score)
        xs = []
        ys = []
        for i in valid_idx:
            xs.append(i)
            ys.append(scores[i])
        m = len(xs)
        if m >= 3:
            mean_x = sum(xs) / m
            mean_y = sum(ys) / m
            num = sum((xs[k] - mean_x) * (ys[k] - mean_y) for k in range(m))
            den = sum((xs[k] - mean_x) ** 2 for k in range(m))
            slope = (num / den) if den > 0 else 0.0
            d["score_slope"] = round(slope, 3)
        else:
            d["score_slope"] = None

        # Trend label — combines magnitude of recent change + slope direction
        # Trending = either recent delta magnitude >= 5pts OR slope magnitude >= 0.5/day
        # Otherwise "stable"
        ref_chg = d["score_7d_chg"] or d["score_14d_chg"] or 0
        slope = d["score_slope"] or 0
        if ref_chg >= 5 or slope >= 0.5:
            d["score_trend"] = "rising"
        elif ref_chg <= -5 or slope <= -0.5:
            d["score_trend"] = "falling"
        else:
            d["score_trend"] = "stable"

    # 4. Summary buckets
    rising = [sym for sym, d in by_symbol.items() if d.get("score_trend") == "rising"]
    falling = [sym for sym, d in by_symbol.items() if d.get("score_trend") == "falling"]
    # Sort rising by largest positive 7d change (or slope as fallback)
    rising.sort(key=lambda s: -(by_symbol[s].get("score_7d_chg") or 0
                                  or (by_symbol[s].get("score_slope") or 0) * 30))
    falling.sort(key=lambda s: (by_symbol[s].get("score_7d_chg") or 0
                                  or (by_symbol[s].get("score_slope") or 0) * 30))

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dates": dates_iso,
        "n_days": len(dates_iso),
        "n_symbols": len(by_symbol),
        "summary": {
            "rising_count": len(rising),
            "falling_count": len(falling),
            "stable_count": len(by_symbol) - len(rising) - len(falling),
            "top_5_rising": rising[:5],
            "top_5_falling": falling[:5],
        },
        "by_symbol": by_symbol,
    }
    body = json.dumps(output, separators=(",", ":"))
    s3.put_object(
        Bucket=S3_BUCKET, Key="screener/history.json",
        Body=body, ContentType="application/json",
        CacheControl="max-age=14400")
    print(f"[history] wrote {len(body)/1024:.1f} KB · "
          f"{len(by_symbol)} symbols × {len(dates_iso)} days · "
          f"rising={len(rising)} falling={len(falling)}")
