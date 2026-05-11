import json, time, boto3, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

FMP      = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE     = "https://financialmodelingprep.com/stable"
S3_BUCKET= "justhodl-dashboard-live"
CACHE_KEY= "screener/data.json"
CACHE_TTL= 4 * 3600
WORKERS  = 1  # was 2 → 1 after adding institutional + insider + surprises endpoints. 9 calls/stock × 503 stocks = 4.5k calls. At ~10 req/sec we finish in ~7.5min, comfortably under FMP's 750/min limit.

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
    # ── STAGE 2 ADDS (2026-05-11) ─────────────────────────────
    # Institutional 13F ownership (latest 2 quarters to detect QoQ change)
    inst    = fmp("institutional-ownership/symbol-positions-summary",
                  f"&symbol={symbol}&limit=2")
    # Insider trades — recent transactions (Form 4); we read most recent 50
    insider = fmp("insider-trading", f"&symbol={symbol}&limit=50")
    # Analyst earnings estimates + surprises
    surprises = fmp("earnings-surprises", f"&symbol={symbol}&limit=8")

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
                shares = float(t.get("securitiesTransacted") or 0)
                price = float(t.get("price") or 0)
                amount = abs(shares * price)
                txt = (t.get("transactionType") or "").upper()
                acq = (t.get("acquistionOrDisposition") or t.get("acquisitionOrDisposition") or "").upper()
                name = t.get("reportingName") or t.get("name") or "unknown"
                is_buy = ("P-PURCHASE" in txt) or ("A-AWARD" in txt and acq == "A") or (acq == "A" and "P" in txt)
                is_sell = ("S-SALE" in txt) or (acq == "D")
                if is_buy and not is_sell:
                    insider_buys_90d_usd += amount
                    insider_buyers_90d.add(name)
                    insider_recent_buys_count += 1
                elif is_sell:
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

    # Earnings surprise streak (count consecutive recent beats)
    beat_streak = 0
    last_surprise_pct = None
    avg_surprise_pct = None
    if isinstance(surprises, list) and surprises:
        # surprises are typically ordered most-recent-first; iterate forward
        surprise_pcts = []
        for s in surprises:
            actual = sf(s.get("actualEarningResult"))
            est = sf(s.get("estimatedEarning"))
            if actual is None or est is None:
                continue
            beat = actual > est
            if beat_streak >= 0 and beat:
                beat_streak += 1
            elif not beat:
                # Stop counting — streak ends on first miss
                if beat_streak == 0:
                    beat_streak = 0
                break
            if est != 0:
                surprise_pcts.append((actual - est) / abs(est) * 100)
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
        "altmanZ":         altman_z,
        # Institutional (derived)
        "instSignal":      inst_signal,
        "instHolders":     None,
        "instChgPct":      None,
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


def compute_steal_score(stocks):
    """Computes stealScore (0-100), stealRank (1=highest), stealBucket label
    in-place for each stock. Mutates the list."""
    if not stocks:
        return

    # ── 9 factor weights — sum = 100 ──
    factors = [
        # (label, weight, value_map, invert_lower_is_better)
        ("valuation_pe",       10, _vmap(stocks, "peRatio"),       True),
        ("valuation_evebitda", 10, _vmap(stocks, "evEbitda"),      True),
        ("growth_revenue",     15, _vmap(stocks, "revenueGrowth"), False),
        ("profit_opmargin",     8, _vmap(stocks, "operatingMargin"), False),
        ("profit_roic",         7, _vmap(stocks, "roic"),          False),
        ("earnings_quality",   10, _vmap_quality(stocks),          False),
        ("balance_de",          5, _vmap(stocks, "debtToEquity"),  True),
        ("balance_currratio",   5, _vmap(stocks, "currentRatio"),  False),
        ("momentum_6m",        10, _vmap(stocks, "chg6m"),         False),
        ("inst_flow",          10, _vmap(stocks, "instQoQChgPct"), False),
        ("insider_flow",        5, _vmap(stocks, "insiderNet90dUsd"), False),
        ("estimate_revisions",  5, _vmap(stocks, "beatStreak"),    False),
    ]
    # Total weight = 100

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
    scored.sort(key=lambda x: -x[1])
    for rank_pos, (i, _sc) in enumerate(scored):
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
    return {"statusCode":200,"headers":hdrs,"body":json.dumps({
        "success":True,"count":len(stocks),
        "elapsed_seconds":round(elapsed,1),"generated_at":payload["generated_at"]
    })}
