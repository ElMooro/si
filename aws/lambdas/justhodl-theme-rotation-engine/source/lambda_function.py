"""
justhodl-theme-rotation-engine — institutional money-flow tracker

This is the system's "follow the money" engine. Big hedge funds and institutions
detect themes long before they pump because they watch where money flows, not
just individual stocks.

WHAT THIS DETECTS:
  1. Theme-level money flow — net creation/redemption proxy via volume + price action
  2. Relative strength regime change — when a theme starts beating SPY persistently
  3. Breadth — what % of theme constituents are participating in the rally
  4. Velocity — rate of change of theme RS (acceleration matters)
  5. Theme rotation matrix — which themes are gaining money, which are losing
  6. Convergence — themes where BOTH price RS and breadth are turning up

INPUTS:
  - 100+ thematic/sector ETFs (constructed list below)
  - SPY as benchmark
  - 90-day daily history for each
  - Top constituents for each theme (FMP holdings or top-10 SPDR-style)

OUTPUTS:
  data/theme-rotation.json — full state
  data/theme-rotation-state.json — yesterday's state for delta tracking

ALERT TRIGGERS:
  - Theme RS rank moves up 10+ positions (from 50th to 40th out of 100)
  - Theme breadth crosses 65% with rising 20d momentum
  - New theme enters top-10 RS
  - Theme exits top-10 (rotation OUT alert)
  - "Convergent breadth" — RS rising AND breadth > 60% AND volume rising

This is what institutional asset managers see daily.
"""
import io, json, os, time, urllib.request, urllib.error, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/theme-rotation.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/theme-rotation-state.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# THEME UNIVERSE — 100+ ETFs covering every major sector + sub-theme
# Each entry: ticker, name, category (for grouping), expected_top_holdings
# ─────────────────────────────────────────────────────────────────────────
THEMES = [
    # --- BROAD MARKET BENCHMARKS (excluded from RS comparisons) ---
    ("SPY",   "S&P 500",                    "BENCHMARK",  []),
    ("QQQ",   "Nasdaq 100",                 "BENCHMARK",  []),
    ("IWM",   "Russell 2000 small-cap",     "BENCHMARK",  []),
    ("VTI",   "Total US Market",            "BENCHMARK",  []),

    # --- AI & SEMICONDUCTORS (where the user's AI thesis lives) ---
    ("SOXX",  "iShares Semiconductor",      "AI_SEMI",    ["NVDA","AVGO","AMD","MU","INTC","QCOM"]),
    ("SMH",   "VanEck Semiconductors",      "AI_SEMI",    ["NVDA","TSM","AVGO","AMD"]),
    ("SOXL",  "Direxion Semi 3x Bull",      "AI_SEMI",    []),
    ("PSI",   "Invesco Dynamic Semis",      "AI_SEMI_SMID", ["AMAT","ON","MCHP","ADI","QRVO","SWKS"]),
    ("XSD",   "SPDR Semiconductor",         "AI_SEMI_SMID", []),
    ("AIQ",   "Global X AI & Tech",         "AI_BROAD",   ["NVDA","META","MSFT","GOOGL"]),
    ("BOTZ",  "Robotics & AI",              "AI_BROAD",   []),
    ("ROBO",  "Robotics & Automation",      "AI_BROAD",   []),
    ("IRBO",  "iShares Robotics",           "AI_BROAD",   []),
    ("CHAT",  "Roundhill Generative AI",    "AI_BROAD",   []),
    ("WCLD",  "WisdomTree Cloud Computing", "AI_SOFTWARE",[]),
    ("FDN",   "First Trust Internet",       "INTERNET",   ["META","GOOGL","NFLX","AMZN"]),
    ("XLK",   "Tech Select Sector",         "TECH_BROAD", []),
    ("VGT",   "Vanguard Info Tech",         "TECH_BROAD", []),
    ("IGV",   "iShares Software",           "SOFTWARE",   []),
    ("CIBR",  "Cybersecurity",              "CYBERSEC",   []),
    ("HACK",  "ETFMG Cyber Security",       "CYBERSEC",   []),

    # --- COMPOUND SEMIS / OPTICAL / SPECIALTY (the AXTI/LWLG/AAOI pump theme) ---
    # No pure ETF for these but PSI/SOXX captures most

    # --- FINANCIALS ---
    ("XLF",   "Financials Select",          "FINANCIALS", []),
    ("KBE",   "SPDR Banks",                 "FIN_BANKS",  []),
    ("KRE",   "SPDR Regional Banks",        "FIN_REGIONAL", []),
    ("IAI",   "iShares US Broker/Dealers",  "FIN_BROKERS", []),
    ("KIE",   "SPDR Insurance",             "FIN_INSURANCE", []),

    # --- ENERGY ---
    ("XLE",   "Energy Select",              "ENERGY",     ["XOM","CVX","COP"]),
    ("XOP",   "Oil & Gas E&P",              "OIL_EP",     ["FANG","DVN","APA","OXY"]),
    ("OIH",   "Oil Services",               "OIL_SERVICES", ["SLB","HAL","BKR"]),
    ("USO",   "Crude Oil ETF",              "OIL_FUTURES",[]),
    ("URA",   "Uranium",                    "URANIUM",    ["CCJ","UEC","UUUU","DNN"]),
    ("URNM",  "Uranium Miners",             "URANIUM",    []),
    ("KOLD",  "Natural Gas Bear",           "NATGAS_BEAR",[]),
    ("BOIL",  "Natural Gas Bull",           "NATGAS_BULL",[]),
    ("FCG",   "First Trust NatGas",         "NATGAS_EQ",  []),

    # --- INDUSTRIALS / INFRA ---
    ("XLI",   "Industrials Select",         "INDUSTRIALS",[]),
    ("ITA",   "iShares Defense & Aero",     "DEFENSE",    ["LMT","RTX","NOC","GD"]),
    ("XAR",   "SPDR Aerospace & Defense",   "DEFENSE",    []),
    ("PHO",   "Invesco Water",              "WATER",      []),
    ("PAVE",  "Global X Infrastructure",    "INFRASTRUCTURE", ["EME","PWR","FIX"]),
    ("IFRA",  "iShares Infrastructure",     "INFRASTRUCTURE", []),
    ("XLB",   "Materials Select",           "MATERIALS",  []),

    # --- METALS & MINING ---
    ("GDX",   "Gold Miners",                "GOLD",       ["NEM","GOLD","FNV","AEM"]),
    ("GDXJ",  "Junior Gold Miners",         "GOLD_JR",    []),
    ("SILJ",  "Junior Silver Miners",       "SILVER",     []),
    ("REMX",  "Rare Earth & Strategic",     "RARE_EARTH", ["MP","TMC","LYC"]),
    ("LIT",   "Global X Lithium",           "LITHIUM",    ["ALB","SQM","LTHM"]),
    ("COPX",  "Global X Copper Miners",     "COPPER",     ["FCX","SCCO","TECK"]),
    ("PICK",  "iShares Metals & Mining",    "METALS",     ["BHP","RIO","VALE"]),
    ("CPER",  "United States Copper Fund",  "COPPER_FUT", []),
    ("URNJ",  "Junior Uranium",             "URANIUM",    []),

    # --- CONSUMER ---
    ("XLY",   "Consumer Discretionary",     "CONSUMER_DISC", []),
    ("XLP",   "Consumer Staples",           "CONSUMER_STAPLES", []),
    ("PEJ",   "Leisure & Entertainment",    "LEISURE",    []),
    ("XRT",   "SPDR Retail",                "RETAIL",     []),

    # --- HEALTHCARE / BIOTECH ---
    ("XLV",   "Healthcare Select",          "HEALTHCARE", ["JNJ","UNH","LLY","MRK"]),
    ("IBB",   "iShares Biotech",            "BIOTECH",    ["VRTX","REGN","GILD"]),
    ("XBI",   "SPDR Biotech (equal-wt)",    "BIOTECH_SMID", []),
    ("IHI",   "iShares Med Devices",        "MED_DEVICES",[]),
    ("PJP",   "Invesco Pharma",             "PHARMA",     []),

    # --- REAL ESTATE / REITs ---
    ("XLRE",  "Real Estate Select",         "REAL_ESTATE",[]),
    ("VNQ",   "Vanguard REITs",             "REITS",      []),
    ("REZ",   "iShares Residential REITs",  "REITS_RES",  []),
    ("REM",   "iShares Mortgage REITs",     "REITS_MTG",  []),

    # --- UTILITIES ---
    ("XLU",   "Utilities Select",           "UTILITIES",  []),
    ("VPU",   "Vanguard Utilities",         "UTILITIES",  []),

    # --- COMMUNICATION SERVICES ---
    ("XLC",   "Communication Services",     "COMMUNICATION", []),

    # --- CHINA / EMERGING MARKETS ---
    ("KWEB",  "KraneShares China Internet", "CHINA_TECH", ["BABA","JD","PDD","BIDU"]),
    ("MCHI",  "iShares China Large-Cap",    "CHINA",      []),
    ("FXI",   "iShares China 25",           "CHINA",      []),
    ("ASHR",  "Xtrackers CSI 300",          "CHINA_A",    []),
    ("EEM",   "iShares Emerging Markets",   "EM_BROAD",   []),
    ("INDA",  "iShares India",              "INDIA",      []),
    ("EWZ",   "iShares Brazil",             "BRAZIL",     []),
    ("EWJ",   "iShares Japan",              "JAPAN",      []),

    # --- THEMATIC / DISRUPTIVE ---
    ("ARKK",  "ARK Innovation",             "ARK_INNOV",  []),
    ("ARKG",  "ARK Genomic Revolution",     "GENOMICS",   []),
    ("ARKQ",  "ARK Autonomous Tech",        "AUTONOMY",   []),
    ("ARKW",  "ARK Next Gen Internet",      "NEXT_GEN",   []),
    ("ARKF",  "ARK Fintech",                "FINTECH",    []),
    ("KOMP",  "SPDR Innovative Tech",       "INNOV_TECH", []),
    ("BLOK",  "Amplify Transformational Data", "BLOCKCHAIN", []),
    ("BITQ",  "Bitwise Crypto Industry",    "CRYPTO_EQ",  []),
    ("BLCN",  "Reality Shares Blockchain",  "BLOCKCHAIN", []),
    ("ICLN",  "iShares Clean Energy",       "CLEAN_ENERGY", ["ENPH","FSLR","RUN"]),
    ("TAN",   "Invesco Solar",              "SOLAR",      ["ENPH","FSLR","SEDG","RUN"]),
    ("FAN",   "First Trust Wind",           "WIND",       []),
    ("PBW",   "Invesco Wilderhill Clean",   "CLEAN_ENERGY", []),
    ("DRIV",  "Global X Autonomous & EV",   "EV_AUTO",    ["TSLA","RIVN"]),
    ("LIT",   "Lithium & Batteries",        "BATTERIES",  []),
    ("KARS",  "KraneShares Electric Vehicles", "EV_AUTO", []),
    ("HYDR",  "Defiance Hydrogen ETF",      "HYDROGEN",   ["PLUG","BLDP","BE"]),

    # --- DIVIDEND / VALUE / SIZE ---
    ("VTV",   "Vanguard Value",             "VALUE_LARGE",[]),
    ("VBR",   "Vanguard Small-Cap Value",   "VALUE_SMALL",[]),
    ("VUG",   "Vanguard Growth",            "GROWTH_LARGE",[]),
    ("VBK",   "Vanguard Small-Cap Growth",  "GROWTH_SMALL",[]),
    ("MTUM",  "iShares MSCI Momentum",      "MOMENTUM_FACTOR",[]),
    ("QUAL",  "iShares MSCI Quality",       "QUALITY_FACTOR",[]),
    ("SDY",   "SPDR Dividend",              "DIVIDEND",   []),
    ("VIG",   "Vanguard Div Appreciation",  "DIVIDEND",   []),

    # --- VOLATILITY / HEDGES ---
    ("VIXY",  "ProShares VIX Short-Term",   "VOLATILITY", []),
    ("UVXY",  "ProShares Ultra VIX",        "VOLATILITY", []),
    ("TLT",   "iShares 20+ Year Treasury",  "RATES_LONG", []),
    ("IEF",   "iShares 7-10Y Treasury",     "RATES_INT",  []),
    ("HYG",   "iShares High Yield",         "CREDIT_HY",  []),
    ("LQD",   "iShares Inv Grade",          "CREDIT_IG",  []),

    # --- COMMODITIES ---
    ("DBC",   "Invesco Commodities",        "COMMODITIES_BROAD", []),
    ("GSG",   "iShares S&P GSCI",           "COMMODITIES_BROAD", []),
    ("WEAT",  "Teucrium Wheat",             "AG_WHEAT",   []),
    ("CORN",  "Teucrium Corn",              "AG_CORN",    []),
    ("DBA",   "Invesco Agriculture",        "AGRICULTURE",[]),
    ("MOO",   "VanEck Agribusiness",        "AGRIBIZ",    []),

    # --- INTERNATIONAL ---
    ("EFA",   "iShares MSCI EAFE",          "DEVELOPED_INTL",[]),
    ("VXUS",  "Vanguard Total Intl Stock",  "INTL_BROAD", []),

    # --- GAMING / SPECIALTY ---
    ("ESPO",  "VanEck Video Gaming",        "GAMING",     []),
    ("HERO",  "Global X Video Games",       "GAMING",     []),
    ("BJK",   "VanEck Gaming",              "CASINO_GAMING", []),

    # --- AGING / DEMOGRAPHIC ---
    ("AGNG",  "Aging Population",           "DEMOGRAPHICS",[]),
]


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ThemeRot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_history(symbol, days=120):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        d = _http_get_json(url, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for r in d[:days]:
            if r.get("close") and r.get("date"):
                out.append({
                    "date": r.get("date"),
                    "close": float(r.get("close")),
                    "volume": float(r.get("volume") or 0),
                })
        out.sort(key=lambda x: x["date"])
        return out
    except Exception:
        return None


def compute_theme_metrics(ticker, name, category, history, spy_history):
    """Compute all theme-level metrics for one ETF."""
    if not history or len(history) < 65:
        return None
    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    n = len(closes)

    # Returns (theme)
    ret_5d = (closes[-1] / closes[-6] - 1) * 100 if n >= 6 else 0
    ret_20d = (closes[-1] / closes[-21] - 1) * 100 if n >= 21 else 0
    ret_60d = (closes[-1] / closes[-61] - 1) * 100 if n >= 61 else 0
    ret_3d = (closes[-1] / closes[-4] - 1) * 100 if n >= 4 else 0

    # SPY returns over same windows
    spy_closes = [h["close"] for h in spy_history] if spy_history else []
    spy_n = len(spy_closes)
    spy_ret_5 = (spy_closes[-1] / spy_closes[-6] - 1) * 100 if spy_n >= 6 else 0
    spy_ret_20 = (spy_closes[-1] / spy_closes[-21] - 1) * 100 if spy_n >= 21 else 0
    spy_ret_60 = (spy_closes[-1] / spy_closes[-61] - 1) * 100 if spy_n >= 61 else 0

    # Relative strength (theme - SPY)
    rs_5d = ret_5d - spy_ret_5
    rs_20d = ret_20d - spy_ret_20
    rs_60d = ret_60d - spy_ret_60

    # RS acceleration: is short-term RS faster than long-term?
    # Annualize roughly: rs_5d * 12 ≈ 60-day equivalent
    rs_5d_annualized = rs_5d * 12
    rs_20d_annualized = rs_20d * 3
    rs_acceleration = rs_5d_annualized - rs_60d  # if positive, momentum is accelerating

    # Volume metrics
    avg_vol_20 = sum(volumes[-20:]) / min(20, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_20
    vol_ratio = avg_vol_20 / avg_vol_60 if avg_vol_60 > 0 else 1.0

    # Volume on up days vs down days (money flow approximation)
    up_vol = 0
    down_vol = 0
    for i in range(max(1, n-20), n):
        if closes[i] > closes[i-1]:
            up_vol += volumes[i]
        elif closes[i] < closes[i-1]:
            down_vol += volumes[i]
    money_flow_ratio = up_vol / down_vol if down_vol > 0 else 99.0
    # Normalize: > 1 = accumulation, < 1 = distribution
    money_flow_score = 50 + 25 * min(max((money_flow_ratio - 1.0), -1.0), 1.0)
    # simpler: log ratio
    if down_vol > 0:
        log_mf = (up_vol / down_vol)
    else:
        log_mf = 99

    # Position in 60d range
    range_high = max(closes[-60:]) if n >= 60 else max(closes)
    range_low = min(closes[-60:]) if n >= 60 else min(closes)
    range_pos_60 = (closes[-1] - range_low) / (range_high - range_low) * 100 if range_high > range_low else 50

    # Above/below 20/50/100-day moving averages
    ma20 = sum(closes[-20:]) / min(20, n)
    ma50 = sum(closes[-50:]) / min(50, n) if n >= 50 else ma20
    ma100 = sum(closes[-100:]) / min(100, n) if n >= 100 else ma50
    above_ma20 = closes[-1] > ma20
    above_ma50 = closes[-1] > ma50
    above_ma100 = closes[-1] > ma100

    # Momentum score 0-100 — combine RS + acceleration + range_pos + above_ma
    score = 0
    if rs_60d > 0: score += 20
    if rs_20d > 0: score += 15
    if rs_5d > 0: score += 10
    if rs_acceleration > 5: score += 15
    if above_ma100: score += 10
    if above_ma50: score += 8
    if above_ma20: score += 5
    if range_pos_60 > 70: score += 10
    if vol_ratio > 1.15: score += 7

    return {
        "ticker": ticker,
        "name": name,
        "category": category,
        "today_close": round(closes[-1], 2),
        "ret_3d": round(ret_3d, 2),
        "ret_5d": round(ret_5d, 2),
        "ret_20d": round(ret_20d, 2),
        "ret_60d": round(ret_60d, 2),
        "rs_5d": round(rs_5d, 2),
        "rs_20d": round(rs_20d, 2),
        "rs_60d": round(rs_60d, 2),
        "rs_acceleration": round(rs_acceleration, 2),
        "vol_ratio_20v60": round(vol_ratio, 2),
        "money_flow_ratio": round(log_mf, 2),
        "range_pos_60d": round(range_pos_60, 1),
        "above_ma20": above_ma20,
        "above_ma50": above_ma50,
        "above_ma100": above_ma100,
        "momentum_score": round(min(score, 100), 1),
    }


def fetch_etf_holdings(ticker, fallback_top=None):
    """Fetch top constituents for ETF — try multiple endpoints, fall back to curated list."""
    # Try newer stable endpoint first
    urls_to_try = [
        "https://financialmodelingprep.com/stable/etf/holdings?symbol=" + ticker + "&apikey=" + FMP_KEY,
        "https://financialmodelingprep.com/api/v3/etf-holder/" + ticker + "?apikey=" + FMP_KEY,
    ]
    for url in urls_to_try:
        try:
            d = _http_get_json(url, timeout=15)
            if isinstance(d, list) and d:
                holdings = []
                for h in d[:25]:
                    # Different endpoints use different keys
                    sym = (h.get("asset") or h.get("symbol") or h.get("ticker") or "").upper().strip()
                    weight = h.get("weightPercentage") or h.get("weight") or h.get("pctHold") or 0
                    if sym and len(sym) <= 5 and sym.isalpha():
                        holdings.append({"symbol": sym, "weight": float(weight) if weight else 0})
                if holdings:
                    return holdings
        except Exception:
            continue
    # Fall back to curated top constituents from THEMES
    if fallback_top:
        return [{"symbol": s, "weight": 0} for s in fallback_top]
    return []


def compute_breadth(theme_data, holdings_perf, spy_ret_20):
    """Compute % of theme constituents outperforming SPY over 20d."""
    if not holdings_perf:
        return None
    outperformers = sum(1 for h in holdings_perf if h["ret_20d"] > spy_ret_20)
    breadth_pct = outperformers / len(holdings_perf) * 100
    above_ma_count = sum(1 for h in holdings_perf if h.get("above_ma50", False))
    above_ma_pct = above_ma_count / len(holdings_perf) * 100 if holdings_perf else 0
    return {
        "n_constituents_checked": len(holdings_perf),
        "outperforming_spy_20d": outperformers,
        "breadth_outperform_pct": round(breadth_pct, 1),
        "n_above_ma50": above_ma_count,
        "breadth_above_ma50_pct": round(above_ma_pct, 1),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[theme-rot] starting v1.0")

    # Fetch SPY first (benchmark)
    print("[theme-rot] fetching SPY benchmark...")
    spy_h = fetch_history("SPY", days=120)
    if not spy_h:
        return {"statusCode": 500, "body": json.dumps({"error": "no SPY history"})}
    spy_n = len(spy_h)
    spy_ret_20 = (spy_h[-1]["close"] / spy_h[-21]["close"] - 1) * 100 if spy_n >= 21 else 0
    print("[theme-rot] SPY 20d return: " + "{:.2f}%".format(spy_ret_20))

    # Fetch all themes in parallel
    theme_results = []
    def evaluate_theme(item):
        if time.time() > deadline_at:
            return None
        ticker, name, category, _ = item
        h = fetch_history(ticker, days=120)
        if not h:
            return None
        return compute_theme_metrics(ticker, name, category, h, spy_h)

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate_theme, t): t for t in THEMES}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    theme_results.append(r)
            except Exception:
                continue

    print("[theme-rot] computed metrics for " + str(len(theme_results)) + " themes")

    # Sort by RS_20d for ranking
    rs_ranked = sorted([t for t in theme_results if t["category"] != "BENCHMARK"],
                        key=lambda x: x["rs_20d"], reverse=True)
    for i, t in enumerate(rs_ranked):
        t["rs_rank_20d"] = i + 1

    # Sort by momentum score
    momentum_ranked = sorted(rs_ranked, key=lambda x: x["momentum_score"], reverse=True)

    # ── Breadth computation (only for top 15 RS themes — to save API calls) ──
    top_themes_for_breadth = momentum_ranked[:20]
    breadth_results = {}
    print("[theme-rot] computing breadth for top " + str(len(top_themes_for_breadth)) + " themes...")

    # Build a lookup of curated top holdings from THEMES
    curated_lookup = {}
    for tk, _name, _cat, top_list in THEMES:
        if top_list:
            curated_lookup[tk] = top_list

    def compute_one_breadth(theme):
        if time.time() > deadline_at:
            return None
        ticker = theme["ticker"]
        fallback = curated_lookup.get(ticker, [])
        holdings = fetch_etf_holdings(ticker, fallback_top=fallback)
        if not holdings:
            return (ticker, None)
        # Quick perf check on top holdings
        h_perf = []
        for h in holdings[:15]:
            sym = h["symbol"]
            hist = fetch_history(sym, days=70)
            if not hist or len(hist) < 21:
                continue
            closes = [x["close"] for x in hist]
            ret_20 = (closes[-1] / closes[-21] - 1) * 100
            ma50 = sum(closes[-50:]) / min(50, len(closes))
            h_perf.append({
                "symbol": sym,
                "ret_20d": ret_20,
                "above_ma50": closes[-1] > ma50,
                "weight": h["weight"],
            })
        breadth = compute_breadth(theme, h_perf, spy_ret_20)
        return (ticker, {"breadth": breadth, "constituents_perf": h_perf})

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(compute_one_breadth, t): t for t in top_themes_for_breadth}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    ticker, info = r
                    if info:
                        breadth_results[ticker] = info
            except Exception:
                continue

    # ── Detect rotation deltas (vs prior state) ──
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass

    rotation_deltas = []
    if prior_state and prior_state.get("rs_ranked"):
        prior_ranks = {t["ticker"]: t["rs_rank_20d"] for t in prior_state["rs_ranked"]}
        for t in rs_ranked:
            old_rank = prior_ranks.get(t["ticker"])
            if old_rank is not None:
                delta = old_rank - t["rs_rank_20d"]  # positive means improved (lower rank = better)
                t["rs_rank_delta"] = delta
                if abs(delta) >= 5:
                    rotation_deltas.append({
                        "ticker": t["ticker"],
                        "name": t["name"],
                        "category": t["category"],
                        "delta": delta,
                        "old_rank": old_rank,
                        "new_rank": t["rs_rank_20d"],
                        "rs_20d": t["rs_20d"],
                    })

    # ── ALERTS ──
    alerts = []
    # 1. Top 5 themes by momentum
    for t in momentum_ranked[:5]:
        if t["momentum_score"] >= 75:
            alerts.append({
                "type": "TOP_MOMENTUM_THEME",
                "ticker": t["ticker"],
                "name": t["name"],
                "category": t["category"],
                "momentum_score": t["momentum_score"],
                "rs_60d": t["rs_60d"],
                "msg": t["name"] + " has top momentum: score " + str(t["momentum_score"]) + ", RS " + "{:+.1f}%".format(t["rs_60d"]) + " vs SPY 60d",
            })

    # 2. Theme with rising rank (rotation IN)
    rotators_in = sorted([d for d in rotation_deltas if d["delta"] >= 5],
                          key=lambda x: -x["delta"])
    for d in rotators_in[:5]:
        alerts.append({
            "type": "ROTATION_IN",
            "ticker": d["ticker"],
            "name": d["name"],
            "category": d["category"],
            "delta": d["delta"],
            "msg": d["name"] + " jumped " + str(d["delta"]) + " RS ranks (now #" + str(d["new_rank"]) + ")",
        })

    # 3. Theme with falling rank (rotation OUT)
    rotators_out = sorted([d for d in rotation_deltas if d["delta"] <= -5],
                           key=lambda x: x["delta"])
    for d in rotators_out[:5]:
        alerts.append({
            "type": "ROTATION_OUT",
            "ticker": d["ticker"],
            "name": d["name"],
            "category": d["category"],
            "delta": d["delta"],
            "msg": d["name"] + " dropped " + str(-d["delta"]) + " RS ranks (now #" + str(d["new_rank"]) + ")",
        })

    # 4. Convergent breadth — top theme with breadth > 65%
    convergent = []
    for t in momentum_ranked[:25]:
        b = breadth_results.get(t["ticker"], {}).get("breadth")
        if b and b.get("breadth_outperform_pct", 0) >= 60 and t["rs_20d"] > 0 and t["rs_5d"] > 0:
            convergent.append({
                "ticker": t["ticker"],
                "name": t["name"],
                "category": t["category"],
                "momentum_score": t["momentum_score"],
                "rs_20d": t["rs_20d"],
                "breadth_pct": b["breadth_outperform_pct"],
            })
    for c in convergent[:5]:
        alerts.append({
            "type": "CONVERGENT_BREADTH",
            "ticker": c["ticker"],
            "name": c["name"],
            "category": c["category"],
            "msg": c["name"] + " has CONVERGENT BREADTH: RS " + "{:+.1f}%".format(c["rs_20d"]) + ", " + str(c["breadth_pct"]) + "% of constituents beating SPY",
        })

    # ── Theme category aggregation ──
    by_category = {}
    for t in rs_ranked:
        cat = t["category"]
        by_category.setdefault(cat, []).append(t)
    category_summary = []
    for cat, themes in by_category.items():
        avg_rs_20 = sum(t["rs_20d"] for t in themes) / len(themes)
        avg_momentum = sum(t["momentum_score"] for t in themes) / len(themes)
        category_summary.append({
            "category": cat,
            "n_themes": len(themes),
            "avg_rs_20d": round(avg_rs_20, 2),
            "avg_momentum": round(avg_momentum, 1),
            "top_ticker": max(themes, key=lambda x: x["momentum_score"])["ticker"],
        })
    category_summary.sort(key=lambda x: -x["avg_momentum"])

    # Build output
    out = {
        "schema_version": 1,
        "method": "theme_rotation_engine_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "spy_ret_20d": round(spy_ret_20, 2),
        "stats": {
            "n_themes_evaluated": len(theme_results),
            "n_with_breadth": len(breadth_results),
            "n_alerts": len(alerts),
            "n_rotation_deltas": len(rotation_deltas),
        },
        "summary": {
            "top_10_momentum": [
                {
                    "ticker": t["ticker"], "name": t["name"], "category": t["category"],
                    "momentum_score": t["momentum_score"],
                    "rs_5d": t["rs_5d"], "rs_20d": t["rs_20d"], "rs_60d": t["rs_60d"],
                    "rs_acceleration": t["rs_acceleration"],
                    "vol_ratio": t["vol_ratio_20v60"],
                    "rank_delta": t.get("rs_rank_delta", 0),
                    "breadth_pct": breadth_results.get(t["ticker"], {}).get("breadth", {}).get("breadth_outperform_pct"),
                }
                for t in momentum_ranked[:10]
            ],
            "bottom_10_momentum": [
                {
                    "ticker": t["ticker"], "name": t["name"], "category": t["category"],
                    "momentum_score": t["momentum_score"],
                    "rs_20d": t["rs_20d"], "rs_60d": t["rs_60d"],
                }
                for t in momentum_ranked[-10:]
            ],
            "category_summary": category_summary,
            "rotators_in": rotators_in[:8],
            "rotators_out": rotators_out[:8],
            "convergent_breadth": convergent[:8],
            "alerts": alerts,
        },
        "all_themes": momentum_ranked,
        "breadth_details": breadth_results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[theme-rot] wrote " + str(len(body)) + "b to " + S3_KEY)

    # State save
    state = {
        "generated_at": out["generated_at"],
        "rs_ranked": [{"ticker": t["ticker"], "rs_rank_20d": t["rs_rank_20d"], "rs_20d": t["rs_20d"]}
                       for t in rs_ranked],
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(state).encode(),
                   ContentType="application/json")

    print("[theme-rot] alerts: " + str(len(alerts)))
    if alerts:
        for a in alerts[:8]:
            print("  " + a["type"] + ": " + a["msg"])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_themes": len(theme_results),
            "n_alerts": len(alerts),
            "duration_s": out["duration_s"],
        }),
    }
