"""
justhodl-theme-detector
=======================
Auto-detects active themes in the market via thematic ETF lifecycle analysis.

The universe of thematic ETFs is the market's own self-classification of
investable themes. For each ETF in our curated universe (~70 ETFs covering
AI, semis, energy, materials, defense, biotech, fintech, geographic, crypto,
real estate, and commodity themes), we compute:

  • Multi-window returns: 5d, 30d, 90d, 180d, 365d
  • Relative strength vs SPY: same windows
  • Realized volatility percentile (90d vs 1y history)
  • Breadth: % of top 10 holdings positive 30d
  • Phase scores across 7 lifecycle phases

The 7 lifecycle phases are scored heuristically and the max-scoring phase
is the classification. Output JSON includes every theme's full numeric
profile + phase classification + a precomputed list of "best for tier-2
hunting" themes (the EXTENDED ones).

Schedule: cron(0 6 * * ? *) — daily 06:00 UTC
Output:   s3://justhodl-dashboard-live/data/themes-detected.json

The downstream Lambdas (theme-tier-classifier, asymmetric-hunter,
nobrainer-rationale) consume this output to find tier-2/3 nobrainer
candidates inside EXTENDED themes.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
POLYGON_BASE = "https://api.polygon.io"

S3 = boto3.client("s3", region_name=REGION)

# ─────────────────────────────────────────────────────────────────────────────
# THEMATIC ETF UNIVERSE — the self-classification of investable themes
# ─────────────────────────────────────────────────────────────────────────────
# Each entry: ETF ticker → name, category, top 10 holdings (for breadth calc)
# Curated to cover the full investable theme space. New ETFs auto-add by
# appending here.
THEME_ETFS = {
    # ── AI / TECH ───────────────────────────────────────────────────
    "SMH":  {"name": "Semiconductors",        "category": "tech_semis",       "top_holdings": ["NVDA","AVGO","TSM","AMD","QCOM","ASML","AMAT","LRCX","INTC","MU"]},
    "SOXX": {"name": "PHLX Semiconductor",    "category": "tech_semis",       "top_holdings": ["NVDA","AVGO","AMD","QCOM","AMAT","LRCX","TSM","ASML","INTC","TXN"]},
    "BOTZ": {"name": "Robotics & AI",         "category": "tech_ai",          "top_holdings": ["NVDA","ABB","ISRG","KEYS","FANUY","DT","UPS","MELI","ROK","ANSS"]},
    "AIQ":  {"name": "AI & Tech",             "category": "tech_ai",          "top_holdings": ["GOOGL","META","NVDA","AAPL","MSFT","AMZN","TSLA","AVGO","ORCL","CRM"]},
    "ROBO": {"name": "Robotics ETF",          "category": "tech_robotics",    "top_holdings": ["DT","ISRG","KEYS","ABB","FANUY","ROK","AIN","DLR","OMC","ANSS"]},
    "IGV":  {"name": "Software",              "category": "tech_software",    "top_holdings": ["MSFT","ORCL","ADBE","CRM","NOW","INTU","PANW","PLTR","CDNS","SNPS"]},
    "CLOU": {"name": "Cloud Computing",       "category": "tech_cloud",       "top_holdings": ["MDB","ZS","CRWD","DDOG","SNOW","PANW","ORCL","ANET","VRSN","OKTA"]},
    "CIBR": {"name": "Cybersecurity",         "category": "tech_security",    "top_holdings": ["CRWD","PANW","FTNT","CSCO","ANET","OKTA","NET","S","AKAM","RPD"]},
    "FDN":  {"name": "Internet",              "category": "tech_internet",    "top_holdings": ["META","GOOGL","AMZN","NFLX","CRM","BKNG","EBAY","PYPL","ABNB","UBER"]},
    "ARKK": {"name": "Innovation",            "category": "tech_innovation",  "top_holdings": ["TSLA","ROKU","PLTR","COIN","RBLX","U","DKNG","SHOP","PATH","RXRX"]},
    "WCLD": {"name": "Cloud Software",        "category": "tech_cloud",       "top_holdings": ["DDOG","CRWD","ZS","NET","MDB","SNOW","HUBS","TEAM","BILL","NOW"]},
    "FINX": {"name": "FinTech",               "category": "tech_fintech",     "top_holdings": ["V","MA","PYPL","SQ","INTU","FIS","FISV","COIN","SOFI","HOOD"]},
    # ── ENERGY ──────────────────────────────────────────────────────
    "XLE":  {"name": "Energy",                "category": "energy_broad",     "top_holdings": ["XOM","CVX","EOG","COP","SLB","MPC","PSX","VLO","OXY","FANG"]},
    "XOP":  {"name": "Oil & Gas E&P",         "category": "energy_ep",        "top_holdings": ["MPC","VLO","PSX","EOG","FANG","DVN","OXY","OVV","MRO","APA"]},
    "OIH":  {"name": "Oil Services",          "category": "energy_services",  "top_holdings": ["SLB","BKR","HAL","FTI","CHX","NOV","PUMP","RES","WTTR","NESR"]},
    "URA":  {"name": "Uranium",               "category": "energy_uranium",   "top_holdings": ["CCJ","BHP","DNN","URG","UEC","NXE","EU","UUUU","DML","BKY"]},
    "URNM": {"name": "Uranium Miners",        "category": "energy_uranium",   "top_holdings": ["CCJ","DNN","URG","UEC","NXE","EU","UUUU","DML","BKY","SXTA"]},
    "ICLN": {"name": "Clean Energy",          "category": "energy_clean",     "top_holdings": ["FSLR","ENPH","PCG","WBD","VST","BEPC","BEP","PLUG","RUN","SEDG"]},
    "TAN":  {"name": "Solar",                 "category": "energy_solar",     "top_holdings": ["FSLR","ENPH","RUN","NXT","SHLS","SEDG","ARRY","MAXN","CSIQ","JKS"]},
    "FAN":  {"name": "Wind Energy",           "category": "energy_wind",      "top_holdings": ["NEE","ED","BEP","BEPC","RWE","EDP","TPIC","DNNGY","VWS","ORSTED"]},
    "NLR":  {"name": "Nuclear",               "category": "energy_nuclear",   "top_holdings": ["VST","TLN","CEG","PCG","BWXT","CCJ","NEE","DUK","CMS","EXC"]},
    "AMLP": {"name": "MLPs / Pipelines",      "category": "energy_pipelines", "top_holdings": ["EPD","ET","MPLX","WES","PAA","ENB","TRGP","KMI","WMB","OKE"]},
    # ── MATERIALS / COMMODITIES MINERS ──────────────────────────────
    "GDX":  {"name": "Gold Miners",           "category": "materials_gold",   "top_holdings": ["NEM","GFI","KGC","AEM","WPM","AU","GOLD","FNV","AGI","BTG"]},
    "GDXJ": {"name": "Junior Gold Miners",    "category": "materials_gold",   "top_holdings": ["KGC","AGI","PAAS","BTG","EQX","EGO","OR","SSRM","NGD","SAND"]},
    "COPX": {"name": "Copper Miners",         "category": "materials_copper", "top_holdings": ["FCX","SCCO","BHP","RIO","TECK","HBM","ERO","TGB","FM","TRQ"]},
    "REMX": {"name": "Rare Earth Metals",     "category": "materials_rare",   "top_holdings": ["MP","TROX","SQM","ALB","LYC","CSTM","USAR","TMR","NDA","REEMF"]},
    "LIT":  {"name": "Lithium & Battery",     "category": "materials_lithium","top_holdings": ["TSLA","ALB","SQM","BYDDF","PLL","LTHM","MP","RIVN","SLI","LAC"]},
    "SLX":  {"name": "Steel",                 "category": "materials_steel",  "top_holdings": ["NUE","VALE","CLF","STLD","X","MT","RIO","TX","TS","ATI"]},
    "WOOD": {"name": "Lumber & Forestry",     "category": "materials_lumber", "top_holdings": ["WY","RYN","PCH","LPX","BLD","IFF","WRK","PKG","CCL","AVY"]},
    "PICK": {"name": "Mining (broad)",        "category": "materials_mining", "top_holdings": ["BHP","RIO","FCX","VALE","SCCO","NEM","TECK","AAUKF","FMG","FCX"]},
    "SIL":  {"name": "Silver Miners",         "category": "materials_silver", "top_holdings": ["PAAS","WPM","HL","CDE","FRES","FSM","AG","SVM","EXK","HOC.L"]},
    # ── INDUSTRIALS / DEFENSE / INFRA ──────────────────────────────
    "XLI":  {"name": "Industrials",           "category": "industrials_broad","top_holdings": ["GE","RTX","CAT","HON","BA","DE","UPS","ETN","LMT","UNP"]},
    "ITA":  {"name": "Defense & Aerospace",   "category": "industrials_def",  "top_holdings": ["RTX","BA","LMT","GE","GD","NOC","TDG","TXT","HII","LDOS"]},
    "PPA":  {"name": "Aerospace & Defense",   "category": "industrials_def",  "top_holdings": ["RTX","BA","LMT","GE","GD","NOC","TDG","TXT","HII","LDOS"]},
    "PAVE": {"name": "Infrastructure",        "category": "industrials_infra","top_holdings": ["URI","PWR","CSX","TRGP","MLM","NSC","VMC","ETN","PCAR","EME"]},
    "GRID": {"name": "Smart Grid",            "category": "industrials_grid", "top_holdings": ["ETN","JCI","ABB","ROK","EMR","AME","HUBB","FERG","NEE","SCHN.PA"]},
    "JETS": {"name": "Airlines",              "category": "industrials_air",  "top_holdings": ["LUV","DAL","UAL","AAL","ALK","JBLU","RYAAY","BA","RTX","GE"]},
    "IYT":  {"name": "Transports",            "category": "industrials_trans","top_holdings": ["UPS","UNP","CSX","NSC","ODFL","JBHT","FDX","CHRW","EXPD","KEX"]},
    "XHB":  {"name": "Homebuilders",          "category": "industrials_hb",   "top_holdings": ["DHI","LEN","PHM","NVR","TOL","MTH","TPH","KBH","MHO","LGIH"]},
    "AIRR": {"name": "Industrial Renaissance","category": "industrials_resh", "top_holdings": ["TXN","WAB","ROK","EME","CNH","MLM","URI","VMC","NSC","GLW"]},
    # ── HEALTHCARE ──────────────────────────────────────────────────
    "XLV":  {"name": "Healthcare",            "category": "health_broad",     "top_holdings": ["LLY","UNH","JNJ","ABBV","MRK","TMO","PFE","ABT","DHR","AMGN"]},
    "XBI":  {"name": "Biotech (equal-wt)",    "category": "health_biotech",   "top_holdings": ["VRTX","REGN","GILD","BIIB","MRNA","ALNY","ILMN","SRPT","INCY","BMRN"]},
    "IBB":  {"name": "Biotech (cap-wt)",      "category": "health_biotech",   "top_holdings": ["VRTX","REGN","GILD","AMGN","BIIB","MRNA","ILMN","ALNY","INCY","BMRN"]},
    "IHI":  {"name": "Medical Devices",       "category": "health_devices",   "top_holdings": ["ABT","TMO","MDT","ISRG","BSX","SYK","EW","ZBH","DXCM","BAX"]},
    "GNOM": {"name": "Genomics",              "category": "health_genomics",  "top_holdings": ["ILMN","VRTX","REGN","MRNA","PACB","TWST","VEEV","CRSP","BEAM","NTLA"]},
    "PJP":  {"name": "Pharma (US)",           "category": "health_pharma",    "top_holdings": ["LLY","JNJ","ABBV","MRK","PFE","BMY","TMO","AMGN","GILD","ZTS"]},
    # ── FINANCIALS ──────────────────────────────────────────────────
    "XLF":  {"name": "Financials",            "category": "fin_broad",        "top_holdings": ["BRK.B","JPM","V","BAC","MA","WFC","GS","MS","AXP","BLK"]},
    "KRE":  {"name": "Regional Banks",        "category": "fin_regional",     "top_holdings": ["WAL","EWBC","CMA","ZION","RF","MTB","PNC","CFG","FITB","USB"]},
    "KBE":  {"name": "Banks (broad)",         "category": "fin_banks",        "top_holdings": ["JPM","BAC","WFC","C","USB","PNC","TFC","GS","MS","BK"]},
    "KIE":  {"name": "Insurance",             "category": "fin_insurance",    "top_holdings": ["BRK.B","PGR","CB","TRV","AIG","ALL","MET","PRU","AFL","HIG"]},
    "IAI":  {"name": "Broker-Dealers",        "category": "fin_brokers",      "top_holdings": ["GS","MS","SCHW","BLK","CME","ICE","SPGI","MCO","MSCI","NDAQ"]},
    # ── CONSUMER ────────────────────────────────────────────────────
    "XLY":  {"name": "Consumer Discretionary","category": "consumer_disc",    "top_holdings": ["AMZN","TSLA","HD","MCD","BKNG","LOW","NKE","SBUX","TJX","ABNB"]},
    "XLP":  {"name": "Consumer Staples",      "category": "consumer_staples", "top_holdings": ["WMT","PG","COST","KO","PEP","PM","MO","MDLZ","CL","TGT"]},
    "XRT":  {"name": "Retail",                "category": "consumer_retail",  "top_holdings": ["AMZN","WMT","COST","HD","TGT","TJX","LOW","BBY","ROST","DG"]},
    "IBUY": {"name": "Online Retail",         "category": "consumer_online",  "top_holdings": ["AMZN","EBAY","PYPL","SHOP","JD","MELI","BABA","ETSY","CHWY","BKNG"]},
    # ── REAL ESTATE ────────────────────────────────────────────────
    "XLRE": {"name": "Real Estate",           "category": "re_broad",         "top_holdings": ["PLD","AMT","EQIX","CCI","PSA","WELL","DLR","O","SPG","AVB"]},
    "REZ":  {"name": "Residential REITs",     "category": "re_resid",         "top_holdings": ["AVB","EQR","ESS","MAA","UDR","INVH","CPT","AMH","CSR","SUI"]},
    "REM":  {"name": "Mortgage REITs",        "category": "re_mortgage",      "top_holdings": ["AGNC","STWD","RWT","NLY","BXMT","ARI","RC","MFA","TWO","PMT"]},
    "INDS": {"name": "Industrial REITs",      "category": "re_industrial",    "top_holdings": ["PLD","EQIX","DLR","EXR","CCI","AMT","CUBE","WPC","REXR","STAG"]},
    "ICF":  {"name": "Cohen & Steers Realty", "category": "re_diversified",   "top_holdings": ["AMT","PLD","CCI","EQIX","WELL","DLR","PSA","O","SPG","EQR"]},
    # ── COMMODITIES ────────────────────────────────────────────────
    "GLD":  {"name": "Gold (physical)",       "category": "commodity_gold",   "top_holdings": ["GLD"]},
    "SLV":  {"name": "Silver (physical)",     "category": "commodity_silver", "top_holdings": ["SLV"]},
    "USO":  {"name": "Oil",                   "category": "commodity_oil",    "top_holdings": ["USO"]},
    "UNG":  {"name": "Natural Gas",           "category": "commodity_natgas", "top_holdings": ["UNG"]},
    "DBB":  {"name": "Base Metals",           "category": "commodity_base",   "top_holdings": ["DBB"]},
    "DBA":  {"name": "Agriculture",           "category": "commodity_ag",     "top_holdings": ["DBA"]},
    "BCI":  {"name": "Broad Commodities",     "category": "commodity_broad",  "top_holdings": ["BCI"]},
    # ── CRYPTO ─────────────────────────────────────────────────────
    "BITO": {"name": "Bitcoin Strategy",      "category": "crypto_btc",       "top_holdings": ["BITO"]},
    "ETHE": {"name": "Ethereum",              "category": "crypto_eth",       "top_holdings": ["ETHE"]},
    "BLOK": {"name": "Blockchain",            "category": "crypto_blockchain","top_holdings": ["MSTR","COIN","MARA","RIOT","NVDA","PYPL","SQ","IBM","BITF","CIFR"]},
    "BITQ": {"name": "Crypto Industry",       "category": "crypto_industry",  "top_holdings": ["MSTR","COIN","MARA","RIOT","HUT","IREN","CIFR","BTBT","BITF","CLSK"]},
    # ── GEOGRAPHIC / EM ────────────────────────────────────────────
    "FXI":  {"name": "China Large Cap",       "category": "intl_china",       "top_holdings": ["BABA","TCEHY","JD","PDD","BIDU","NIO","LI","XPEV","TME","BILI"]},
    "KWEB": {"name": "China Internet",        "category": "intl_china",       "top_holdings": ["BABA","TCEHY","JD","PDD","BIDU","NTES","TME","TAL","BILI","BABA"]},
    "EWZ":  {"name": "Brazil",                "category": "intl_brazil",      "top_holdings": ["VALE","ITUB","PBR","BBD","ABEV","BSBR","BAK","SBS","CIG","CSAN"]},
    "INDA": {"name": "India",                 "category": "intl_india",       "top_holdings": ["INFY","HDB","IBN","WIT","SIFY","RDY","TTM","SLB","VEDL","AZRE"]},
    "EWJ":  {"name": "Japan",                 "category": "intl_japan",       "top_holdings": ["TM","SONY","MUFG","SMFG","HMC","TAK","MFG","NMR","NTTYY","TKAYY"]},
    "EEM":  {"name": "Emerging Markets",      "category": "intl_em",          "top_holdings": ["TSM","BABA","TCEHY","JD","INFY","MELI","ICICI","RELIANCE","HDB","WIT"]},
    "EWG":  {"name": "Germany",               "category": "intl_germany",     "top_holdings": ["SAP","SIEGY","ALIZY","BASFY","BAYRY","DTEGY","BMWYY","VLKAY","DDAIF","ADDYY"]},
    "EWU":  {"name": "United Kingdom",        "category": "intl_uk",          "top_holdings": ["AZN","SHEL","HSBC","UL","DEO","RIO","BP","BTI","GSK","BCS"]},
}

SPY_TICKER = "SPY"
LOOKBACK_DAYS = 400


# ─────────────────────────────────────────────────────────────────────────────
# POLYGON ADAPTER
# ─────────────────────────────────────────────────────────────────────────────
def fetch_polygon_aggs(ticker, end_dt=None):
    """Fetch ~LOOKBACK_DAYS daily aggs for ticker. Returns list of {t, c, v}."""
    if end_dt is None:
        end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS + 30)
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start_dt.strftime('%Y-%m-%d')}/{end_dt.strftime('%Y-%m-%d')}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    last_err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodlAI/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                data = json.loads(raw)
                # Polygon returns status="OK" for fresh data, "DELAYED" for free-tier.
                # Accept anything with a results array.
                results = data.get("results") or []
                if results:
                    return [{"t": r["t"], "c": r["c"], "v": r.get("v", 0)} for r in results]
                # No results — log why
                status = data.get("status")
                err_msg = data.get("error") or data.get("message")
                count = data.get("resultsCount", 0)
                if attempt == 0:
                    print(f"[poly] {ticker} no_results status={status} count={count} err={err_msg}")
                return []
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            try:
                err_body = e.read().decode()[:200]
            except Exception:
                err_body = "<unreadable>"
            if e.code == 429 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            print(f"[poly] {ticker} HTTP {e.code} attempt={attempt}: {err_body}")
            return []
        except urllib.error.URLError as e:
            last_err = f"URLError {e}"
            if attempt < 2:
                time.sleep(1)
                continue
            print(f"[poly] {ticker} URLError attempt={attempt}: {e}")
            return []
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < 2:
                time.sleep(1)
                continue
            print(f"[poly] {ticker} error attempt={attempt}: {type(e).__name__}: {e}")
            return []
    if last_err:
        print(f"[poly] {ticker} all attempts failed last_err={last_err}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────────────────────────────────────
def returns_at(closes_with_t, lookback_days):
    """Return % over lookback_days. closes_with_t is sorted asc list of {t, c}."""
    if not closes_with_t or len(closes_with_t) < 2:
        return None
    last = closes_with_t[-1]
    target_ms = last["t"] - lookback_days * 86400000
    # Find closest bar at-or-before target_ms
    prior = None
    for bar in closes_with_t:
        if bar["t"] <= target_ms:
            prior = bar
        else:
            break
    if not prior or prior["c"] <= 0:
        # Use first bar if we don't have lookback range
        prior = closes_with_t[0]
    if prior["c"] <= 0:
        return None
    return (last["c"] / prior["c"] - 1) * 100


def realized_vol_pct(closes_with_t, window_days=90, history_days=365):
    """Compute current realized vol vs its 1y history. Returns 0-100 percentile."""
    if len(closes_with_t) < window_days + 30:
        return None
    closes = [b["c"] for b in closes_with_t]
    # Rolling realized vol
    vols = []
    for i in range(window_days, len(closes) - 1):
        log_returns = []
        for j in range(i - window_days, i):
            if closes[j] > 0 and closes[j + 1] > 0:
                log_returns.append((closes[j + 1] / closes[j]) - 1)
        if not log_returns:
            continue
        m = sum(log_returns) / len(log_returns)
        var = sum((r - m) ** 2 for r in log_returns) / max(1, len(log_returns) - 1)
        vols.append(var ** 0.5)
    if not vols:
        return None
    current = vols[-1]
    sorted_vols = sorted(vols[-history_days:])
    if not sorted_vols:
        return None
    rank = sum(1 for v in sorted_vols if v < current)
    return round(rank / len(sorted_vols) * 100, 1)


def breadth_30d(top_holdings, holding_data):
    """% of top 10 holdings positive over 30d."""
    pos = 0
    n = 0
    for ticker in top_holdings:
        if ticker == "" or ticker is None:
            continue
        bars = holding_data.get(ticker)
        if not bars:
            continue
        ret30 = returns_at(bars, 30)
        if ret30 is None:
            continue
        n += 1
        if ret30 > 0:
            pos += 1
    if n == 0:
        return None
    return round(pos / n, 3)


# ─────────────────────────────────────────────────────────────────────────────
# 7-PHASE LIFECYCLE SCORING
# ─────────────────────────────────────────────────────────────────────────────
def safe(x, fallback=0):
    return x if x is not None else fallback


def score_phases(metrics):
    """Return dict of {phase: 0-100 score}. Max score wins."""
    r5  = safe(metrics.get("ret_5d"))
    r30 = safe(metrics.get("ret_30d"))
    r90 = safe(metrics.get("ret_90d"))
    r180= safe(metrics.get("ret_180d"))
    r365= safe(metrics.get("ret_365d"))
    rs30= safe(metrics.get("rs_30d"))
    rs90= safe(metrics.get("rs_90d"))
    rs180=safe(metrics.get("rs_180d"))
    vol = safe(metrics.get("vol_pct_90d"), 50)
    br  = safe(metrics.get("breadth_30d"), 0.5)

    scores = {}

    # DORMANT: flat returns, low vol, no momentum
    s = 0
    if abs(r180) < 5:                s += 30
    if abs(r30) < 3:                 s += 30
    if vol < 30:                     s += 25
    if 0.4 <= br <= 0.6:             s += 15
    scores["DORMANT"] = min(100, s)

    # EMERGING: recent momentum but not yet extended
    s = 0
    if 5 < r30 < 25:                 s += 35
    if rs30 > 3:                     s += 25
    if -5 < r180 < 25:               s += 20
    if br > 0.55:                    s += 10
    if 5 < r90 < 25:                 s += 10
    scores["EMERGING"] = min(100, s)

    # ACCELERATING: strong, broad, vol expanding
    s = 0
    if r30 > 8:                      s += 25
    if r90 > 15:                     s += 25
    if 25 < r180 < 60:               s += 20
    if rs30 > 5 and rs90 > 8:        s += 20
    if br > 0.65:                    s += 10
    scores["ACCELERATING"] = min(100, s)

    # EXTENDED: long, hard run; market believes; expensive (← TIER-2 HUNT GROUND)
    s = 0
    if r180 > 35:                    s += 30
    if r365 > 50:                    s += 25
    if rs180 > 20:                   s += 20
    if vol > 60:                     s += 15
    if r90 > 15:                     s += 10
    scores["EXTENDED"] = min(100, s)

    # PEAKING: 6m+ strong but recent flat/down, breadth deteriorating
    s = 0
    if r180 > 30:                    s += 25
    if -5 < r30 < 3:                 s += 25
    if r180 > 0 and r90 < r180 * 0.4: s += 20
    if br < 0.55:                    s += 20
    if vol > 60:                     s += 10
    scores["PEAKING"] = min(100, s)

    # COOLING: rolling over
    s = 0
    if r30 < -3:                     s += 30
    if r90 < 0:                      s += 25
    if -15 < r180 < 5:               s += 20
    if rs30 < -2:                    s += 15
    if br < 0.45:                    s += 10
    scores["COOLING"] = min(100, s)

    # DYING: persistent weakness; market exiting
    s = 0
    if r180 < -10:                   s += 30
    if r365 < -5:                    s += 25
    if rs180 < -10:                  s += 20
    if br < 0.35:                    s += 15
    if r90 < -5:                     s += 10
    scores["DYING"] = min(100, s)

    return scores


def classify(scores):
    """Return (winning_phase, top_score, confidence_label)."""
    sorted_phases = sorted(scores.items(), key=lambda x: -x[1])
    top, top_score = sorted_phases[0]
    second_score = sorted_phases[1][1] if len(sorted_phases) > 1 else 0
    margin = top_score - second_score
    if top_score < 30:
        confidence = "low"
    elif margin >= 25 and top_score >= 60:
        confidence = "high"
    elif margin >= 15 or top_score >= 70:
        confidence = "medium"
    else:
        confidence = "low"
    return top, top_score, confidence


# ─────────────────────────────────────────────────────────────────────────────
# INTERPRETATION (human-friendly summary line)
# ─────────────────────────────────────────────────────────────────────────────
def interpret(etf, name, phase, metrics, top_holdings):
    r30 = metrics.get("ret_30d")
    r90 = metrics.get("ret_90d")
    r180= metrics.get("ret_180d")
    r365= metrics.get("ret_365d")
    rs180=metrics.get("rs_180d")
    vol = metrics.get("vol_pct_90d")
    br  = metrics.get("breadth_30d")
    fmt = lambda x, sfx="": f"{x:+.1f}{sfx}" if x is not None else "n/a"

    phrases = {
        "DORMANT":     f"{name} is asleep. {fmt(r180,'%')} over 6m, vol pct {fmt(vol)}. Wait for catalyst.",
        "EMERGING":    f"{name} is waking up. {fmt(r30,'%')} 30d ({fmt(metrics.get('rs_30d'),'%')} vs SPY), 6m still tame at {fmt(r180,'%')}. EARLY ENTRY ZONE.",
        "ACCELERATING":f"{name} is running. {fmt(r30,'%')} 30d, {fmt(r90,'%')} 90d, breadth {fmt((br or 0)*100)}%. Theme is being recognized.",
        "EXTENDED":    f"{name} has run hard: {fmt(r180,'%')} 6m, {fmt(r365,'%')} 12m, vol pct {fmt(vol)}. TIER-2 HUNT GROUND — laggards inside this theme are the trade.",
        "PEAKING":     f"{name} losing steam. 6m up {fmt(r180,'%')} but 30d only {fmt(r30,'%')}, breadth {fmt((br or 0)*100)}%. Watch for distribution.",
        "COOLING":     f"{name} rolling over. {fmt(r30,'%')} 30d, {fmt(r90,'%')} 90d. Demand caution on tier-1.",
        "DYING":       f"{name} dying. {fmt(r180,'%')} 6m, {fmt(r365,'%')} 12m, breadth {fmt((br or 0)*100)}%. Avoid OR consider short candidates.",
    }
    top1 = top_holdings[0] if top_holdings else None
    base = phrases.get(phase, "")
    if phase in ("EXTENDED", "ACCELERATING") and top1:
        base += f" Tier-1 leader: {top1}."
    return base


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)

    # 1. Build full ticker fetch list (all ETFs + SPY + all top holdings, deduped)
    all_tickers = set([SPY_TICKER])
    for etf, meta in THEME_ETFS.items():
        all_tickers.add(etf)
        for h in meta.get("top_holdings", []):
            if h and not any(c in h for c in [".", " "]):  # skip non-US tickers we can't fetch
                all_tickers.add(h)
    all_tickers = sorted(all_tickers)
    print(f"[theme-detector] fetching {len(all_tickers)} tickers from Polygon")

    # 2. Parallel fetch (12 concurrent)
    bars_by_ticker = {}
    n_ok = 0
    n_fail = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fetch_polygon_aggs, t): t for t in all_tickers}
        for f in as_completed(futs):
            t = futs[f]
            try:
                bars = f.result()
                bars_by_ticker[t] = bars
                if bars and len(bars) >= 30:
                    n_ok += 1
                else:
                    n_fail += 1
            except Exception as e:
                bars_by_ticker[t] = []
                n_fail += 1
                print(f"[poly] {t} failed: {e}")
    fetch_dur = time.time() - started
    print(f"[theme-detector] fetched {n_ok} ok / {n_fail} failed in {fetch_dur:.1f}s")

    # 3. Compute SPY benchmark returns once
    spy_bars = bars_by_ticker.get(SPY_TICKER, [])
    spy_returns = {}
    for w in [5, 30, 90, 180, 365]:
        spy_returns[w] = returns_at(spy_bars, w) or 0

    # 4. Score every theme
    themes = []
    for etf, meta in THEME_ETFS.items():
        bars = bars_by_ticker.get(etf, [])
        if not bars or len(bars) < 30:
            print(f"[theme-detector] skipping {etf} — insufficient data ({len(bars)} bars)")
            continue
        ret_5d   = returns_at(bars, 5)
        ret_30d  = returns_at(bars, 30)
        ret_90d  = returns_at(bars, 90)
        ret_180d = returns_at(bars, 180)
        ret_365d = returns_at(bars, 365)
        vol_pct  = realized_vol_pct(bars)

        # Relative strength (theme - SPY for same window)
        rs = {}
        for w, r in [(5, ret_5d), (30, ret_30d), (90, ret_90d), (180, ret_180d), (365, ret_365d)]:
            if r is not None:
                rs[w] = r - spy_returns.get(w, 0)
            else:
                rs[w] = None

        # Breadth (top holdings)
        holding_bars = {h: bars_by_ticker.get(h) for h in meta["top_holdings"]}
        br = breadth_30d(meta["top_holdings"], holding_bars)

        metrics = {
            "ret_5d":      round(ret_5d, 2)   if ret_5d   is not None else None,
            "ret_30d":     round(ret_30d, 2)  if ret_30d  is not None else None,
            "ret_90d":     round(ret_90d, 2)  if ret_90d  is not None else None,
            "ret_180d":    round(ret_180d, 2) if ret_180d is not None else None,
            "ret_365d":    round(ret_365d, 2) if ret_365d is not None else None,
            "rs_5d":       round(rs[5], 2)   if rs[5]   is not None else None,
            "rs_30d":      round(rs[30], 2)  if rs[30]  is not None else None,
            "rs_90d":      round(rs[90], 2)  if rs[90]  is not None else None,
            "rs_180d":     round(rs[180], 2) if rs[180] is not None else None,
            "rs_365d":     round(rs[365], 2) if rs[365] is not None else None,
            "vol_pct_90d": vol_pct,
            "breadth_30d": br,
        }

        scores = score_phases(metrics)
        phase, top_score, confidence = classify(scores)
        interpretation = interpret(etf, meta["name"], phase, metrics, meta["top_holdings"])

        themes.append({
            "etf": etf,
            "name": meta["name"],
            "category": meta["category"],
            "phase": phase,
            "phase_score": top_score,
            "phase_confidence": confidence,
            "phase_scores": scores,
            "metrics": metrics,
            "top_holdings": meta["top_holdings"],
            "interpretation": interpretation,
            "n_bars_used": len(bars),
        })

    # 5. Build summary
    by_phase = defaultdict(list)
    for t in themes:
        by_phase[t["phase"]].append(t)

    # Hottest = max(EXTENDED, ACCELERATING) by phase_score
    hottest = sorted(
        [t for t in themes if t["phase"] in ("ACCELERATING", "EXTENDED")],
        key=lambda x: -(x["phase_score"] + 0.5 * (x["metrics"].get("ret_180d") or 0))
    )[:6]

    # Best for tier-2 hunting = EXTENDED only, sorted by phase_score
    tier2_grounds = sorted(
        [t for t in themes if t["phase"] == "EXTENDED"],
        key=lambda x: -x["phase_score"]
    )[:5]

    # Dying = sorted by DYING score
    dying = sorted(
        [t for t in themes if t["phase"] == "DYING"],
        key=lambda x: -x["phase_scores"]["DYING"]
    )[:5]

    # Emerging = sorted by EMERGING score
    emerging = sorted(
        [t for t in themes if t["phase"] == "EMERGING"],
        key=lambda x: -x["phase_score"]
    )[:5]

    summary = {
        "n_themes": len(themes),
        "phase_distribution": {p: len(by_phase[p]) for p in
                               ["DORMANT", "EMERGING", "ACCELERATING", "EXTENDED",
                                "PEAKING", "COOLING", "DYING"]},
        "hottest_themes":         [t["etf"] for t in hottest],
        "best_for_tier2_hunting": [t["etf"] for t in tier2_grounds],
        "dying_themes":           [t["etf"] for t in dying],
        "emerging_themes":        [t["etf"] for t in emerging],
        "spy_returns":            {f"{k}d": round(v, 2) for k, v in spy_returns.items()},
    }

    # 6. Sort themes by composite "interestingness" — EXTENDED at top, then ACCELERATING, etc.
    phase_order = {"EXTENDED": 0, "ACCELERATING": 1, "EMERGING": 2, "PEAKING": 3,
                   "COOLING": 4, "DYING": 5, "DORMANT": 6}
    themes.sort(key=lambda t: (phase_order.get(t["phase"], 9), -t["phase_score"]))

    output = {
        "v": "1.0",
        "generated_at": now.isoformat(),
        "duration_s": round(time.time() - started, 1),
        "method": "thematic_etf_lifecycle_v1",
        "method_description": (
            "Auto-detect theme lifecycle via ~70 thematic ETFs. For each ETF: "
            "compute multi-window returns (5/30/90/180/365d), relative strength "
            "vs SPY, realized vol percentile, and breadth (% of top 10 holdings "
            "positive 30d). Score 7 lifecycle phases (DORMANT/EMERGING/"
            "ACCELERATING/EXTENDED/PEAKING/COOLING/DYING) and classify by max. "
            "EXTENDED phase is the sweet spot for tier-2 hunting — theme is "
            "proven, market is paying for tier-1, but laggards inside the same "
            "ETF haven't been bid."
        ),
        "fetch_stats": {
            "n_tickers": len(all_tickers),
            "n_ok": n_ok,
            "n_fail": n_fail,
            "fetch_duration_s": round(fetch_dur, 1),
        },
        "summary": summary,
        "themes": themes,
    }

    body = json.dumps(output, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET,
        Key="data/themes-detected.json",
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    print(f"[theme-detector] wrote {len(body):,}b to data/themes-detected.json")
    print(f"[theme-detector] phase distribution: {summary['phase_distribution']}")
    print(f"[theme-detector] hottest: {summary['hottest_themes']}")
    print(f"[theme-detector] tier-2 hunt grounds: {summary['best_for_tier2_hunting']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_themes": len(themes),
            "phase_distribution": summary["phase_distribution"],
            "hottest": summary["hottest_themes"],
            "tier2_hunt": summary["best_for_tier2_hunting"],
            "dying": summary["dying_themes"],
            "emerging": summary["emerging_themes"],
            "duration_s": output["duration_s"],
        }),
    }
