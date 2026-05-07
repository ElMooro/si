"""
justhodl-divergence-engine-v2 — Phase 2 enhancement to v1 divergence-scanner.

WHAT THIS ADDS BEYOND V1
────────────────────────
v1 (justhodl-divergence-scanner) covers 12 US-centric cross-asset pairs:
  Gold/real rates, IWM/curve, EEM/dollar, QQQ/yields, banks/curve, VIX/HY,
  BTC/QQQ, GLD/BTC, defensives/cyclicals, energy/breakevens, TIP/IEF, XLV/SPY.

v2 ADDS ~32 new divergence pairs across SEVEN categories that v1 doesn't touch:

  A. LABOR MARKET (5)        — claims, JOLTS, hours, U-6, wage/inflation
  B. MANUFACTURING (5)       — capacity, new orders, capex, IP/PMI
  C. LEADING INDICATORS (3)  — LEI/CEI, expectations vs current, LEI vs curve
  D. EM/FRONTIER GEOGRAPHY (7) — India, Brazil, Mexico, Vietnam, Indonesia,
                                 South Africa, Frontier
  E. MICRO/SMALL CAPS (2)    — IWC vs IWM, IWM vs SPY (already in v1 but
                                 different lens)
  F. COPPER-PRODUCERS (4)    — Chile, Peru, Finland-proxy, copper-miners
  G. EURODOLLAR CENTERS (4)  — HKD peg stress, EU offshore proxy,
                                 Singapore proxy, GBP funding
  H. CROSS-CURRENCY DEPTH (2) — EUR/JPY basis, FX vol regime

Total: 32 new pairs. Combined with v1's 12 = 44 relationships monitored.

DESIGN PRINCIPLES
─────────────────
1. ADDITIVE ONLY. v1 stays untouched. v2 has its own Lambda + S3 path.
2. MIXED FREQUENCY HANDLING. Daily ETFs paired via 60-day rolling z-score.
   Weekly FRED (claims) paired via 13-week rolling z-score. Monthly FRED
   (ISM, LEI) paired via 12-month rolling z-score. Each pair has its
   own appropriate window.
3. DIVERGENCE = z(A) − polarity * z(B). Flagged when |divergence| > 2.0,
   EXTREME when > 3.0 (rare alert event).
4. CATEGORIZED OUTPUT. Each pair tagged by category for the frontend.

INPUT
─────
  FRED API (24 series) — 5 years of daily/weekly/monthly history
  FMP /stable/historical-price-eod (12 ETFs + 4 single names) — 180 days

OUTPUT
──────
  s3://justhodl-dashboard-live/data/divergence-v2.json

SCHEDULE
────────
  rate(2 hours) — same as v1, captures intraday ETF moves while
  weekly/monthly FRED data updates on its natural cadence.

CONSUMED BY
───────────
  divergence.html (combined v1+v2 dashboard)
  morning-intelligence (Telegram brief)
  Future: composite quant score (when monetary regime + divergence both align)
"""
import io
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/divergence-v2.json")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

Z_FLAG_THRESHOLD = 2.0      # flag a divergence
Z_EXTREME_THRESHOLD = 3.0   # Telegram alert (rare)

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# DATA SOURCES — FRED series + ETFs needed across all relationships
# ─────────────────────────────────────────────────────────────────────────────
FRED_SERIES = {
    # Labor (weekly/monthly)
    "ICSA":     {"freq": "weekly",  "title": "Initial Jobless Claims"},
    "CCSA":     {"freq": "weekly",  "title": "Continuing Jobless Claims"},
    "UNRATE":   {"freq": "monthly", "title": "Unemployment Rate"},
    "U6RATE":   {"freq": "monthly", "title": "U-6 Unemployment Rate"},
    "AWHAETP":  {"freq": "monthly", "title": "Avg Weekly Hours All Employees"},
    "JTSQUL":   {"freq": "monthly", "title": "JOLTS Quits Level"},
    "JTSHIL":   {"freq": "monthly", "title": "JOLTS Hires Level"},
    "ECIWAG":   {"freq": "quarterly", "title": "Employment Cost Index Wages"},
    "PCEPI":    {"freq": "monthly", "title": "PCE Inflation Index"},

    # Manufacturing (monthly)
    "INDPRO":   {"freq": "monthly", "title": "Industrial Production Index"},
    "IPMAN":    {"freq": "monthly", "title": "Industrial Production: Manufacturing"},
    "TCU":      {"freq": "monthly", "title": "Capacity Utilization"},
    "NEWORDER": {"freq": "monthly", "title": "Mfg New Orders Capital Goods"},
    "DGORDER":  {"freq": "monthly", "title": "Durable Goods New Orders"},
    "AMTMNO":   {"freq": "monthly", "title": "Mfg New Orders Total"},

    # Leading / Coincident
    "USSLIND":  {"freq": "monthly", "title": "Leading Economic Index (St Louis)"},
    "USPHCI":   {"freq": "monthly", "title": "Coincident Economic Activity Index"},
    "UMCSENT":  {"freq": "monthly", "title": "U Mich Consumer Sentiment"},
    "T10Y2Y":   {"freq": "daily",   "title": "10Y-2Y Treasury Spread"},

    # Cross-currency / Eurodollar centers
    "DEXMXUS":  {"freq": "daily",   "title": "MXN/USD"},
    "DEXBZUS":  {"freq": "daily",   "title": "BRL/USD"},
    "DEXCHUS":  {"freq": "daily",   "title": "CNY/USD"},
    "DEXHKUS":  {"freq": "daily",   "title": "HKD/USD (peg stress)"},
    "DEXSDUS":  {"freq": "daily",   "title": "SGD/USD"},
    "DEXUSEU":  {"freq": "daily",   "title": "USD/EUR"},
    "DEXJPUS":  {"freq": "daily",   "title": "JPY/USD"},
    "DEXUSUK":  {"freq": "daily",   "title": "USD/GBP"},
    "DEXSZUS":  {"freq": "daily",   "title": "CHF/USD (Swiss Franc)"},
    "DEXKOUS":  {"freq": "daily",   "title": "KRW/USD (Korean Won)"},
    "DEXTAUS":  {"freq": "daily",   "title": "TWD/USD (Taiwan)"},

    # Money/funding
    "DGS10":    {"freq": "daily",   "title": "10Y Treasury Yield"},
    "DGS2":     {"freq": "daily",   "title": "2Y Treasury Yield"},
    "DGS3MO":   {"freq": "daily",   "title": "3M Treasury Yield"},
    "DTWEXBGS": {"freq": "daily",   "title": "Broad Dollar Index"},
    "BAA10YM":  {"freq": "monthly", "title": "BAA-10Y Spread (corporate stress)"},
    "AAA10YM":  {"freq": "monthly", "title": "AAA-10Y Spread"},
    "BUSLOANS": {"freq": "weekly",  "title": "Commercial & Industrial Loans"},
    "DRSDCIS":  {"freq": "quarterly", "title": "Credit Card Delinquencies"},
    "DRTSCIS":  {"freq": "quarterly", "title": "All Loans Delinquency Rate"},

    # Commodities
    "PCOPPUSDM": {"freq": "monthly", "title": "Copper Price USD"},
    "PNICKUSDM": {"freq": "monthly", "title": "Nickel Price USD"},
    "DCOILWTICO": {"freq": "daily", "title": "WTI Crude"},
    "DCOILBRENTEU": {"freq": "daily", "title": "Brent Crude"},
    "GOLDPMGBD228NLBM": {"freq": "daily", "title": "Gold AM Fix"},

    # Crisis-leading indicators (the big ones)
    "HTRUCKSSAAR": {"freq": "monthly", "title": "Heavy Truck Sales SAAR (recession leader)"},
    "PERMIT":      {"freq": "monthly", "title": "New Privately-Owned Housing Permits"},
    "HOUST":       {"freq": "monthly", "title": "Housing Starts"},
    "TOTALSA":     {"freq": "monthly", "title": "Total Vehicle Sales"},
    "RSAFS":       {"freq": "monthly", "title": "Retail Sales (Advance)"},
    "RECPROUSM156N": {"freq": "monthly", "title": "NY Fed Recession Probability"},

    # OECD harmonized unemployment (country-specific)
    "LRHUTTTTCHM156S": {"freq": "monthly", "title": "Switzerland Unemployment"},
    "LRHUTTTTCLM156S": {"freq": "monthly", "title": "Chile Unemployment"},
    "LRHUTTTTESM156S": {"freq": "monthly", "title": "Spain Unemployment"},
    "LRHUTTTTITM156S": {"freq": "monthly", "title": "Italy Unemployment"},
    "LRHUTTTTFRM156S": {"freq": "monthly", "title": "France Unemployment"},
    "LRHUTTTTDEM156S": {"freq": "monthly", "title": "Germany Unemployment"},
    "LRHUTTTTJPM156S": {"freq": "monthly", "title": "Japan Unemployment"},
    "LRHUTTTTKRM156S": {"freq": "monthly", "title": "Korea Unemployment"},
    "LRHUTTTTEZM156S": {"freq": "monthly", "title": "Eurozone Unemployment"},

    # Country industrial production
    "DEUPROINDMISMEI":  {"freq": "monthly", "title": "Germany Industrial Production"},
    "ITAPROINDMISMEI":  {"freq": "monthly", "title": "Italy Industrial Production"},
    "ESPPROINDMISMEI":  {"freq": "monthly", "title": "Spain Industrial Production"},
    "FRAPROINDMISMEI":  {"freq": "monthly", "title": "France Industrial Production"},
    "JPNPROINDMISMEI":  {"freq": "monthly", "title": "Japan Industrial Production"},
    "KORPROINDMISMEI":  {"freq": "monthly", "title": "Korea Industrial Production"},
    "CHNPROINDMISMEI":  {"freq": "monthly", "title": "China Industrial Production"},
    "CHLPROINDMISMEI":  {"freq": "monthly", "title": "Chile Industrial Production"},
    "GBRPROINDMISMEI":  {"freq": "monthly", "title": "UK Industrial Production"},
}

ETFS = [
    # Geography — EM
    "EEM",   # MSCI Emerging Markets
    "INDA",  # India
    "EWZ",   # Brazil
    "EWW",   # Mexico
    "VNM",   # Vietnam
    "EIDO",  # Indonesia
    "EZA",   # South Africa
    "FXI",   # China large-cap
    "FM",    # Frontier Markets
    "FRN",   # Frontier Markets Bond

    # Geography — Eurozone country-level
    "EWG",   # Germany
    "EWP",   # Spain
    "EWI",   # Italy
    "EWQ",   # France
    "EWN",   # Netherlands (offshore USD hub)
    "EWS",   # Singapore
    "EWH",   # Hong Kong
    "EWU",   # UK
    "EWL",   # Switzerland (also nickel/copper proxy via EFNL alt)
    "EFNL",  # Finland (copper/nickel proxy)
    "EWY",   # South Korea
    "EWJ",   # Japan
    "EWT",   # Taiwan

    # Copper/commodity producers
    "ECH",   # Chile (copper)
    "EPU",   # Peru (copper)
    "COPX",  # Global X Copper Miners
    "PICK",  # iShares Metals & Mining

    # Caps + reference
    "IWC",   # iShares Microcap
    "IWM",   # Russell 2000 Small Cap
    "SPY",   # S&P 500
    "QQQ",   # Nasdaq 100
    "TLT",   # Long Treasuries
    "GLD",   # Gold
    "SLV",   # Silver (for gold/silver ratio)
    "USO",   # Oil
    "VIXY",  # VIX short-term
    "HYG",   # HY credit
    "DBC",   # Broad commodities
    "WOOD",  # Lumber/timber proxy
    "SOXX",  # Semiconductors (Taiwan beta)
    "BDRY",  # Breakwave Dry Bulk Shipping (Baltic Dry proxy)
]


# ─────────────────────────────────────────────────────────────────────────────
# RELATIONSHIPS — 32 new divergence pairs (in addition to v1's 12)
# ─────────────────────────────────────────────────────────────────────────────
# Tuple: (id, name, series_a, series_b, polarity, category, description, window_days)
#   polarity: +1 = positive correlation (move together), -1 = inverse
#   window_days: rolling z-score window (60 daily, 90 weekly, 365 monthly)

RELATIONSHIPS = [
    # ─── A. LABOR MARKET ──────────────────────────────────────────────────
    ("claims_lead_lag", "Initial vs Continuing Claims (lead-lag)",
     ("fred", "ICSA"), ("fred", "CCSA"), +1, "labor",
     "Initial claims lead continuing by 4-8 weeks; divergence flags inflection",
     180),
    ("u6_unrate_gap", "U-6 vs U-3 Unemployment Spread",
     ("fred", "U6RATE"), ("fred", "UNRATE"), +1, "labor",
     "U-6 normally moves with U-3; gap widening = labor underutilization rising",
     365),
    ("jolts_quits_hires", "JOLTS Quits vs Hires (worker confidence)",
     ("fred", "JTSQUL"), ("fred", "JTSHIL"), +1, "labor",
     "Quits/hires normally co-move; quits dropping faster = recession leading indicator",
     365),
    ("hours_employment", "Avg Hours Worked vs Unemployment",
     ("fred", "AWHAETP"), ("fred", "UNRATE"), -1, "labor",
     "Hours fall before layoffs — deepest leading labor signal (Bernanke favorite)",
     365),
    ("wage_inflation", "Wage Growth (ECI) vs PCE Inflation",
     ("fred", "ECIWAG"), ("fred", "PCEPI"), +1, "labor",
     "Wages lag inflation 2-3 quarters; divergence = real wage shock",
     720),

    # ─── B. MANUFACTURING ─────────────────────────────────────────────────
    ("ip_capacity", "Industrial Production vs Capacity Utilization",
     ("fred", "INDPRO"), ("fred", "TCU"), +1, "manufacturing",
     "IP and capacity should move together; capacity falling faster = demand collapse",
     365),
    ("new_orders_total", "Mfg New Orders (Cap Goods) vs Total New Orders",
     ("fred", "NEWORDER"), ("fred", "AMTMNO"), +1, "manufacturing",
     "Capex orders lead total orders; divergence = capex hesitancy",
     365),
    ("durable_capex", "Durable Goods vs Core Capex",
     ("fred", "DGORDER"), ("fred", "NEWORDER"), +1, "manufacturing",
     "Durable goods include defense; core capex is private investment signal",
     365),
    ("ip_manufacturing_split", "IP Manufacturing vs Total IP",
     ("fred", "IPMAN"), ("fred", "INDPRO"), +1, "manufacturing",
     "Manufacturing share of IP — shifts indicate cycle phase",
     365),
    ("ipman_uti_xli", "Mfg Production vs XLI Industrials",
     ("fred", "IPMAN"), ("etf", "PICK"), +1, "manufacturing",
     "Real production should match equity beta; divergence = sentiment mispricing",
     365),

    # ─── C. LEADING INDICATORS ────────────────────────────────────────────
    ("lei_cei", "Leading vs Coincident Economic Indicators",
     ("fred", "USSLIND"), ("fred", "USPHCI"), +1, "leading",
     "LEI leads CEI by 6-9 months; LEI dropping while CEI flat = imminent recession",
     720),
    ("lei_curve", "LEI vs 10Y-2Y Yield Curve",
     ("fred", "USSLIND"), ("fred", "T10Y2Y"), +1, "leading",
     "Curve is component of LEI; divergence = other LEI components diverging",
     720),
    ("sentiment_lei", "Consumer Sentiment vs LEI",
     ("fred", "UMCSENT"), ("fred", "USSLIND"), +1, "leading",
     "Sentiment is forward; sentiment improving while LEI dropping = false dawn",
     365),

    # ─── D. EM / FRONTIER GEOGRAPHY ───────────────────────────────────────
    ("india_em", "India (INDA) vs EM (EEM)",
     ("etf", "INDA"), ("etf", "EEM"), +1, "em",
     "India outperforms EM = domestic demand strength; underperforms = USD strain",
     90),
    ("brazil_copper", "Brazil (EWZ) vs Copper",
     ("etf", "EWZ"), ("fred", "PCOPPUSDM"), +1, "em",
     "Brazil is heavy resources; divergence = Real currency or political shock",
     90),
    ("mexico_dollar", "Mexico (EWW) vs USMXN",
     ("etf", "EWW"), ("fred", "DEXMXUS"), -1, "em",
     "Strong USD/MXN hurts Mexico; dollar-decoupling = capital flight worry",
     60),
    ("vietnam_china", "Vietnam (VNM) vs China (FXI)",
     ("etf", "VNM"), ("etf", "FXI"), -1, "em",
     "Vietnam benefits from China supply-chain shift; co-moving = correlated outflow",
     60),
    ("indonesia_em", "Indonesia (EIDO) vs EM (EEM)",
     ("etf", "EIDO"), ("etf", "EEM"), +1, "em",
     "Indonesia commodity exporter; divergence = nickel/coal price action",
     90),
    ("south_africa_gold", "South Africa (EZA) vs Gold",
     ("etf", "EZA"), ("etf", "GLD"), +1, "em",
     "EZA heavy in mining; gold-EZA divergence = ZAR or operational stress",
     90),
    ("frontier_em", "Frontier Markets (FM) vs EM (EEM)",
     ("etf", "FM"), ("etf", "EEM"), +1, "em",
     "FM outperforms EM late-cycle; FM lagging = liquidity contraction starting",
     90),

    # ─── E. MICRO / SMALL CAP ROTATION ────────────────────────────────────
    ("microcap_smallcap", "Microcap (IWC) vs Smallcap (IWM)",
     ("etf", "IWC"), ("etf", "IWM"), +1, "rotation",
     "Microcap leadership = strongest risk appetite; lagging = early risk-off",
     60),
    ("smallcap_largecap", "Smallcap (IWM) vs S&P (SPY) [v2 lens]",
     ("etf", "IWM"), ("etf", "SPY"), +1, "rotation",
     "Confirmation of v1 IWM/curve; tracks breadth + risk appetite",
     60),

    # ─── F. COPPER PRODUCERS ──────────────────────────────────────────────
    ("chile_copper", "Chile (ECH) vs Copper Price",
     ("etf", "ECH"), ("fred", "PCOPPUSDM"), +1, "copper",
     "Chile = world's largest copper producer; divergence = Codelco/peso shock",
     90),
    ("peru_copper", "Peru (EPU) vs Copper Price",
     ("etf", "EPU"), ("fred", "PCOPPUSDM"), +1, "copper",
     "Peru = #2 copper producer; political stability shock = sharp divergence",
     90),
    ("finland_nickel", "Finland (EFNL) vs Nickel Price",
     ("etf", "EFNL"), ("fred", "PNICKUSDM"), +1, "copper",
     "Finland = nickel/copper miner; battery-metal demand divergence",
     90),
    ("copper_miners_metal", "Copper Miners (COPX) vs Copper Price",
     ("etf", "COPX"), ("fred", "PCOPPUSDM"), +1, "copper",
     "Equity should track metal; lagging = sector skepticism, leading = production beats",
     60),

    # ─── G. EURODOLLAR CENTERS ────────────────────────────────────────────
    ("hkd_peg_stress", "HK Equity (EWH) vs HKD/USD Peg",
     ("etf", "EWH"), ("fred", "DEXHKUS"), -1, "eurodollar",
     "HKD trades at top of band when offshore USD funding tight; EWH divergence",
     60),
    ("netherlands_dollar", "Netherlands (EWN) vs Broad Dollar",
     ("etf", "EWN"), ("fred", "DTWEXBGS"), -1, "eurodollar",
     "NL is major Eurobond issuance hub; dollar strength = funding strain on issuers",
     60),
    ("singapore_em", "Singapore (EWS) vs EM (EEM)",
     ("etf", "EWS"), ("etf", "EEM"), +1, "eurodollar",
     "SG = USD funding gateway for ASEAN; divergence = liquidity stress signal",
     90),
    ("uk_dollar", "UK (EWU) vs USD/GBP",
     ("etf", "EWU"), ("fred", "DEXUSUK"), -1, "eurodollar",
     "UK FTSE earnings are dollar-heavy; sterling weakness = mechanical EWU lift",
     60),

    # ─── H. CROSS-CURRENCY DEPTH ──────────────────────────────────────────
    ("eur_jpy_dxy", "EUR/USD vs JPY/USD (DXY components)",
     ("fred", "DEXUSEU"), ("fred", "DEXJPUS"), +1, "fx",
     "Both major DXY components; divergence = single-currency idiosyncratic shock",
     60),
    ("yield_curve_dollar", "Yield Curve (T10Y2Y) vs Broad Dollar",
     ("fred", "T10Y2Y"), ("fred", "DTWEXBGS"), -1, "fx",
     "Steepening curve usually weakens dollar (carry trade); divergence = unusual",
     60),

    # ─── I. CRISIS-LEADING INDICATORS (Bernanke / Calculated Risk classics) ─
    ("heavy_trucks_spy", "Heavy Truck Sales vs S&P 500 ⚡",
     ("fred", "HTRUCKSSAAR"), ("etf", "SPY"), +1, "crisis_leading",
     "HTRUCKSSAAR peaks 12-18 months before EVERY post-WWII recession. The classic CR signal.",
     720),
    ("heavy_trucks_unrate", "Heavy Truck Sales vs Unemployment",
     ("fred", "HTRUCKSSAAR"), ("fred", "UNRATE"), -1, "crisis_leading",
     "Truck demand collapses BEFORE layoffs; divergence = late-cycle warning",
     720),
    ("permits_spy", "Building Permits vs S&P 500",
     ("fred", "PERMIT"), ("etf", "SPY"), +1, "crisis_leading",
     "Permits are forward-looking; equity diverging from permits = real-economy weakness",
     365),
    ("permits_starts", "Building Permits vs Housing Starts",
     ("fred", "PERMIT"), ("fred", "HOUST"), +1, "crisis_leading",
     "Permits lead starts by 1-2 months; gap widening = builder hesitation",
     365),
    ("auto_sales_sentiment", "Auto Sales vs Consumer Sentiment",
     ("fred", "TOTALSA"), ("fred", "UMCSENT"), +1, "crisis_leading",
     "Big-ticket spending follows confidence; auto weakness leading sentiment = imminent slowdown",
     365),
    ("baa_spread_spy", "BAA Corporate Spread vs S&P 500",
     ("fred", "BAA10YM"), ("etf", "SPY"), -1, "crisis_leading",
     "Corporate credit stress leads equity selloffs by 3-6 months",
     365),
    ("delinquencies_spy", "Credit Card Delinquencies vs S&P 500",
     ("fred", "DRSDCIS"), ("etf", "SPY"), -1, "crisis_leading",
     "Consumer stress leading equity strength = late-cycle topping pattern",
     720),
    ("business_loans_indpro", "Bank C&I Loans vs Industrial Production",
     ("fred", "BUSLOANS"), ("fred", "INDPRO"), +1, "crisis_leading",
     "Lending normally tracks production; divergence = credit-tightening or production stall",
     365),
    ("retail_sentiment", "Retail Sales vs Consumer Sentiment",
     ("fred", "RSAFS"), ("fred", "UMCSENT"), +1, "crisis_leading",
     "Sentiment leading actual spend by 1-3 months",
     365),
    ("recession_prob_spy", "NY Fed Recession Probability vs S&P 500",
     ("fred", "RECPROUSM156N"), ("etf", "SPY"), -1, "crisis_leading",
     "When NY Fed model says recession coming and SPY at highs = max divergence",
     365),

    # ─── J. EUROPEAN COUNTRY UNEMPLOYMENT (specific to Khalid's request) ───
    ("spain_unemployment", "Spain Equity (EWP) vs Spain Unemployment",
     ("etf", "EWP"), ("fred", "LRHUTTTTESM156S"), -1, "europe_labor",
     "Spain has structurally high unemployment; equity diverging UP while joblessness rises = bubble",
     365),
    ("italy_unemployment", "Italy Equity (EWI) vs Italy Unemployment",
     ("etf", "EWI"), ("fred", "LRHUTTTTITM156S"), -1, "europe_labor",
     "Italy political-fragility marker; unemployment rising while EWI rallies = late-cycle",
     365),
    ("france_unemployment", "France Equity (EWQ) vs France Unemployment",
     ("etf", "EWQ"), ("fred", "LRHUTTTTFRM156S"), -1, "europe_labor",
     "France labor lag indicator vs equity",
     365),
    ("germany_unemployment", "Germany Equity (EWG) vs Germany Unemployment",
     ("etf", "EWG"), ("fred", "LRHUTTTTDEM156S"), -1, "europe_labor",
     "Germany = Eurozone industrial heart; unemployment rising = continental weakness",
     365),
    ("switzerland_unemployment", "Switzerland Equity (EWL) vs CH Unemployment",
     ("etf", "EWL"), ("fred", "LRHUTTTTCHM156S"), -1, "europe_labor",
     "Switzerland labor stable historically; rises here = Eurozone contagion warning",
     365),
    ("eurozone_unemployment", "Germany Equity (EWG) vs Eurozone Unemployment",
     ("etf", "EWG"), ("fred", "LRHUTTTTEZM156S"), -1, "europe_labor",
     "Continental labor health vs largest national equity proxy",
     365),

    # ─── K. ASIAN UNEMPLOYMENT + IP DEPTH ─────────────────────────────────
    ("japan_unemployment", "Japan (EWJ) vs Japan Unemployment",
     ("etf", "EWJ"), ("fred", "LRHUTTTTJPM156S"), -1, "asia_labor",
     "Japan structurally low unemployment; rises = exceptional weakness signal",
     365),
    ("korea_unemployment", "Korea (EWY) vs Korea Unemployment",
     ("etf", "EWY"), ("fred", "LRHUTTTTKRM156S"), -1, "asia_labor",
     "Korea labor leads China cycle (export channel)",
     365),

    # ─── L. INDUSTRIAL PRODUCTION BY COUNTRY ──────────────────────────────
    ("germany_ip_equity", "Germany IP vs Germany Equity (EWG)",
     ("fred", "DEUPROINDMISMEI"), ("etf", "EWG"), +1, "country_ip",
     "Real production vs equity beta; divergence = earnings recalibration",
     365),
    ("italy_ip_equity", "Italy IP vs Italy Equity (EWI)",
     ("fred", "ITAPROINDMISMEI"), ("etf", "EWI"), +1, "country_ip",
     "Italy industrial activity vs FTSE MIB",
     365),
    ("spain_ip_equity", "Spain IP vs Spain Equity (EWP)",
     ("fred", "ESPPROINDMISMEI"), ("etf", "EWP"), +1, "country_ip",
     "Spain IBEX vs production",
     365),
    ("france_ip_equity", "France IP vs France Equity (EWQ)",
     ("fred", "FRAPROINDMISMEI"), ("etf", "EWQ"), +1, "country_ip",
     "France CAC40 vs production",
     365),
    ("japan_ip_equity", "Japan IP vs Japan Equity (EWJ)",
     ("fred", "JPNPROINDMISMEI"), ("etf", "EWJ"), +1, "country_ip",
     "Nikkei vs Japan IP",
     365),
    ("korea_ip_equity", "Korea IP vs Korea Equity (EWY)",
     ("fred", "KORPROINDMISMEI"), ("etf", "EWY"), +1, "country_ip",
     "Korea KOSPI vs production — semis cycle proxy",
     365),
    ("china_ip_fxi", "China IP vs China Equity (FXI)",
     ("fred", "CHNPROINDMISMEI"), ("etf", "FXI"), +1, "country_ip",
     "China IP vs Hang Seng/MSCI China",
     365),
    ("chile_ip_copper", "Chile IP vs Copper Price",
     ("fred", "CHLPROINDMISMEI"), ("fred", "PCOPPUSDM"), +1, "country_ip",
     "Chilean industrial activity tracks copper revenue ~70%",
     365),
    ("uk_ip_equity", "UK IP vs UK Equity (EWU)",
     ("fred", "GBRPROINDMISMEI"), ("etf", "EWU"), +1, "country_ip",
     "FTSE earnings vs domestic production (export-heavy index)",
     365),

    # ─── M. CHILE-SPECIFIC LABOR + COPPER ─────────────────────────────────
    ("chile_unemployment", "Chile Equity (ECH) vs Chile Unemployment",
     ("etf", "ECH"), ("fred", "LRHUTTTTCLM156S"), -1, "copper",
     "Chilean equities heavy in copper miners; unemployment vs equity divergence",
     365),

    # ─── N. ASIA TECH SUPPLY CHAIN (Taiwan + Korea + semis) ───────────────
    ("taiwan_semis", "Taiwan Equity (EWT) vs Semiconductors (SOXX)",
     ("etf", "EWT"), ("etf", "SOXX"), +1, "asia_tech",
     "Taiwan = TSMC + supply chain; SOXX divergence = single-stock vs sector",
     60),
    ("korea_semis", "Korea Equity (EWY) vs Semiconductors (SOXX)",
     ("etf", "EWY"), ("etf", "SOXX"), +1, "asia_tech",
     "Korea = Samsung/SK Hynix; memory cycle vs broad semis",
     60),
    ("krw_semis", "KRW/USD vs Semiconductors (SOXX)",
     ("fred", "DEXKOUS"), ("etf", "SOXX"), +1, "asia_tech",
     "Won weakness usually accompanies semi cycle bottom",
     60),

    # ─── O. SHIPPING / GLOBAL TRADE ───────────────────────────────────────
    ("baltic_dry_commodities", "Baltic Dry Bulk Shipping vs Commodities",
     ("etf", "BDRY"), ("etf", "DBC"), +1, "shipping",
     "Shipping rates lead commodity demand by 2-3 months; BDRY collapse = trade slowdown",
     60),
    ("baltic_dry_china", "Baltic Dry Shipping vs China Equity",
     ("etf", "BDRY"), ("etf", "FXI"), +1, "shipping",
     "Dry bulk = iron ore/coal proxy = China industrial demand",
     60),

    # ─── P. COMMODITY RATIOS (regime indicators) ──────────────────────────
    ("gold_silver_spy", "Gold/Silver ratio vs S&P 500",
     ("etf", "GLD"), ("etf", "SLV"), +1, "commodity_ratios",
     "Rising gold/silver ratio = risk-off; SPY divergence = late-cycle topping",
     60),
    ("lumber_houst", "Lumber (WOOD) vs Housing Starts",
     ("etf", "WOOD"), ("fred", "HOUST"), +1, "commodity_ratios",
     "Lumber demand leads housing starts; divergence = builder caution",
     90),
    ("oil_brent_wti", "WTI vs Brent (curve flattening)",
     ("fred", "DCOILWTICO"), ("fred", "DCOILBRENTEU"), +1, "commodity_ratios",
     "WTI-Brent spread reflects US export supply dynamics",
     60),

    # ─── Q. SAFE-HAVEN FX ──────────────────────────────────────────────────
    ("chf_gold", "Swiss Franc (CHF) vs Gold",
     ("fred", "DEXSZUS"), ("etf", "GLD"), -1, "safe_haven",
     "Both safe-havens; divergence = SNB intervention or specific shock",
     60),

    # ─── R. CORPORATE CREDIT TIERS ────────────────────────────────────────
    ("aaa_baa_spread", "AAA-10Y vs BAA-10Y Corporate Spreads",
     ("fred", "AAA10YM"), ("fred", "BAA10YM"), +1, "credit_tiers",
     "BAA stress without AAA stress = risk-on credit risk-taking. Both rising = systemic.",
     365),
]


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHERS (parallel)
# ─────────────────────────────────────────────────────────────────────────────
def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-DivV2/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_fred(series_id, days=2000):
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&observation_start={start}&observation_end={end}")
    try:
        d = _http_get_json(url, timeout=20)
        obs = d.get("observations", [])
        out = []
        for o in obs:
            v = o.get("value", ".")
            if v != "." and v is not None:
                try:
                    out.append({"date": o["date"], "value": float(v)})
                except (ValueError, TypeError):
                    continue
        return out
    except Exception as e:
        print(f"[fred] {series_id}: {type(e).__name__}: {e}")
        return []


def fetch_etf(symbol, days=180):
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
           f"?symbol={symbol}&apikey={FMP_KEY}")
    try:
        d = _http_get_json(url, timeout=15)
        # FMP returns list of {date, close, ...} sorted desc
        items = d if isinstance(d, list) else d.get("historical", [])
        out = []
        for item in items[:days]:
            try:
                out.append({"date": item["date"], "value": float(item["close"])})
            except (KeyError, ValueError, TypeError):
                continue
        # Sort ascending by date for time-series ops
        out.sort(key=lambda x: x["date"])
        return out
    except Exception as e:
        print(f"[fmp] {symbol}: {type(e).__name__}: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Z-SCORE COMPUTATION (handles mixed frequencies)
# ─────────────────────────────────────────────────────────────────────────────
def latest_z_score(series, window):
    """Z-score of latest value vs trailing rolling window of same series.
    series: list of {date, value} sorted ascending.
    window: number of trailing observations.
    """
    if not series or len(series) < 5:
        return None
    values = [s["value"] for s in series[-window:]]
    if len(values) < 5:
        return None
    try:
        mean = statistics.mean(values)
        stdev = statistics.stdev(values)
        if stdev < 1e-10:
            return 0.0
        return (values[-1] - mean) / stdev
    except statistics.StatisticsError:
        return None


def compute_divergence(rel, data_cache):
    """Compute divergence z-score for one relationship."""
    rel_id, name, sa, sb, polarity, category, desc, window = rel

    series_a = data_cache.get((sa[0], sa[1]))
    series_b = data_cache.get((sb[0], sb[1]))

    if not series_a or not series_b:
        return {
            "id": rel_id, "name": name, "category": category,
            "status": "no_data",
            "series_a": f"{sa[0]}:{sa[1]}",
            "series_b": f"{sb[0]}:{sb[1]}",
        }

    # Frequency-appropriate window:
    # - daily ETFs: 60-90 trading days
    # - weekly FRED (claims): 13-26 obs
    # - monthly FRED (ISM/LEI): 12-24 obs
    z_a = latest_z_score(series_a, window if sa[0] == "fred" else min(window, 90))
    z_b = latest_z_score(series_b, window if sb[0] == "fred" else min(window, 90))

    if z_a is None or z_b is None:
        return {
            "id": rel_id, "name": name, "category": category,
            "status": "insufficient_data",
            "series_a": f"{sa[0]}:{sa[1]}",
            "series_b": f"{sb[0]}:{sb[1]}",
        }

    # Divergence = z(A) - polarity * z(B)
    divergence = round(z_a - polarity * z_b, 3)
    abs_div = abs(divergence)

    if abs_div >= Z_EXTREME_THRESHOLD:
        status = "extreme"
    elif abs_div >= Z_FLAG_THRESHOLD:
        status = "flagged"
    else:
        status = "normal"

    direction = "above" if divergence > 0 else "below"
    return {
        "id": rel_id,
        "name": name,
        "category": category,
        "description": desc,
        "polarity": polarity,
        "z_a": round(z_a, 3),
        "z_b": round(z_b, 3),
        "divergence_z": divergence,
        "status": status,
        "direction": direction,
        "latest_a": series_a[-1]["value"] if series_a else None,
        "latest_b": series_b[-1]["value"] if series_b else None,
        "as_of_a": series_a[-1]["date"] if series_a else None,
        "as_of_b": series_b[-1]["date"] if series_b else None,
        "window_days": window,
    }


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM (extreme alerts only)
# ─────────────────────────────────────────────────────────────────────────────
def send_telegram(msg):
    try:
        token = SSM.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = SSM.get_parameter(Name=TG_CHAT_ID_PARAM, WithDecryption=True)["Parameter"]["Value"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": msg, "parse_mode": "Markdown",
        }).encode()
        urllib.request.urlopen(url, data=data, timeout=10).read()
    except Exception as e:
        print(f"[telegram] failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()

    # Determine which FRED series + ETFs to fetch (only those used in
    # at least one relationship, to minimize API calls)
    needed_fred = set()
    needed_etfs = set()
    for rel in RELATIONSHIPS:
        for source, key in (rel[2], rel[3]):
            if source == "fred":
                needed_fred.add(key)
            elif source == "etf":
                needed_etfs.add(key)

    print(f"[divv2] Fetching {len(needed_fred)} FRED + {len(needed_etfs)} ETFs in parallel")

    data_cache = {}
    fetch_errors = {"fred": 0, "etf": 0}

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {}
        for sid in needed_fred:
            futures[ex.submit(fetch_fred, sid)] = ("fred", sid)
        for sym in needed_etfs:
            futures[ex.submit(fetch_etf, sym)] = ("etf", sym)
        for fut in as_completed(futures):
            source, key = futures[fut]
            try:
                data = fut.result()
                data_cache[(source, key)] = data
                if not data:
                    fetch_errors[source] += 1
            except Exception as e:
                print(f"[divv2] fetch err {source}:{key}: {e}")
                fetch_errors[source] += 1
                data_cache[(source, key)] = []

    # Compute divergences
    results = []
    for rel in RELATIONSHIPS:
        result = compute_divergence(rel, data_cache)
        results.append(result)

    # Aggregate stats
    by_status = {"normal": 0, "flagged": 0, "extreme": 0, "no_data": 0, "insufficient_data": 0}
    by_category = {}
    extreme_alerts = []
    flagged = []

    for r in results:
        status = r.get("status", "no_data")
        by_status[status] = by_status.get(status, 0) + 1
        cat = r.get("category", "other")
        by_category.setdefault(cat, []).append(r)
        if status == "extreme":
            extreme_alerts.append(r)
        elif status == "flagged":
            flagged.append(r)

    # Sort each category by abs(divergence_z) descending
    for cat, items in by_category.items():
        items.sort(key=lambda x: abs(x.get("divergence_z", 0)), reverse=True)

    # Composite divergence index (0-100): % of relationships flagged or extreme
    n_total = len([r for r in results if r.get("status") not in ("no_data", "insufficient_data")])
    n_flagged_or_extreme = by_status["flagged"] + by_status["extreme"]
    composite_index = round(100 * n_flagged_or_extreme / max(n_total, 1), 1)

    # Build output payload
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "version": "v2",
        "n_relationships": len(RELATIONSHIPS),
        "n_with_data": n_total,
        "n_flagged": by_status["flagged"],
        "n_extreme": by_status["extreme"],
        "composite_divergence_index": composite_index,
        "by_status": by_status,
        "fetch_errors": fetch_errors,
        "by_category": by_category,
        "extreme_alerts": extreme_alerts,
        "flagged": flagged,
        "all_relationships": results,
        "duration_s": round(time.time() - started, 1),
    }

    # Persist to S3
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY, Body=body,
        ContentType="application/json",
        CacheControl="max-age=300",
    )

    # Telegram alert if any extreme — rare event
    if extreme_alerts:
        lines = [f"🚨 *Divergence v2.5 EXTREME (>3σ)* — {len(extreme_alerts)} signals"]
        for a in extreme_alerts[:5]:
            lines.append(f"• {a['name']}: z={a['divergence_z']} ({a['direction']} {a['polarity']:+})")
        if len(extreme_alerts) > 5:
            lines.append(f"... and {len(extreme_alerts) - 5} more")
        lines.append(f"\nComposite Index: {composite_index}/100. See divergence-v2.html.")
        send_telegram("\n".join(lines))

    print(f"[divv2] done in {payload['duration_s']}s — flagged={by_status['flagged']} extreme={by_status['extreme']}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_relationships": payload["n_relationships"],
            "n_flagged": payload["n_flagged"],
            "n_extreme": payload["n_extreme"],
            "composite_index": composite_index,
            "duration_s": payload["duration_s"],
        }),
    }
