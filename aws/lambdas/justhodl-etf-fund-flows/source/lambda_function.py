"""justhodl-etf-fund-flows

Institutional ETF Capital Flow Intelligence Engine.

WHY THIS MATTERS
════════════════
ETF Global Fund Flows is the most expensive Polygon subscription ($99/mo)
and the most differentiated edge in the entire JustHodl.AI data stack.
Daily creation/redemption flows for 3,000+ ETFs is data that hedge funds
pay Lipper/Morningstar/EPFR $30K+/yr for. Academic backing is real:

  - Ben-David et al. (2017): ETF flows precede underlying price action
  - BIS Quarterly Review (2018): ETF flows are leading indicator of
    cross-asset risk sentiment
  - Israeli et al. (2017): TLT inflows precede flight-to-safety regimes

THE INSTITUTIONAL EDGE
══════════════════════
Single-ETF flows are noise. The alpha is in the COMBINATIONS:

  - Sector rotation: which sectors are seeing persistent inflow vs
    outflow over 5/21 days (not just today's print)
  - Smart vs dumb money: SPY/QQQ/sector-SPDR flows (institutional) vs
    ARKK/SOXL/leveraged flows (retail-favored). Divergence = contrarian
    signal
  - Risk-on/off: equity inflows + Treasury outflows = risk-on;
    Treasury inflows + equity outflows = de-risking
  - Growth vs value: VUG/MTUM flows vs IWD/VLUE flows
  - Credit stress: HYG outflows + LQD inflows = flight to quality
  - Domestic/international: SPY/QQQ vs EFA/EEM/VWO flows

OUTPUT ARCHITECTURE
═══════════════════
Writes 5 S3 files:

  etf-flows/daily.json
      Full snapshot: per-ETF raw flows, % AUM, 5/21d cumulative,
      90d z-score, persistence days, signal label
      Read by: /flows.html dashboard, analytics workbench

  etf-flows/composite.json
      6 institutional composite signals with current values + history
      Read by: /flows.html, signal-board, research/critique prompts

  etf-flows/rotation.json
      Sector rotation matrix: which sectors are gaining/losing,
      with category aggregates
      Read by: /flows.html sector heatmap

  etf-flows/per-ticker-context.json
      Lookup: {TICKER: sector_etf_flow_context} for every
      research-universe ticker. Injected into research/critique prompts.
      Read by: equity-research Lambda, research-critique Lambda

  etf-flows/history/{YYYY-MM-DD}/{ETF}.json
      Date-stamped snapshots for backtest attribution by regime.

POLYGON API
═══════════
Endpoint: https://api.polygon.io/etf-global/v1/fund-flows
Auth: ?apiKey=$POLYGON_KEY
Per-ETF request: ?composite_ticker={ETF}
Response: { results: [{ processed_date, fund_flow_daily, fund_flow_5d,
            fund_flow_21d, aum, ... }] }

Scheduled: cron(0 22 * * ? *) = 17:00 ET daily (after Polygon's
EOD processing). Rate limit: ~100 calls in ~1 minute (well within
Polygon's 1000/min Starter limit).
"""
import json
import os
import time
import urllib.request
import urllib.error
import statistics
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_PREFIX = "etf-flows/"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
ETF_FLOWS_ENDPOINT = f"{POLYGON_BASE}/etf-global/v1/fund-flows"
FETCH_TIMEOUT = 15
MAX_WORKERS = 12

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# ETF UNIVERSE — institutionally tagged ~100 ETFs
# ═════════════════════════════════════════════════════════════════════
# Each entry has:
#   category:    high-level bucket (broad/sector/factor/country/treasury/
#                                    credit/commodity/thematic/leveraged/crypto)
#   subcategory: granular tag (technology/financials/momentum/japan/etc.)
#   smart_money: True if the ETF is institutionally-favored, False if
#                retail-favored. Used for the smart-vs-dumb composite.
#   region:      US/Intl/EM/Global
#   ref_sector:  GICS sector this ETF maps to (for ticker→ETF lookup)

ETF_UNIVERSE = {
    # Broad market — smart money's preferred US exposure
    "SPY":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "VOO":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "IVV":  {"category": "broad", "subcategory": "us_large_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "QQQ":  {"category": "broad", "subcategory": "us_megacap_tech",    "smart_money": True,  "region": "US",   "ref_sector": None},
    "IWM":  {"category": "broad", "subcategory": "us_small_cap",       "smart_money": True,  "region": "US",   "ref_sector": None},
    "VTI":  {"category": "broad", "subcategory": "us_total",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "DIA":  {"category": "broad", "subcategory": "us_dow",             "smart_money": True,  "region": "US",   "ref_sector": None},
    # GICS Sector SPDRs — the institutional sector rotation universe
    "XLK":  {"category": "sector", "subcategory": "technology",         "smart_money": True,  "region": "US", "ref_sector": "Technology"},
    "XLF":  {"category": "sector", "subcategory": "financials",         "smart_money": True,  "region": "US", "ref_sector": "Financial Services"},
    "XLE":  {"category": "sector", "subcategory": "energy",             "smart_money": True,  "region": "US", "ref_sector": "Energy"},
    "XLV":  {"category": "sector", "subcategory": "healthcare",         "smart_money": True,  "region": "US", "ref_sector": "Healthcare"},
    "XLP":  {"category": "sector", "subcategory": "staples",            "smart_money": True,  "region": "US", "ref_sector": "Consumer Defensive"},
    "XLY":  {"category": "sector", "subcategory": "discretionary",      "smart_money": True,  "region": "US", "ref_sector": "Consumer Cyclical"},
    "XLI":  {"category": "sector", "subcategory": "industrials",        "smart_money": True,  "region": "US", "ref_sector": "Industrials"},
    "XLB":  {"category": "sector", "subcategory": "materials",          "smart_money": True,  "region": "US", "ref_sector": "Basic Materials"},
    "XLU":  {"category": "sector", "subcategory": "utilities",          "smart_money": True,  "region": "US", "ref_sector": "Utilities"},
    "XLRE": {"category": "sector", "subcategory": "real_estate",        "smart_money": True,  "region": "US", "ref_sector": "Real Estate"},
    "XLC":  {"category": "sector", "subcategory": "communication",      "smart_money": True,  "region": "US", "ref_sector": "Communication Services"},
    # Factor ETFs — style positioning
    "MTUM": {"category": "factor", "subcategory": "momentum",           "smart_money": True,  "region": "US", "ref_sector": None},
    "VLUE": {"category": "factor", "subcategory": "value",              "smart_money": True,  "region": "US", "ref_sector": None},
    "QUAL": {"category": "factor", "subcategory": "quality",            "smart_money": True,  "region": "US", "ref_sector": None},
    "USMV": {"category": "factor", "subcategory": "min_vol",            "smart_money": True,  "region": "US", "ref_sector": None},
    "IWD":  {"category": "factor", "subcategory": "russell_value",      "smart_money": True,  "region": "US", "ref_sector": None},
    "IWF":  {"category": "factor", "subcategory": "russell_growth",     "smart_money": True,  "region": "US", "ref_sector": None},
    "SPLV": {"category": "factor", "subcategory": "low_vol",            "smart_money": True,  "region": "US", "ref_sector": None},
    "VUG":  {"category": "factor", "subcategory": "vanguard_growth",    "smart_money": True,  "region": "US", "ref_sector": None},
    "VTV":  {"category": "factor", "subcategory": "vanguard_value",     "smart_money": True,  "region": "US", "ref_sector": None},
    # Country/Region — global positioning
    "EFA":  {"category": "country", "subcategory": "eafe",              "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "VEA":  {"category": "country", "subcategory": "developed_intl",    "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EEM":  {"category": "country", "subcategory": "emerging_markets",  "smart_money": True,  "region": "EM",     "ref_sector": None},
    "VWO":  {"category": "country", "subcategory": "emerging_markets",  "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWJ":  {"category": "country", "subcategory": "japan",             "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "FXI":  {"category": "country", "subcategory": "china_large",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "MCHI": {"category": "country", "subcategory": "china_broad",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "KWEB": {"category": "country", "subcategory": "china_internet",    "smart_money": False, "region": "EM",     "ref_sector": None},
    "EWZ":  {"category": "country", "subcategory": "brazil",            "smart_money": True,  "region": "EM",     "ref_sector": None},
    "INDA": {"category": "country", "subcategory": "india",             "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWG":  {"category": "country", "subcategory": "germany",           "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWU":  {"category": "country", "subcategory": "uk",                "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWY":  {"category": "country", "subcategory": "south_korea",       "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWT":  {"category": "country", "subcategory": "taiwan",            "smart_money": True,  "region": "EM",     "ref_sector": None},
    "EWA":  {"category": "country", "subcategory": "australia",         "smart_money": True,  "region": "Intl",   "ref_sector": None},
    "EWC":  {"category": "country", "subcategory": "canada",            "smart_money": True,  "region": "Intl",   "ref_sector": None},
    # Treasury — duration positioning + flight-to-quality
    "TLT":  {"category": "treasury", "subcategory": "long_20plus",      "smart_money": True,  "region": "US",   "ref_sector": None},
    "IEF":  {"category": "treasury", "subcategory": "7_10yr",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "SHY":  {"category": "treasury", "subcategory": "1_3yr",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "GOVT": {"category": "treasury", "subcategory": "broad",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "BIL":  {"category": "treasury", "subcategory": "tbill",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "TIP":  {"category": "treasury", "subcategory": "tips",             "smart_money": True,  "region": "US",   "ref_sector": None},
    "AGG":  {"category": "treasury", "subcategory": "agg_bond",         "smart_money": True,  "region": "US",   "ref_sector": None},
    # Credit — risk appetite proxy
    "HYG":  {"category": "credit", "subcategory": "high_yield",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "JNK":  {"category": "credit", "subcategory": "high_yield",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "LQD":  {"category": "credit", "subcategory": "investment_grade",   "smart_money": True,  "region": "US",   "ref_sector": None},
    "EMB":  {"category": "credit", "subcategory": "em_bond",            "smart_money": True,  "region": "EM",   "ref_sector": None},
    # Commodities — inflation/macro positioning
    "GLD":  {"category": "commodity", "subcategory": "gold",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "IAU":  {"category": "commodity", "subcategory": "gold",            "smart_money": True,  "region": "US",   "ref_sector": None},
    "SLV":  {"category": "commodity", "subcategory": "silver",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "USO":  {"category": "commodity", "subcategory": "oil",             "smart_money": False, "region": "US",   "ref_sector": None},
    "UNG":  {"category": "commodity", "subcategory": "nat_gas",         "smart_money": False, "region": "US",   "ref_sector": None},
    "DBC":  {"category": "commodity", "subcategory": "broad",           "smart_money": True,  "region": "US",   "ref_sector": None},
    "DBA":  {"category": "commodity", "subcategory": "agriculture",     "smart_money": True,  "region": "US",   "ref_sector": None},
    "CPER": {"category": "commodity", "subcategory": "copper",          "smart_money": True,  "region": "US",   "ref_sector": None},
    # Thematic — typically retail-driven
    "ARKK": {"category": "thematic", "subcategory": "innovation",       "smart_money": False, "region": "US",   "ref_sector": None},
    "ARKW": {"category": "thematic", "subcategory": "web3",             "smart_money": False, "region": "US",   "ref_sector": None},
    "ARKG": {"category": "thematic", "subcategory": "genomics",         "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXX": {"category": "thematic", "subcategory": "semiconductors",   "smart_money": True,  "region": "US",   "ref_sector": "Technology"},
    "SMH":  {"category": "thematic", "subcategory": "semiconductors",   "smart_money": True,  "region": "US",   "ref_sector": "Technology"},
    "TAN":  {"category": "thematic", "subcategory": "solar",            "smart_money": False, "region": "US",   "ref_sector": None},
    "ICLN": {"category": "thematic", "subcategory": "clean_energy",     "smart_money": False, "region": "Global","ref_sector": None},
    "LIT":  {"category": "thematic", "subcategory": "lithium",          "smart_money": False, "region": "Global","ref_sector": None},
    "IBB":  {"category": "thematic", "subcategory": "biotech",          "smart_money": True,  "region": "US",   "ref_sector": "Healthcare"},
    "XBI":  {"category": "thematic", "subcategory": "biotech",          "smart_money": True,  "region": "US",   "ref_sector": "Healthcare"},
    "KRE":  {"category": "thematic", "subcategory": "regional_banks",   "smart_money": True,  "region": "US",   "ref_sector": "Financial Services"},
    # Leveraged — pure retail proxy
    "TQQQ": {"category": "leveraged", "subcategory": "3x_qqq_bull",     "smart_money": False, "region": "US",   "ref_sector": None},
    "SQQQ": {"category": "leveraged", "subcategory": "3x_qqq_bear",     "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXL": {"category": "leveraged", "subcategory": "3x_semi_bull",    "smart_money": False, "region": "US",   "ref_sector": None},
    "SOXS": {"category": "leveraged", "subcategory": "3x_semi_bear",    "smart_money": False, "region": "US",   "ref_sector": None},
    "TMF":  {"category": "leveraged", "subcategory": "3x_treasury",     "smart_money": False, "region": "US",   "ref_sector": None},
    "UVXY": {"category": "leveraged", "subcategory": "vol_long",        "smart_money": False, "region": "US",   "ref_sector": None},
    "SVXY": {"category": "leveraged", "subcategory": "vol_short",       "smart_money": False, "region": "US",   "ref_sector": None},
    # Crypto
    "IBIT": {"category": "crypto",   "subcategory": "bitcoin",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "FBTC": {"category": "crypto",   "subcategory": "bitcoin",          "smart_money": True,  "region": "US",   "ref_sector": None},
    "ETHA": {"category": "crypto",   "subcategory": "ethereum",         "smart_money": True,  "region": "US",   "ref_sector": None},
    "BITO": {"category": "crypto",   "subcategory": "bitcoin_futures",  "smart_money": False, "region": "US",   "ref_sector": None},
    # ── EXPANDED: leveraged broad market (institutional/retail risk positioning) ──
    "SPXL": {"category": "leveraged", "subcategory": "3x_sp500_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SPXS": {"category": "leveraged", "subcategory": "3x_sp500_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UPRO": {"category": "leveraged", "subcategory": "3x_sp500_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SPXU": {"category": "leveraged", "subcategory": "3x_sp500_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "SSO": {"category": "leveraged", "subcategory": "2x_sp500_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SDS": {"category": "leveraged", "subcategory": "2x_sp500_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "SH": {"category": "leveraged", "subcategory": "1x_sp500_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UDOW": {"category": "leveraged", "subcategory": "3x_dow_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SDOW": {"category": "leveraged", "subcategory": "3x_dow_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "QLD": {"category": "leveraged", "subcategory": "2x_qqq_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "QID": {"category": "leveraged", "subcategory": "2x_qqq_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "TNA": {"category": "leveraged", "subcategory": "3x_smallcap_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TZA": {"category": "leveraged", "subcategory": "3x_smallcap_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UWM": {"category": "leveraged", "subcategory": "2x_smallcap_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TWM": {"category": "leveraged", "subcategory": "2x_smallcap_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ── EXPANDED: leveraged sector (what investors lever long/short by sector) ──
    "TECL": {"category": "leveraged", "subcategory": "3x_tech_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TECS": {"category": "leveraged", "subcategory": "3x_tech_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "FAS": {"category": "leveraged", "subcategory": "3x_financials_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "FAZ": {"category": "leveraged", "subcategory": "3x_financials_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "ERX": {"category": "leveraged", "subcategory": "3x_energy_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "ERY": {"category": "leveraged", "subcategory": "3x_energy_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "GUSH": {"category": "leveraged", "subcategory": "2x_oilgas_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DRIP": {"category": "leveraged", "subcategory": "2x_oilgas_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "LABU": {"category": "leveraged", "subcategory": "3x_biotech_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "LABD": {"category": "leveraged", "subcategory": "3x_biotech_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "CURE": {"category": "leveraged", "subcategory": "3x_healthcare_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DRN": {"category": "leveraged", "subcategory": "3x_realestate_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DRV": {"category": "leveraged", "subcategory": "3x_realestate_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UTSL": {"category": "leveraged", "subcategory": "3x_utilities_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DUSL": {"category": "leveraged", "subcategory": "3x_industrials_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DPST": {"category": "leveraged", "subcategory": "3x_regionalbank_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "RETL": {"category": "leveraged", "subcategory": "3x_retail_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DFEN": {"category": "leveraged", "subcategory": "3x_defense_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "NAIL": {"category": "leveraged", "subcategory": "3x_homebuilder_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "WANT": {"category": "leveraged", "subcategory": "3x_discretionary_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "PILL": {"category": "leveraged", "subcategory": "3x_pharma_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "WEBL": {"category": "leveraged", "subcategory": "3x_internet_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "WEBS": {"category": "leveraged", "subcategory": "3x_internet_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "CWEB": {"category": "leveraged", "subcategory": "2x_chinainternet_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "YINN": {"category": "leveraged", "subcategory": "3x_china_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "YANG": {"category": "leveraged", "subcategory": "3x_china_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ── EXPANDED: leveraged commodity & metals ──
    "NUGT": {"category": "leveraged", "subcategory": "2x_goldminers_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DUST": {"category": "leveraged", "subcategory": "2x_goldminers_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "JNUG": {"category": "leveraged", "subcategory": "2x_juniorgold_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "JDST": {"category": "leveraged", "subcategory": "2x_juniorgold_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UGL": {"category": "leveraged", "subcategory": "2x_gold_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "GLL": {"category": "leveraged", "subcategory": "2x_gold_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "AGQ": {"category": "leveraged", "subcategory": "2x_silver_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "ZSL": {"category": "leveraged", "subcategory": "2x_silver_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UCO": {"category": "leveraged", "subcategory": "2x_crude_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SCO": {"category": "leveraged", "subcategory": "2x_crude_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "BOIL": {"category": "leveraged", "subcategory": "2x_natgas_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "KOLD": {"category": "leveraged", "subcategory": "2x_natgas_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ── EXPANDED: leveraged bonds & crypto ──
    "TMV": {"category": "leveraged", "subcategory": "3x_treasury_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "TYD": {"category": "leveraged", "subcategory": "3x_7_10yr_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TYO": {"category": "leveraged", "subcategory": "3x_7_10yr_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "BITX": {"category": "leveraged", "subcategory": "2x_bitcoin_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "BITI": {"category": "leveraged", "subcategory": "1x_bitcoin_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "ETHU": {"category": "leveraged", "subcategory": "2x_ethereum_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── EXPANDED: leveraged single-stock (retail conviction on hot names) ──
    "NVDL": {"category": "leveraged", "subcategory": "2x_nvda_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "NVDS": {"category": "leveraged", "subcategory": "1x_nvda_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "TSLL": {"category": "leveraged", "subcategory": "2x_tsla_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TSLQ": {"category": "leveraged", "subcategory": "1x_tsla_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "CONL": {"category": "leveraged", "subcategory": "2x_coin_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "MSTU": {"category": "leveraged", "subcategory": "2x_mstr_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── EXPANDED: thematic & sub-sector complexes ──
    "VGT": {"category": "thematic", "subcategory": "technology", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "IGV": {"category": "thematic", "subcategory": "software", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "WCLD": {"category": "thematic", "subcategory": "cloud", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "SKYY": {"category": "thematic", "subcategory": "cloud", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "XOP": {"category": "thematic", "subcategory": "oil_gas_ep", "smart_money": False, "region": "US", "ref_sector": "Energy"},
    "OIH": {"category": "thematic", "subcategory": "oil_services", "smart_money": False, "region": "US", "ref_sector": "Energy"},
    "GDX": {"category": "thematic", "subcategory": "gold_miners", "smart_money": False, "region": "US", "ref_sector": "Basic Materials"},
    "GDXJ": {"category": "thematic", "subcategory": "junior_gold_miners", "smart_money": False, "region": "US", "ref_sector": "Basic Materials"},
    "XME": {"category": "thematic", "subcategory": "metals_mining", "smart_money": False, "region": "US", "ref_sector": "Basic Materials"},
    "COPX": {"category": "thematic", "subcategory": "copper_miners", "smart_money": False, "region": "US", "ref_sector": "Basic Materials"},
    "URA": {"category": "thematic", "subcategory": "uranium", "smart_money": False, "region": "US", "ref_sector": "Energy"},
    "URNM": {"category": "thematic", "subcategory": "uranium_miners", "smart_money": False, "region": "US", "ref_sector": "Energy"},
    "ITB": {"category": "thematic", "subcategory": "homebuilders", "smart_money": False, "region": "US", "ref_sector": "Consumer Cyclical"},
    "XHB": {"category": "thematic", "subcategory": "homebuilders", "smart_money": False, "region": "US", "ref_sector": "Consumer Cyclical"},
    "JETS": {"category": "thematic", "subcategory": "airlines", "smart_money": False, "region": "US", "ref_sector": "Industrials"},
    "IYT": {"category": "thematic", "subcategory": "transports", "smart_money": False, "region": "US", "ref_sector": "Industrials"},
    "PAVE": {"category": "thematic", "subcategory": "infrastructure", "smart_money": False, "region": "US", "ref_sector": "Industrials"},
    "KBE": {"category": "thematic", "subcategory": "banks", "smart_money": False, "region": "US", "ref_sector": "Financial Services"},
    "XRT": {"category": "thematic", "subcategory": "retail", "smart_money": False, "region": "US", "ref_sector": "Consumer Cyclical"},
    "BOTZ": {"category": "thematic", "subcategory": "robotics_ai", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "ITA": {"category": "thematic", "subcategory": "aerospace_defense", "smart_money": False, "region": "US", "ref_sector": "Industrials"},
    "KARS": {"category": "thematic", "subcategory": "electric_vehicles", "smart_money": False, "region": "US", "ref_sector": "Consumer Cyclical"},
    "HACK": {"category": "thematic", "subcategory": "cybersecurity", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "CIBR": {"category": "thematic", "subcategory": "cybersecurity", "smart_money": False, "region": "US", "ref_sector": "Technology"},
    "ARKB": {"category": "thematic", "subcategory": "bitcoin", "smart_money": False, "region": "US", "ref_sector": None},
    # ── Single-stock leveraged — direct read on mega-cap / AI-leader positioning ──
    "AMDL": {"category": "leveraged", "subcategory": "2x_amd_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "AMDD": {"category": "leveraged", "subcategory": "1x_amd_bear",   "smart_money": False, "region": "US", "ref_sector": None},
    "GGLL": {"category": "leveraged", "subcategory": "2x_googl_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "AAPU": {"category": "leveraged", "subcategory": "2x_aapl_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "AAPD": {"category": "leveraged", "subcategory": "1x_aapl_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "METU": {"category": "leveraged", "subcategory": "2x_meta_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "AMZU": {"category": "leveraged", "subcategory": "2x_amzn_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "MSFU": {"category": "leveraged", "subcategory": "2x_msft_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "PLTU": {"category": "leveraged", "subcategory": "2x_pltr_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "MSTZ": {"category": "leveraged", "subcategory": "1x_mstr_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "SMCL": {"category": "leveraged", "subcategory": "2x_smci_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "AVGX": {"category": "leveraged", "subcategory": "2x_avgo_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    # ═══ EXPANSION v3 — maximal leveraged/inverse positioning coverage ═══
    # ── FANG+ / mega-cap tech 3x (the cleanest read on Big-Tech conviction) ──
    "FNGU": {"category": "leveraged", "subcategory": "3x_fangplus_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "FNGD": {"category": "leveraged", "subcategory": "3x_fangplus_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "BULZ": {"category": "leveraged", "subcategory": "3x_fang_innovation_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "BERZ": {"category": "leveraged", "subcategory": "3x_fang_innovation_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ── Broad market 1x/2x/3x (full risk-positioning ladder) ──
    "DDM":  {"category": "leveraged", "subcategory": "2x_dow_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DXD":  {"category": "leveraged", "subcategory": "2x_dow_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "DOG":  {"category": "leveraged", "subcategory": "1x_dow_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "PSQ":  {"category": "leveraged", "subcategory": "1x_qqq_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "SPUU": {"category": "leveraged", "subcategory": "2x_sp500_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "HIBL": {"category": "leveraged", "subcategory": "3x_highbeta_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "HIBS": {"category": "leveraged", "subcategory": "3x_highbeta_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "MIDU": {"category": "leveraged", "subcategory": "3x_midcap_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "MVV":  {"category": "leveraged", "subcategory": "2x_midcap_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "MZZ":  {"category": "leveraged", "subcategory": "2x_midcap_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "RWM":  {"category": "leveraged", "subcategory": "1x_smallcap_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "SAA":  {"category": "leveraged", "subcategory": "2x_smallcap_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── 2x sector (ProShares Ultra/UltraShort — granular sector lever read) ──
    "ROM":  {"category": "leveraged", "subcategory": "2x_tech_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "REW":  {"category": "leveraged", "subcategory": "2x_tech_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "USD":  {"category": "leveraged", "subcategory": "2x_semi_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SSG":  {"category": "leveraged", "subcategory": "2x_semi_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UYG":  {"category": "leveraged", "subcategory": "2x_financials_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SKF":  {"category": "leveraged", "subcategory": "2x_financials_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "DIG":  {"category": "leveraged", "subcategory": "2x_energy_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "DUG":  {"category": "leveraged", "subcategory": "2x_energy_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "RXL":  {"category": "leveraged", "subcategory": "2x_healthcare_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "BIB":  {"category": "leveraged", "subcategory": "2x_biotech_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "BIS":  {"category": "leveraged", "subcategory": "2x_biotech_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UXI":  {"category": "leveraged", "subcategory": "2x_industrials_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "UCC":  {"category": "leveraged", "subcategory": "2x_discretionary_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "URE":  {"category": "leveraged", "subcategory": "2x_realestate_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SRS":  {"category": "leveraged", "subcategory": "2x_realestate_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UYM":  {"category": "leveraged", "subcategory": "2x_materials_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SMN":  {"category": "leveraged", "subcategory": "2x_materials_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UPW":  {"category": "leveraged", "subcategory": "2x_utilities_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── Region leveraged (global risk-on/off positioning) ──
    "EDC":  {"category": "leveraged", "subcategory": "3x_em_bull", "smart_money": False, "region": "EM", "ref_sector": None},
    "EDZ":  {"category": "leveraged", "subcategory": "3x_em_bear", "smart_money": False, "region": "EM", "ref_sector": None},
    "EURL": {"category": "leveraged", "subcategory": "3x_europe_bull", "smart_money": False, "region": "Intl", "ref_sector": None},
    "INDL": {"category": "leveraged", "subcategory": "2x_india_bull", "smart_money": False, "region": "EM", "ref_sector": None},
    "BRZU": {"category": "leveraged", "subcategory": "2x_brazil_bull", "smart_money": False, "region": "EM", "ref_sector": None},
    "KORU": {"category": "leveraged", "subcategory": "3x_korea_bull", "smart_money": False, "region": "EM", "ref_sector": None},
    "MEXX": {"category": "leveraged", "subcategory": "3x_mexico_bull", "smart_money": False, "region": "EM", "ref_sector": None},
    "TPOR": {"category": "leveraged", "subcategory": "3x_transports_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── FX / dollar positioning (independent dollar-tide cross-check) ──
    "UUP":  {"category": "fx", "subcategory": "usd_bull", "smart_money": True, "region": "US", "ref_sector": None},
    "UDN":  {"category": "fx", "subcategory": "usd_bear", "smart_money": True, "region": "US", "ref_sector": None},
    "EUO":  {"category": "fx", "subcategory": "2x_euro_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "ULE":  {"category": "fx", "subcategory": "2x_euro_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "YCS":  {"category": "fx", "subcategory": "2x_yen_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ── Crypto leveraged ──
    "BITU": {"category": "leveraged", "subcategory": "2x_bitcoin_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SBIT": {"category": "leveraged", "subcategory": "2x_bitcoin_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "ETHT": {"category": "leveraged", "subcategory": "2x_ethereum_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "MSTX": {"category": "leveraged", "subcategory": "2x_mstr_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # ── Single-stock leveraged additions (bull + bear legs on hot names) ──
    "TSLR": {"category": "leveraged", "subcategory": "2x_tsla_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "TSLS": {"category": "leveraged", "subcategory": "2x_tsla_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "NVDX": {"category": "leveraged", "subcategory": "2x_nvda_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "NVDU": {"category": "leveraged", "subcategory": "2x_nvda_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "GGLS": {"category": "leveraged", "subcategory": "1x_googl_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "METD": {"category": "leveraged", "subcategory": "1x_meta_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "AMZD": {"category": "leveraged", "subcategory": "1x_amzn_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "MSFD": {"category": "leveraged", "subcategory": "1x_msft_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "CONI": {"category": "leveraged", "subcategory": "1x_coin_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # ─── EXPANSION (ops 1937): coverage-verified leveraged/inverse/thematic ───
    # Single-stock leverage — new names + completed bull/bear legs
    "MUU":  {"category": "leveraged", "subcategory": "2x_mu_bull",    "smart_money": False, "region": "US", "ref_sector": None},
    "MUD":  {"category": "leveraged", "subcategory": "2x_mu_bear",    "smart_money": False, "region": "US", "ref_sector": None},
    "NFXL": {"category": "leveraged", "subcategory": "2x_nflx_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "NFXS": {"category": "leveraged", "subcategory": "2x_nflx_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "BABX": {"category": "leveraged", "subcategory": "2x_baba_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "TSMX": {"category": "leveraged", "subcategory": "2x_tsm_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "BRKU": {"category": "leveraged", "subcategory": "2x_brk_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "CRWL": {"category": "leveraged", "subcategory": "2x_crwd_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "HOOX": {"category": "leveraged", "subcategory": "2x_hood_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "AVL":  {"category": "leveraged", "subcategory": "2x_avgo_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "AAPB": {"category": "leveraged", "subcategory": "2x_aapl_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "NVDD": {"category": "leveraged", "subcategory": "2x_nvda_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "SMCX": {"category": "leveraged", "subcategory": "2x_smci_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "PLTD": {"category": "leveraged", "subcategory": "2x_pltr_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "COII": {"category": "leveraged", "subcategory": "2x_coin_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "AMUU": {"category": "leveraged", "subcategory": "2x_amzn_bull",  "smart_money": False, "region": "US", "ref_sector": None},
    "TSLZ": {"category": "leveraged", "subcategory": "2x_tsla_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    # Broad index leverage gaps
    "URTY": {"category": "leveraged", "subcategory": "3x_r2000_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "SRTY": {"category": "leveraged", "subcategory": "3x_r2000_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UMDD": {"category": "leveraged", "subcategory": "3x_midcap_bull","smart_money": False, "region": "US", "ref_sector": None},
    "SMDD": {"category": "leveraged", "subcategory": "3x_midcap_bear","smart_money": False, "region": "US", "ref_sector": None},
    "QQQU": {"category": "leveraged", "subcategory": "2x_qqq_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "TTT":  {"category": "leveraged", "subcategory": "3x_20y_bear",   "smart_money": False, "region": "US", "ref_sector": None},
    "XLG":  {"category": "broad",     "subcategory": "us_megacap",    "smart_money": True,  "region": "US", "ref_sector": None},
    "IJH":  {"category": "broad",     "subcategory": "us_mid_cap",    "smart_money": True,  "region": "US", "ref_sector": None},
    "FTEC": {"category": "sector",    "subcategory": "technology",    "smart_money": True,  "region": "US", "ref_sector": "Technology"},
    # ProShares Ultra/UltraShort 2x sector pair completion
    "RXD":  {"category": "leveraged", "subcategory": "2x_health_bear",     "smart_money": False, "region": "US", "ref_sector": None},
    "SDP":  {"category": "leveraged", "subcategory": "2x_utilities_bear",  "smart_money": False, "region": "US", "ref_sector": None},
    "SIJ":  {"category": "leveraged", "subcategory": "2x_industrials_bear","smart_money": False, "region": "US", "ref_sector": None},
    "UGE":  {"category": "leveraged", "subcategory": "2x_staples_bull",    "smart_money": False, "region": "US", "ref_sector": None},
    "SZK":  {"category": "leveraged", "subcategory": "2x_staples_bear",    "smart_money": False, "region": "US", "ref_sector": None},
    "SCC":  {"category": "leveraged", "subcategory": "2x_discretionary_bear","smart_money": False, "region": "US", "ref_sector": None},
    "XSD":  {"category": "sector",    "subcategory": "semiconductors",     "smart_money": True,  "region": "US", "ref_sector": "Technology"},
    # Treasury 2x/3x long & short pairs
    "TBT":  {"category": "leveraged", "subcategory": "2x_20y_bear",   "smart_money": False, "region": "US", "ref_sector": None},
    "UBT":  {"category": "leveraged", "subcategory": "2x_20y_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "TBF":  {"category": "leveraged", "subcategory": "1x_20y_bear",   "smart_money": False, "region": "US", "ref_sector": None},
    "PST":  {"category": "leveraged", "subcategory": "2x_7-10y_bear", "smart_money": False, "region": "US", "ref_sector": None},
    "UST":  {"category": "leveraged", "subcategory": "2x_7-10y_bull", "smart_money": False, "region": "US", "ref_sector": None},
    # Gold miners 2x long & short
    "GDXU": {"category": "leveraged", "subcategory": "2x_goldminers_bull", "smart_money": False, "region": "US", "ref_sector": None},
    "GDXD": {"category": "leveraged", "subcategory": "2x_goldminers_bear", "smart_money": False, "region": "US", "ref_sector": None},
    # Crypto leverage gaps
    "BTCL": {"category": "leveraged", "subcategory": "2x_btc_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "ETU":  {"category": "leveraged", "subcategory": "2x_eth_bull",   "smart_money": False, "region": "US", "ref_sector": None},
    "ETHD": {"category": "leveraged", "subcategory": "2x_eth_bear",   "smart_money": False, "region": "US", "ref_sector": None},
    # China leverage gaps
    "CHAU": {"category": "leveraged", "subcategory": "2x_china_bull", "smart_money": False, "region": "CN", "ref_sector": None},
    "XPP":  {"category": "leveraged", "subcategory": "2x_china_bull", "smart_money": False, "region": "CN", "ref_sector": None},
    "FXP":  {"category": "leveraged", "subcategory": "2x_china_bear", "smart_money": False, "region": "CN", "ref_sector": None},
    # High-value thematic 1x — breadth for complexes
    "BUG":  {"category": "sector", "subcategory": "cybersecurity", "smart_money": True, "region": "US", "ref_sector": "Technology"},
    "QTUM": {"category": "sector", "subcategory": "quantum",       "smart_money": True, "region": "US", "ref_sector": "Technology"},
    "ROBO": {"category": "sector", "subcategory": "robotics",      "smart_money": True, "region": "US", "ref_sector": "Technology"},
    "MSOS": {"category": "sector", "subcategory": "cannabis",      "smart_money": True, "region": "US", "ref_sector": "Healthcare"},
    "NLR":  {"category": "sector", "subcategory": "nuclear",       "smart_money": True, "region": "US", "ref_sector": "Utilities"},
    "PPA":  {"category": "sector", "subcategory": "defense",       "smart_money": True, "region": "US", "ref_sector": "Industrials"},
    "ARKX": {"category": "sector", "subcategory": "space",         "smart_money": True, "region": "US", "ref_sector": "Industrials"},
    "PHO":  {"category": "sector", "subcategory": "water",         "smart_money": True, "region": "US", "ref_sector": "Utilities"},
}


# ═════════════════════════════════════════════════════════════════════
# Polygon API client
# ═════════════════════════════════════════════════════════════════════
def fetch_etf_flow_window(ticker: str, days: int = 100) -> dict:
    """Fetch last ~`days` of fund flow data for one ETF in a single call.

    Polygon's /etf-global/v1/fund-flows returns:
        results: [{ processed_date, effective_date, composite_ticker,
                    shares_outstanding, nav, fund_flow }, ...]
    Defaults to ASC order with limit=1 (giving us the OLDEST record).
    We pass order=desc + sort=processed_date + a date range to get the
    most recent ~90 trading days. From those we compute everything:
    latest snapshot, 5d/21d cumulative, AUM (shares*nav), z-score,
    persistence.

    Returns dict with:
      ticker, processed_date (latest), nav, shares_outstanding, aum_usd,
      daily_flow_usd, fund_flow_5d_usd, fund_flow_21d_usd, history (list)
    """
    if not POLYGON_KEY:
        return {"ticker": ticker, "error": "POLYGON_KEY not set"}
    from datetime import timedelta
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days + 10)
    url = (
        f"{ETF_FLOWS_ENDPOINT}"
        f"?composite_ticker={ticker}"
        f"&processed_date.gte={start_date.isoformat()}"
        f"&processed_date.lte={end_date.isoformat()}"
        f"&order=desc"
        f"&sort=processed_date"
        f"&limit=120"
        f"&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ETFFlows/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            if not results:
                return {"ticker": ticker, "error": "no_results",
                        "raw_status": data.get("status"),
                        "request_id": data.get("request_id")}
            # Already sorted desc by API, but be defensive
            results = sorted(
                results, key=lambda x: x.get("processed_date") or "",
                reverse=True,
            )
            latest = results[0]
            nav = _num(latest.get("nav"))
            shares = _num(latest.get("shares_outstanding"))
            aum = (nav * shares) if (nav is not None and shares is not None) else None

            # Cumulatives
            flows = [
                _num(r.get("fund_flow")) for r in results
                if r.get("fund_flow") is not None
            ]
            flow_daily = flows[0] if flows else None
            flow_5d = sum(flows[:5]) if len(flows) >= 5 else (
                sum(flows) if flows else None
            )
            flow_21d = sum(flows[:21]) if len(flows) >= 21 else (
                sum(flows) if flows else None
            )
            # Capture sample row for schema diagnostics
            sample_row = {k: v for k, v in latest.items()}
            return {
                "ticker": ticker,
                "processed_date": latest.get("processed_date"),
                "effective_date": latest.get("effective_date"),
                "nav": nav,
                "shares_outstanding": shares,
                "aum_usd": aum,
                "daily_flow_usd": flow_daily,
                "fund_flow_5d_usd": flow_5d,
                "fund_flow_21d_usd": flow_21d,
                "history": [
                    {"processed_date": r.get("processed_date"),
                     "flow": _num(r.get("fund_flow")),
                     "nav": _num(r.get("nav")),
                     "shares_outstanding": _num(r.get("shares_outstanding"))}
                    for r in results
                ],
                "raw_sample": sample_row,
                "n_history": len(results),
            }
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:300]
        except Exception:
            pass
        return {"ticker": ticker, "error": f"http_{e.code}", "body": body}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)[:200]}


def _num(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def fetch_universe_parallel() -> dict:
    """Fetch all ETFs in parallel — one call each gets snapshot + history."""
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_ticker = {
            ex.submit(fetch_etf_flow_window, t, 100): t
            for t in ETF_UNIVERSE.keys()
        }
        for fut in as_completed(future_to_ticker):
            t = future_to_ticker[fut]
            try:
                results[t] = fut.result()
            except Exception as e:
                results[t] = {"ticker": t, "error": str(e)[:200]}
    return results


# ═════════════════════════════════════════════════════════════════════
# Per-ETF analytics
# ═════════════════════════════════════════════════════════════════════
def compute_per_etf_metrics(snapshot: dict, history: list) -> dict:
    """Compute all institutional metrics for one ETF.

    Returns a dict ready for the daily snapshot. None values when
    inputs are missing (graceful degradation).
    """
    if snapshot.get("error"):
        return {**snapshot, "signal_label": "DATA_MISSING"}

    daily_flow = snapshot.get("daily_flow_usd")
    aum = snapshot.get("aum_usd")
    flow_5d = snapshot.get("fund_flow_5d_usd")
    flow_21d = snapshot.get("fund_flow_21d_usd")

    # Flow as % of AUM (normalized for size — $1B into SPY is meaningless,
    # $1B into ARKK is a 10%+ swing)
    pct_aum_daily = (daily_flow / aum * 100) if (daily_flow and aum and aum > 0) else None
    pct_aum_5d = (flow_5d / aum * 100) if (flow_5d and aum and aum > 0) else None
    pct_aum_21d = (flow_21d / aum * 100) if (flow_21d and aum and aum > 0) else None

    # If 5d/21d not in API, compute from history
    if flow_5d is None and history:
        recent_5 = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)[:5]
        flows_5 = [x["flow"] for x in recent_5 if x.get("flow") is not None]
        if flows_5:
            flow_5d = sum(flows_5)
            pct_aum_5d = (flow_5d / aum * 100) if aum and aum > 0 else None
    if flow_21d is None and history:
        recent_21 = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)[:21]
        flows_21 = [x["flow"] for x in recent_21 if x.get("flow") is not None]
        if flows_21:
            flow_21d = sum(flows_21)
            pct_aum_21d = (flow_21d / aum * 100) if aum and aum > 0 else None

    # 90-day z-score on daily flow (the workhorse signal)
    flow_zscore_90d = None
    if daily_flow is not None and history:
        hist_flows = [x["flow"] for x in history if x.get("flow") is not None]
        if len(hist_flows) >= 30:
            mean = statistics.mean(hist_flows)
            try:
                stdev = statistics.stdev(hist_flows)
                if stdev > 0:
                    flow_zscore_90d = round((daily_flow - mean) / stdev, 2)
            except statistics.StatisticsError:
                pass

    # Persistence: consecutive days in same direction
    persistence_days = 0
    if history:
        sorted_hist = sorted(history, key=lambda x: x.get("processed_date") or "", reverse=True)
        if daily_flow is not None:
            sign = 1 if daily_flow > 0 else -1
            for row in sorted_hist:
                f = row.get("flow")
                if f is None:
                    continue
                if (f > 0 and sign > 0) or (f < 0 and sign < 0):
                    persistence_days += 1
                else:
                    break

    # Signal label — based on 90d z-score thresholds
    # These thresholds are calibrated to surface real institutional moves:
    # z > 2.0 = top 2.5% of historical inflows (rare, meaningful)
    # z 1.0-2.0 = elevated inflow
    # |z| < 1.0 = normal noise
    label = "NEUTRAL"
    if flow_zscore_90d is not None:
        if flow_zscore_90d >= 2.0:
            label = "STRONG_INFLOW"
        elif flow_zscore_90d >= 1.0:
            label = "INFLOW"
        elif flow_zscore_90d <= -2.0:
            label = "STRONG_OUTFLOW"
        elif flow_zscore_90d <= -1.0:
            label = "OUTFLOW"

    # ── Flow–Price divergence layer (ops 3143) ──────────────────────
    # nav history rides along in the same fund-flows rows → 21d/5d price
    # return costs ZERO extra API calls. Divergence quadrants are the
    # institutional construct raw flows miss: inflows into weakness =
    # stealth accumulation; outflows into strength = distribution rally.
    ret_5d_pct = ret_21d_pct = None
    if history:
        navs = sorted(((x.get("processed_date") or "", x.get("nav"))
                       for x in history if x.get("nav")), reverse=True)
        nav_now = snapshot.get("nav") or (navs[0][1] if navs else None)
        if nav_now:
            if len(navs) >= 6 and navs[5][1]:
                ret_5d_pct = round((nav_now / navs[5][1] - 1) * 100, 2)
            if len(navs) >= 22 and navs[21][1]:
                ret_21d_pct = round((nav_now / navs[21][1] - 1) * 100, 2)
    quadrant, divergence_score = "NEUTRAL", None
    if flow_zscore_90d is not None and ret_21d_pct is not None:
        z, r = flow_zscore_90d, ret_21d_pct
        divergence_score = round(z * -math.tanh(r / 8.0), 2)
        if z >= 1.0 and r <= -2.0:
            quadrant = "STEALTH_ACCUMULATION"
        elif z <= -1.0 and r >= 2.0:
            quadrant = "DISTRIBUTION_RALLY"
        elif z >= 1.0 and r >= 2.0:
            quadrant = "TREND_CONFIRMED"
        elif z <= -1.0 and r <= -2.0:
            quadrant = "CAPITULATION"

    return {
        "ticker": snapshot["ticker"],
        "processed_date": snapshot.get("processed_date"),
        "category": ETF_UNIVERSE[snapshot["ticker"]]["category"],
        "subcategory": ETF_UNIVERSE[snapshot["ticker"]]["subcategory"],
        "smart_money": ETF_UNIVERSE[snapshot["ticker"]]["smart_money"],
        "region": ETF_UNIVERSE[snapshot["ticker"]]["region"],
        "ref_sector": ETF_UNIVERSE[snapshot["ticker"]]["ref_sector"],
        "daily_flow_usd": daily_flow,
        "flow_5d_usd": flow_5d,
        "flow_21d_usd": flow_21d,
        "pct_aum_daily": round(pct_aum_daily, 3) if pct_aum_daily is not None else None,
        "pct_aum_5d": round(pct_aum_5d, 2) if pct_aum_5d is not None else None,
        "pct_aum_21d": round(pct_aum_21d, 2) if pct_aum_21d is not None else None,
        "aum_usd": snapshot.get("aum_usd"),
        "flow_zscore_90d": flow_zscore_90d,
        "persistence_days": persistence_days,
        "nav": snapshot.get("nav"),
        "leveraged": bool(__import__("re").search(
            r"(^|_)([123]x|bear|bull|ultra|inverse)(_|$)",
            str(ETF_UNIVERSE[snapshot["ticker"]].get("subcategory") or ""),
            __import__("re").I)),
        "ret_5d_pct": ret_5d_pct,
        "ret_21d_pct": ret_21d_pct,
        "quadrant": quadrant,
        "divergence_score": divergence_score,
        "signal_label": label,
        "n_history_points": len(history),
    }


# ═════════════════════════════════════════════════════════════════════
# Category aggregations — the rotation signal
# ═════════════════════════════════════════════════════════════════════
def aggregate_by_category(metrics: list) -> dict:
    """Sum flows by (category, subcategory). Drives the rotation heatmap."""
    out = {}
    for m in metrics:
        if m.get("error") or not m.get("daily_flow_usd"):
            continue
        cat = m["category"]
        sub = m["subcategory"]
        for key in [cat, f"{cat}.{sub}"]:
            agg = out.setdefault(key, {
                "n_etfs": 0, "total_daily_flow_usd": 0, "total_5d_usd": 0,
                "total_21d_usd": 0, "total_aum_usd": 0,
                "etfs": [], "avg_zscore": [], "category": cat, "subcategory": sub if "." in key else None,
            })
            agg["n_etfs"] += 1
            agg["total_daily_flow_usd"] += m.get("daily_flow_usd") or 0
            agg["total_5d_usd"] += m.get("flow_5d_usd") or 0
            agg["total_21d_usd"] += m.get("flow_21d_usd") or 0
            agg["total_aum_usd"] += m.get("aum_usd") or 0
            agg["etfs"].append(m["ticker"])
            if m.get("flow_zscore_90d") is not None:
                agg["avg_zscore"].append(m["flow_zscore_90d"])
    # Finalize: compute averages
    for key, agg in out.items():
        z = agg.pop("avg_zscore", [])
        agg["avg_zscore_90d"] = round(statistics.mean(z), 2) if z else None
        agg["pct_aum_daily"] = (
            round(100 * agg["total_daily_flow_usd"] / agg["total_aum_usd"], 3)
            if agg["total_aum_usd"] > 0 else None
        )
        agg["pct_aum_5d"] = (
            round(100 * agg["total_5d_usd"] / agg["total_aum_usd"], 2)
            if agg["total_aum_usd"] > 0 else None
        )
    return out


# ═════════════════════════════════════════════════════════════════════
# COMPOSITE SIGNALS — the institutional alpha
# ═════════════════════════════════════════════════════════════════════


# ═════════════════════════════════════════════════════════════════════
# Flow–Price Divergence board + graded-signal emission (ops 3143)
# ═════════════════════════════════════════════════════════════════════
def build_divergence_board(metrics: list) -> dict:
    """Ranked stealth-accumulation vs distribution-rally boards."""
    def row(m):
        return {"ticker": m["ticker"], "category": m["category"],
                "subcategory": m["subcategory"],
                "flow_zscore_90d": m.get("flow_zscore_90d"),
                "ret_21d_pct": m.get("ret_21d_pct"),
                "pct_aum_21d": m.get("pct_aum_21d"),
                "divergence_score": m.get("divergence_score")}
    ok = [m for m in metrics if not m.get("error")
          and m.get("divergence_score") is not None
          and m.get("category") != "broad"]
    clean = [m for m in ok if not m.get("leveraged")]
    lev = [m for m in ok if m.get("leveraged")
           and m.get("quadrant") in ("STEALTH_ACCUMULATION",
                                      "DISTRIBUTION_RALLY")]
    stealth = sorted((m for m in clean
                      if m.get("quadrant") == "STEALTH_ACCUMULATION"),
                     key=lambda m: -(m.get("divergence_score") or 0))
    distro = sorted((m for m in clean
                     if m.get("quadrant") == "DISTRIBUTION_RALLY"),
                    key=lambda m: (m.get("divergence_score") or 0))
    lev.sort(key=lambda m: -abs(m.get("divergence_score") or 0))
    return {
        "method": ("z(flow,90d) vs 21d nav return; stealth = z>=+1 & "
                   "ret<=-2%; distribution = z<=-1 & ret>=+2%; score = "
                   "z * -tanh(ret/8)"),
        "n_scored": len(ok),
        "stealth_accumulation": [row(m) for m in stealth[:10]],
        "distribution_rally": [row(m) for m in distro[:10]],
        "trend_confirmed": sum(1 for m in ok
                               if m.get("quadrant") == "TREND_CONFIRMED"),
        "capitulation": sum(1 for m in ok
                            if m.get("quadrant") == "CAPITULATION"),
        "leveraged_extremes": [row(m) | {"quadrant": m.get("quadrant")}
                                for m in lev[:8]],
        "note": ("leveraged/inverse products are listed separately: their "
                 "flows are structurally contrarian (dip-buying in 2x/3x) "
                 "and would pollute the clean industry read"),
    }


def emit_divergence_signals(metrics: list, now) -> int:
    """Log the strongest divergences into the justhodl-signals table so
    outcome-checker grades them → scorecard/magdist learn whether stealth
    accumulation actually LEADS (|z| >= 1.5 emission bar)."""
    from decimal import Decimal
    logged = 0
    try:
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        cand = [m for m in metrics if not m.get("error")
                and m.get("category") != "broad"
                and not m.get("leveraged")
                and m.get("quadrant") in ("STEALTH_ACCUMULATION",
                                           "DISTRIBUTION_RALLY")
                and abs(m.get("flow_zscore_90d") or 0) >= 1.5
                and m.get("nav")]
        cand.sort(key=lambda m: -abs(m.get("divergence_score") or 0))
        for m in cand[:12]:
            up = m["quadrant"] == "STEALTH_ACCUMULATION"
            stype = "etf_stealth_accum" if up else "etf_distribution_rally"
            direction = "UP" if up else "DOWN"
            tbl.put_item(Item={
                "signal_id": f"etfdiv-{direction}#{m['ticker']}#"
                             f"{now.date().isoformat()}",
                "signal_type": stype,
                "predicted_direction": direction,
                "signal_value": str(m.get("divergence_score")),
                "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark",
                "baseline_price": str(m["nav"]),
                "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"],
                "outcomes": {}, "accuracy_scores": {},
                "status": "pending",
                "logged_at": now.isoformat(),
                "logged_epoch": int(now.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(now.timestamp()) + 120 * 86400,
                "metadata": {"engine": "etf-fund-flows",
                             "quadrant": m["quadrant"],
                             "subcategory": m.get("subcategory")},
                "rationale": (f"{m['quadrant']} {m['ticker']}: flow z "
                              f"{m.get('flow_zscore_90d')} vs 21d ret "
                              f"{m.get('ret_21d_pct')}% "
                              f"({m.get('pct_aum_21d')}% AUM/21d)"),
            })
            logged += 1
    except Exception as e:
        print(f"[etf-flows] signal emit failed: {str(e)[:120]}")
    return logged


def compute_composite_signals(metrics: list, cat_aggs: dict) -> dict:
    """6 institutional composite signals derived from per-ETF flows.

    Each is normalized to a -100 to +100 score (negative = bearish,
    positive = bullish for the named direction).
    """
    by_ticker = {m["ticker"]: m for m in metrics if not m.get("error")}

    def _z(t: str) -> Optional[float]:
        m = by_ticker.get(t, {})
        return m.get("flow_zscore_90d")

    def _avg(tickers: List[str]) -> Optional[float]:
        vals = [_z(t) for t in tickers if _z(t) is not None]
        return statistics.mean(vals) if vals else None

    def _score(positive_avg: Optional[float], negative_avg: Optional[float]) -> Optional[float]:
        """Combine two avg-z-scores into a -100..+100 composite."""
        if positive_avg is None or negative_avg is None:
            return None
        spread = positive_avg - negative_avg
        # Map spread to -100..+100 (clamp at +/-4 z which is the practical extreme)
        return round(max(-100, min(100, spread / 4.0 * 100)), 1)

    # 1. DEFENSIVE ROTATION
    # Positive = money moving to defensive sectors (XLP/XLU/XLV/TLT)
    # Negative = money flowing to cyclical (XLK/XLY/XLF/IWM)
    defensive_avg = _avg(["XLP", "XLU", "XLV", "TLT"])
    cyclical_avg = _avg(["XLK", "XLY", "XLF", "IWM"])
    defensive_rotation = _score(defensive_avg, cyclical_avg)

    # 2. SMART vs DUMB MONEY
    # Positive = smart money buying (SPY/QQQ/sector SPDRs), dumb selling
    # Negative = retail euphoria (ARKK/SOXL/leveraged inflows)
    smart_avg = _avg(["SPY", "QQQ", "IVV", "VOO", "XLK", "XLF", "XLV"])
    dumb_avg = _avg(["ARKK", "TQQQ", "SOXL", "UVXY", "KWEB"])
    smart_dumb = _score(smart_avg, dumb_avg)

    # 3. RISK-ON / RISK-OFF
    # Positive = equity inflows vs Treasury outflows = risk-on
    # Negative = Treasury inflows vs equity outflows = de-risking
    equity_avg = _avg(["SPY", "QQQ", "IWM", "EFA", "EEM"])
    treasury_avg = _avg(["TLT", "IEF", "AGG"])
    risk_on_off = _score(equity_avg, treasury_avg)

    # 4. DOMESTIC vs INTERNATIONAL
    # Positive = US equity inflows vs international outflows
    # Negative = global rotation out of US
    domestic_avg = _avg(["SPY", "QQQ", "VTI"])
    intl_avg = _avg(["EFA", "VEA", "EEM", "VWO"])
    domestic_vs_intl = _score(domestic_avg, intl_avg)

    # 5. GROWTH vs VALUE
    # Positive = growth flows
    # Negative = value rotation
    growth_avg = _avg(["VUG", "IWF", "MTUM", "QQQ"])
    value_avg = _avg(["VTV", "IWD", "VLUE"])
    growth_vs_value = _score(growth_avg, value_avg)

    # 6. CREDIT STRESS (flight-to-quality detector)
    # Positive = IG inflows + HY outflows = stress (flight to quality)
    # Negative = HY inflows + IG outflows = risk appetite healthy
    ig_avg = _avg(["LQD", "AGG", "TIP"])
    hy_avg = _avg(["HYG", "JNK", "EMB"])
    credit_stress = _score(ig_avg, hy_avg)

    # Overall regime suggestion based on composites
    # If defensive_rotation > 30 AND risk_on_off < -30 → DEFENSIVE
    # If both > 30 → bullish (smart money in growth)
    # etc.
    regime = "NEUTRAL"
    if defensive_rotation is not None and risk_on_off is not None:
        if defensive_rotation >= 30 and risk_on_off <= -30:
            regime = "DEFENSIVE"
        elif defensive_rotation <= -30 and risk_on_off >= 30:
            regime = "RISK_ON"
        elif credit_stress is not None and credit_stress >= 40:
            regime = "CREDIT_STRESS"
        elif abs(defensive_rotation) < 15 and abs(risk_on_off) < 15:
            regime = "NEUTRAL"
        else:
            regime = "TRANSITION"

    return {
        "defensive_rotation": {
            "score": defensive_rotation,
            "label": "DEFENSIVE INFLOWS" if (defensive_rotation or 0) >= 30 else ("CYCLICAL INFLOWS" if (defensive_rotation or 0) <= -30 else "MIXED"),
            "components": {"defensive_avg_z": defensive_avg, "cyclical_avg_z": cyclical_avg},
        },
        "smart_vs_dumb": {
            "score": smart_dumb,
            "label": "SMART MONEY BUYING" if (smart_dumb or 0) >= 30 else ("RETAIL EUPHORIA" if (smart_dumb or 0) <= -30 else "MIXED"),
            "components": {"smart_avg_z": smart_avg, "dumb_avg_z": dumb_avg},
        },
        "risk_on_off": {
            "score": risk_on_off,
            "label": "RISK-ON" if (risk_on_off or 0) >= 30 else ("DE-RISKING" if (risk_on_off or 0) <= -30 else "MIXED"),
            "components": {"equity_avg_z": equity_avg, "treasury_avg_z": treasury_avg},
        },
        "domestic_vs_intl": {
            "score": domestic_vs_intl,
            "label": "US PREFERRED" if (domestic_vs_intl or 0) >= 30 else ("INTL ROTATION" if (domestic_vs_intl or 0) <= -30 else "MIXED"),
        },
        "growth_vs_value": {
            "score": growth_vs_value,
            "label": "GROWTH BID" if (growth_vs_value or 0) >= 30 else ("VALUE ROTATION" if (growth_vs_value or 0) <= -30 else "MIXED"),
        },
        "credit_stress": {
            "score": credit_stress,
            "label": "FLIGHT TO QUALITY" if (credit_stress or 0) >= 30 else ("RISK APPETITE HEALTHY" if (credit_stress or 0) <= -30 else "MIXED"),
        },
        "regime": regime,
    }


# ═════════════════════════════════════════════════════════════════════
# Per-ticker context — for research/critique prompt injection
# ═════════════════════════════════════════════════════════════════════
def build_per_ticker_context(metrics: list, composite: dict) -> dict:
    """Build {ticker: flow_context} lookup for ALL tickers in research universe.

    Each entry includes:
      - sector_etf, sector_etf_flow_label, sector_etf_zscore (sector context)
      - market_regime (the composite regime tag)
      - smart_money_signal (from smart_vs_dumb composite)
      - prompt_snippet: a 2-3 sentence string ready to inject into Claude

    We attempt to look up each research ticker's GICS sector → find the
    matching XL* sector ETF in our universe → pull its flow context.
    """
    sector_to_etf = {
        "Technology":             "XLK",
        "Financial Services":     "XLF",
        "Energy":                 "XLE",
        "Healthcare":             "XLV",
        "Consumer Defensive":     "XLP",
        "Consumer Cyclical":      "XLY",
        "Industrials":            "XLI",
        "Basic Materials":        "XLB",
        "Utilities":              "XLU",
        "Real Estate":            "XLRE",
        "Communication Services": "XLC",
    }

    by_ticker = {m["ticker"]: m for m in metrics if not m.get("error")}
    regime = composite.get("regime", "NEUTRAL")
    smart_dumb_label = composite.get("smart_vs_dumb", {}).get("label", "MIXED")
    risk_on_off_label = composite.get("risk_on_off", {}).get("label", "MIXED")

    # We don't know research-universe tickers here; we generate context per
    # SECTOR and a small lookup for known tickers can be built downstream.
    # We DO provide a sector-level lookup that any ticker can use.
    per_sector = {}
    for sector_name, etf in sector_to_etf.items():
        m = by_ticker.get(etf)
        if not m:
            continue
        # Generate a 2-3 sentence prompt snippet for Claude
        flow_label = m.get("signal_label", "NEUTRAL")
        z = m.get("flow_zscore_90d")
        pct_5d = m.get("pct_aum_5d")
        persistence = m.get("persistence_days")
        flow_5d_b = (m.get("flow_5d_usd") or 0) / 1e9  # to billions

        # Compose the snippet
        if z is None:
            snippet = f"Sector ETF {etf} ({sector_name}): flow data unavailable."
        else:
            direction = "inflow" if z > 0 else "outflow"
            magnitude = (
                "extreme " if abs(z) >= 2.0 else
                "elevated " if abs(z) >= 1.0 else
                "modest "
            )
            persist_str = f" over {persistence} consecutive days" if persistence and persistence >= 3 else ""
            snippet = (
                f"Sector ETF {etf} ({sector_name}): {magnitude}{direction} "
                f"(z={z}σ vs 90-day baseline, 5d cumulative {flow_5d_b:+.2f}B, "
                f"{pct_5d:+.2f}% of AUM{persist_str}). "
                f"Market regime: {regime}. Risk posture: {risk_on_off_label}. Smart money: {smart_dumb_label}."
            )

        per_sector[sector_name] = {
            "sector_etf": etf,
            "flow_label": flow_label,
            "flow_zscore_90d": z,
            "flow_5d_usd": m.get("flow_5d_usd"),
            "pct_aum_5d": pct_5d,
            "persistence_days": persistence,
            "regime": regime,
            "prompt_snippet": snippet,
        }
    return {
        "by_sector": per_sector,
        "global_regime": regime,
        "smart_vs_dumb_label": smart_dumb_label,
        "risk_on_off_label": risk_on_off_label,
    }


# ═════════════════════════════════════════════════════════════════════
# S3 writers
# ═════════════════════════════════════════════════════════════════════
def _write_json(key: str, obj: dict, cache_ttl: int = 600):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(obj, default=str).encode(),
        ContentType="application/json",
        CacheControl=f"public, max-age={cache_ttl}",
    )


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def build_event_study(snapshots):
    """Frazzini–Lamont test on OUR OWN tape (ops 3344): flow-spike events
    → forward NAV excess vs SPY. Event = daily flow z >= |2| against a
    rolling 60d baseline, 10-trading-day cooldown (non-overlapping-ish).
    Forward windows 5d & 21d, benchmark-adjusted with SPY NAV over the
    SAME dates. Quadrants at event time (trailing 21d NAV return):
    inflow+falling = stealth_accum, inflow+rising = chase,
    outflow+rising = distribution, outflow+falling = capitulation.
    Leveraged/volatility categories excluded (decay makes NAV excess
    meaningless). History = the engine's rolling ~120d window, so n is
    honest but modest; date-stamped history/ snapshots deepen v2."""
    spy_nav, bench = {}, None
    for cand in ("SPY", "IVV", "VOO", "QQQ"):
        snap_b = snapshots.get(cand) or {}
        m = {}
        for r in (snap_b.get("history") or []):
            d, n = r.get("processed_date"), r.get("nav")
            if d and n and n > 0:
                m[str(d)[:10]] = n
        if len(m) >= 80:
            spy_nav, bench = m, cand
            break
    if not bench:
        return {"event_study": {"error": "no benchmark with >=80 NAV days",
                                "probe": {c: len([r for r in ((snapshots.get(c) or {}).get("history") or [])
                                                  if r.get("nav")]) for c in ("SPY", "IVV", "VOO", "QQQ")}}}

    events = []
    for tk, snap in snapshots.items():
        if tk == bench or snap.get("error"):
            continue
        cat = (ETF_UNIVERSE.get(tk) or {}).get("category")
        if cat in ("leveraged", "volatility"):
            continue
        arr = sorted([{"d": str(r.get("processed_date"))[:10],
                       "f": r.get("flow"), "nav": r.get("nav")}
                      for r in (snap.get("history") or [])
                      if r.get("processed_date") and r.get("nav")],
                     key=lambda x: x["d"])
        if len(arr) < 90:
            continue
        last_ev = -99
        for t in range(60, len(arr) - 21):
            if t - last_ev < 10:
                continue
            base = [x["f"] for x in arr[t - 60:t] if x["f"] is not None]
            ft = arr[t]["f"]
            if ft is None or len(base) < 40:
                continue
            mu = statistics.mean(base)
            try:
                sd = statistics.stdev(base)
            except statistics.StatisticsError:
                continue
            if not sd or sd <= 0:
                continue
            z = (ft - mu) / sd
            if abs(z) < 2.0:
                continue
            n0, n5, n21, nb21 = arr[t]["nav"], arr[t + 5]["nav"], arr[t + 21]["nav"], arr[t - 21]["nav"]
            s0, s5, s21 = spy_nav.get(arr[t]["d"]), spy_nav.get(arr[t + 5]["d"]), spy_nav.get(arr[t + 21]["d"])
            if not all((n0, n5, n21, nb21, s0, s5, s21)):
                continue
            fwd5, fwd21 = n5 / n0 - 1, n21 / n0 - 1
            ex5 = fwd5 - (s5 / s0 - 1)
            ex21 = fwd21 - (s21 / s0 - 1)
            trail21 = n0 / nb21 - 1
            direction = "inflow" if z > 0 else "outflow"
            quad = ("stealth_accum" if (z > 0 and trail21 < 0) else
                    "chase" if z > 0 else
                    "distribution" if trail21 > 0 else "capitulation")
            events.append({"ticker": tk, "date": arr[t]["d"], "z": round(z, 2),
                           "dir": direction, "quadrant": quad,
                           "trail21_pct": round(trail21 * 100, 2),
                           "ex5_bps": round(ex5 * 1e4), "ex21_bps": round(ex21 * 1e4),
                           "smart": bool((ETF_UNIVERSE.get(tk) or {}).get("smart_money")),
                           "category": cat})
            last_ev = t

    def _agg(rows):
        if not rows:
            return {"n": 0}
        e5 = [r["ex5_bps"] for r in rows]
        e21 = [r["ex21_bps"] for r in rows]
        return {"n": len(rows),
                "hit5": round(100.0 * sum(1 for v in e5 if v > 0) / len(e5), 1),
                "hit21": round(100.0 * sum(1 for v in e21 if v > 0) / len(e21), 1),
                "med_ex5_bps": round(statistics.median(e5)),
                "med_ex21_bps": round(statistics.median(e21))}

    study = {"benchmark": bench,
             "method": ("z>=|2| on rolling 60d flow baseline, 10d cooldown, "
                        "forward 5d/21d NAV return minus SPY, leveraged/vol excluded; "
                        "rolling ~120d engine window — n grows via history/ snapshots"),
             "n_events": len(events),
             "overall": _agg(events),
             "by_dir": {d: _agg([r for r in events if r["dir"] == d])
                        for d in ("inflow", "outflow")},
             "by_quadrant": {q: _agg([r for r in events if r["quadrant"] == q])
                             for q in ("stealth_accum", "chase", "distribution", "capitulation")},
             "smart_money": _agg([r for r in events if r["smart"]]),
             "retail_favored": _agg([r for r in events if not r["smart"]]),
             "top_events": sorted(events, key=lambda r: -abs(r["ex21_bps"]))[:12]}
    return {"event_study": study}


def build_leveraged_appetite(metrics):
    """Bull vs bear 5d flows inside the leveraged complex — pure retail
    risk-appetite dial. vol_short counts bull; vol_long counts bear."""
    def _side(sub):
        s = sub or ""
        if "bear" in s or s == "vol_long":
            return "bear"
        if "bull" in s or s == "vol_short":
            return "bull"
        return None
    pairs, bull, bear, suspects = {}, 0.0, 0.0, []
    for m in metrics:
        tk = m.get("ticker")
        u = ETF_UNIVERSE.get(tk) or {}
        if u.get("category") != "leveraged":
            continue
        side = _side(u.get("subcategory"))
        if not side:
            continue
        f = m.get("flow_5d_usd")
        if f is None:
            f = m.get("fund_flow_5d_usd")
        if f is None:
            continue
        aum = m.get("aum_usd") or 0
        if abs(f) > max(5e9, 0.5 * aum):  # data-suspect for a leveraged fund
            suspects.append({"t": tk, "f5d": round(f), "aum": round(aum)})
            continue
        root = (u.get("subcategory") or "").replace("_bull", "").replace("_bear", "")             .replace("vol_long", "vol").replace("vol_short", "vol")
        p = pairs.setdefault(root, {"pair": root, "bull_5d": 0.0, "bear_5d": 0.0, "tickers": []})
        p[side + "_5d"] += f
        p["tickers"].append({"t": tk, "side": side, "f5d": f})
        if side == "bull":
            bull += f
        else:
            bear += f
    net = bull - bear
    thr = max(1.5e8, 0.10 * (abs(bull) + abs(bear)))
    read = "RISK_SEEKING" if net > thr else "RISK_AVERSE" if net < -thr else "NEUTRAL"
    return {"bull_5d_usd": round(bull), "bear_5d_usd": round(bear),
            "net_5d_usd": round(net), "read": read,
            "n_suspect_excluded": len(suspects), "suspects": suspects[:8],
            "pairs": sorted(pairs.values(), key=lambda p: -(abs(p["bull_5d"]) + abs(p["bear_5d"])))[:20]}


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[etf-flows] starting at {datetime.now(timezone.utc).isoformat()}")
    print(f"[etf-flows] universe: {len(ETF_UNIVERSE)} ETFs")

    # 1. Parallel fetch: one call per ETF returns latest snapshot + 100d history
    print("[etf-flows] phase 1: fetching 100-day windows for all ETFs...")
    snapshots = fetch_universe_parallel()
    n_ok = sum(1 for s in snapshots.values() if not s.get("error"))
    print(f"[etf-flows] got data for {n_ok}/{len(ETF_UNIVERSE)} ETFs")

    # 2. Compute per-ETF metrics (history is already inside each snapshot)
    print("[etf-flows] phase 2: computing per-ETF analytics...")
    metrics = [
        compute_per_etf_metrics(snapshots[t], snapshots[t].get("history", []) or [])
        for t in ETF_UNIVERSE.keys()
    ]

    # 4. Category aggregations
    print("[etf-flows] phase 3: category aggregations...")
    category_aggs = aggregate_by_category(metrics)

    # 5. Composite signals (the alpha)
    print("[etf-flows] phase 4: computing composite signals...")
    composite = compute_composite_signals(metrics, category_aggs)
    print(f"[etf-flows] regime: {composite.get('regime')}")

    # 6. Per-ticker context (for prompt injection)
    print("[etf-flows] phase 4c: event study + leveraged appetite (ops 3344)...")
    event_study = build_event_study(snapshots)
    composite["leveraged_appetite"] = build_leveraged_appetite(metrics)
    print(f"[etf-flows] event-study n={event_study.get('event_study', {}).get('n_events')} "
          f"lev-appetite={composite['leveraged_appetite']['read']}")

    print("[etf-flows] phase 5: building per-ticker context...")
    per_ticker = build_per_ticker_context(metrics, composite)

    # 5b. Flow-Price divergence board + graded-signal emission (ops 3143)
    print("[etf-flows] phase 4b: divergence board + signal emission...")
    divergence = build_divergence_board(metrics)
    _emit_now = datetime.now(timezone.utc)
    signals_logged = emit_divergence_signals(metrics, _emit_now)
    print(f"[etf-flows] divergence: {len(divergence['stealth_accumulation'])}"
          f" stealth / {len(divergence['distribution_rally'])} distro / "
          f"{signals_logged} signals logged")

    elapsed = round(time.time() - t0, 1)

    # 7. Write outputs to S3
    print("[etf-flows] phase 6: writing 5 S3 outputs...")
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(ETF_UNIVERSE),
        "n_ok": n_ok,
        "n_failed": len(ETF_UNIVERSE) - n_ok,
        "elapsed_s": elapsed,
        "schema_version": "1.0",
    }

    # 7a. Daily full snapshot
    _write_json(f"{OUTPUT_PREFIX}daily.json", {**meta, "metrics": metrics})

    # 7b. Composite signals
    _write_json(f"{OUTPUT_PREFIX}composite.json",
                {**meta, "composite": composite,
                 "divergence_board": divergence,
                 "divergence_signals_logged": signals_logged})

    # 7b2. Flow event-study (ops 3344)
    _write_json(f"{OUTPUT_PREFIX}event-study.json", {**meta, **event_study})

    # 7c. Rotation matrix (category aggregates)
    _write_json(f"{OUTPUT_PREFIX}rotation.json",
                {**meta, "by_category": category_aggs,
                 "divergence_board": divergence})

    # 7d. Per-ticker context for prompt injection
    _write_json(f"{OUTPUT_PREFIX}per-ticker-context.json", {**meta, "context": per_ticker})

    # 7e. Historical archive (date-stamped)
    archive_key = f"{OUTPUT_PREFIX}history/{today_iso}.json"
    _write_json(archive_key, {**meta, "metrics": metrics, "composite": composite}, cache_ttl=86400)

    print(f"[etf-flows] DONE in {elapsed}s")

    # Summary
    inflow_top = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
    )[:5]
    outflow_top = sorted(
        [m for m in metrics if m.get("flow_zscore_90d") is not None],
        key=lambda x: x["flow_zscore_90d"] or 0
    )[:5]

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_etfs_ok": n_ok,
            "regime": composite.get("regime"),
            "composite_summary": {k: v.get("label") for k, v in composite.items() if isinstance(v, dict) and "label" in v},
            "top_5_inflows": [{"ticker": m["ticker"], "z": m.get("flow_zscore_90d"), "label": m.get("signal_label")} for m in inflow_top],
            "top_5_outflows": [{"ticker": m["ticker"], "z": m.get("flow_zscore_90d"), "label": m.get("signal_label")} for m in outflow_top],
            "outputs_written": [
                f"{OUTPUT_PREFIX}daily.json",
                f"{OUTPUT_PREFIX}composite.json",
                f"{OUTPUT_PREFIX}rotation.json",
                f"{OUTPUT_PREFIX}per-ticker-context.json",
                archive_key,
            ],
        }, default=str),
    }
