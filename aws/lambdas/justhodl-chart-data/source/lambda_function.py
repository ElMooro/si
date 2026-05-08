"""
justhodl-chart-data — Universal historical chart data API.

Single Lambda URL endpoint that serves time-series data from multiple sources:
  • FRED (1947+ for most series, fully historical)
  • ECB SDMX direct (CISS, SovCISS, CLIFS, ILM)
  • Polygon stocks/ETFs (back to 2003)
  • Internal S3 composites (Khalid Index, Plumbing, etc.)
  • OFR primary dealer fails (back to 2015)

USAGE
─────
GET /?series=DGS10&from=1990-01-01&to=2026-05-07&freq=daily
GET /?series=SPY&kind=stock&from=2000-01-01
GET /?series=khalid_index&kind=internal
GET /?multi=DGS10,DGS2,VIXCLS&from=2020-01-01

RESPONSE
────────
{
  "series_id": "DGS10",
  "label": "10-Year Treasury",
  "source": "FRED",
  "freq": "daily",
  "n_obs": 8754,
  "from": "1990-01-01",
  "to": "2026-05-07",
  "data": [
    {"time": "2026-05-07", "value": 4.32},
    ...
  ]
}

ZERO-IMPACT
───────────
✓ Read-only. Does not modify any existing data.
✓ No EventBridge schedule (called on-demand from chart-pro.html).
✓ No conflict with existing Lambdas — separate function, separate URL.
✓ Reserved concurrency=10 (chart users = bursty but bounded).
"""
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

S3 = boto3.client("s3", region_name=REGION)

ALLOWED_ORIGINS = ["https://justhodl.ai", "https://www.justhodl.ai", "http://localhost"]


def cors_headers(origin):
    """Return non-CORS response headers only.
    
    CORS headers are set by AWS Lambda URL CORS config — we MUST NOT
    duplicate them here, because browsers reject responses with multiple
    Access-Control-Allow-Origin headers per the CORS spec.
    """
    return {
        "content-type": "application/json",
        "cache-control": "public, max-age=300",
    }


def http_get(url, timeout=20, retries=2, headers=None):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers=headers or {"User-Agent": "JustHodl chart-data/1.0"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
            else:
                print(f"[chart] HTTP fail {url[:80]}: {e}")
    return None


# ─── FRED ────────────────────────────────────────────────────────────────────
def fetch_fred(series_id, start_date=None, end_date=None, frequency=None):
    """Pull from FRED with optional date filtering and frequency aggregation."""
    params = {
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "sort_order": "asc", "limit": 100000,
    }
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date
    if frequency in ("d", "w", "m", "q", "a"):
        params["frequency"] = frequency
        params["aggregation_method"] = "avg"

    url = f"https://api.stlouisfed.org/fred/series/observations?{urllib.parse.urlencode(params)}"
    body = http_get(url, timeout=30)
    if not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    obs = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v and v != ".":
            try:
                obs.append({"time": o["date"], "value": float(v)})
            except ValueError:
                continue
    return obs


# ─── ECB SDMX ────────────────────────────────────────────────────────────────
ECB_DATASETS = {
    "ciss_us":   ("CISS", "D.US.Z0Z.4F.EC.SS_CI.IDX"),
    "ciss_ea":   ("CISS", "D.U2.Z0Z.4F.EC.SS_CIN.IDX"),
    "ciss_cn":   ("CISS", "D.CN.Z0Z.4F.EC.SS_CIN.IDX"),
    "ciss_de":   ("CISS", "D.DE.Z0Z.4F.EC.SS_CIN.IDX"),
    "ciss_fr":   ("CISS", "D.FR.Z0Z.4F.EC.SS_CIN.IDX"),
    "ciss_it":   ("CISS", "D.IT.Z0Z.4F.EC.SS_CIN.IDX"),
    "ciss_gb":   ("CISS", "D.GB.Z0Z.4F.EC.SS_CI.IDX"),
    "sovciss_de": ("CISS", "M.DE.Z0Z.4F.EC.SOV_CI.IDX"),
    "sovciss_fr": ("CISS", "M.FR.Z0Z.4F.EC.SOV_CI.IDX"),
    "sovciss_it": ("CISS", "M.IT.Z0Z.4F.EC.SOV_CI.IDX"),
    "sovciss_es": ("CISS", "M.ES.Z0Z.4F.EC.SOV_CI.IDX"),
    "clifs_de":  ("CLIFS", "M.DE._Z.4F.EC.CLIFS_CI.IDX"),
    "ilm_claims_fx":  ("ILM", "W.U2.C.A030000.U2.Z06"),
    "ilm_liab_eur":   ("ILM", "W.U2.C.L060000.U4.EUR"),
}


def normalize_ecb_date(date_str):
    """Convert ECB SDMX date formats to ISO YYYY-MM-DD.
    
    ECB returns dates in multiple formats depending on series frequency:
      - Daily:    "2026-05-07"        (already ISO)
      - Weekly:   "2026-W18"          (ISO week of year)  
      - Monthly:  "2026-04"           (year-month)
      - Quarterly:"2026-Q2"           (year-quarter)
      - Annual:   "2026"              (year only)
    
    Lightweight Charts requires real ISO dates so we convert all to that.
    """
    if not date_str:
        return date_str
    s = str(date_str).strip()
    # Daily: YYYY-MM-DD
    if len(s) == 10 and s[4] == '-' and s[7] == '-':
        return s
    # Weekly: YYYY-Www (ISO week)
    if 'W' in s.upper() and 'Q' not in s.upper():
        try:
            year_str, wk_str = s.upper().split('-W')
            year = int(year_str)
            week = int(wk_str)
            # ISO week 1 is the week containing Jan 4
            from datetime import date, timedelta
            jan4 = date(year, 1, 4)
            week_1_monday = jan4 - timedelta(days=jan4.weekday())
            target = week_1_monday + timedelta(weeks=week - 1)
            return target.strftime('%Y-%m-%d')
        except Exception:
            return s
    # Quarterly: YYYY-Qq → first day of quarter
    if 'Q' in s.upper():
        try:
            year_str, q_str = s.upper().split('-Q')
            year = int(year_str)
            q = int(q_str)
            month = (q - 1) * 3 + 1
            return f"{year:04d}-{month:02d}-01"
        except Exception:
            return s
    # Monthly: YYYY-MM
    if len(s) == 7 and s[4] == '-':
        return s + "-01"
    # Annual: YYYY
    if len(s) == 4 and s.isdigit():
        return s + "-01-01"
    return s


def fetch_ecb(series_key):
    """Direct ECB SDMX API fetch. Returns observations chronological.
    
    All dates are normalized to ISO YYYY-MM-DD via normalize_ecb_date()
    so they render correctly in Lightweight Charts (which can't parse
    ISO week format like '2026-W18').
    """
    if series_key not in ECB_DATASETS:
        print(f"[chart] ECB unknown series_key: {series_key}")
        return None
    dataset, key = ECB_DATASETS[series_key]
    url = (f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}"
            "?format=jsondata&detail=dataonly")
    body = http_get(url, timeout=25, headers={
        "User-Agent": "JustHodl chart-data/1.0",
        "Accept": "application/json",
    })
    if not body:
        print(f"[chart] ECB empty body for {series_key} ({dataset}/{key})")
        return None
    try:
        d = json.loads(body)
    except Exception as e:
        print(f"[chart] ECB JSON parse fail {series_key}: {e}")
        return None

    try:
        datasets = d.get("dataSets") or d.get("dataset") or []
        if not datasets:
            return None
        ds = datasets[0]
        series_dict = ds.get("series") or {}
        first_series = next(iter(series_dict.values()), None)
        if not first_series:
            return None
        obs_dict = first_series.get("observations") or {}

        struct = d.get("structure", {})
        obs_dims = struct.get("dimensions", {}).get("observation", [])
        time_dim = next((dim for dim in obs_dims if dim.get("id") == "TIME_PERIOD"), None)
        if not time_dim:
            return None
        time_values = time_dim.get("values", [])

        obs = []
        for idx_str, vals in obs_dict.items():
            try:
                idx = int(idx_str)
                if idx >= len(time_values) or not vals or vals[0] is None:
                    continue
                raw_date = time_values[idx].get("id") or time_values[idx].get("name", "")
                iso_date = normalize_ecb_date(raw_date)
                obs.append({"time": iso_date, "value": float(vals[0])})
            except (ValueError, TypeError, IndexError):
                continue
        obs.sort(key=lambda o: o["time"])
        print(f"[chart] ECB {series_key}: {len(obs)} obs ({obs[0]['time'] if obs else '?'} → {obs[-1]['time'] if obs else '?'})")
        return obs
    except Exception as e:
        print(f"[chart] ECB parse fail {series_key}: {e}")
        return None


# ─── Polygon crypto (BTC, ETH, SOL etc) ──────────────────────────────────────
def fetch_polygon_crypto(ticker, start_date=None, end_date=None):
    """Crypto pairs via Polygon. Tickers must start with 'X:' (e.g., X:BTCUSD)."""
    if not ticker.upper().startswith("X:"):
        ticker = "X:" + ticker.upper()
    if not start_date:
        start_date = "2014-01-01"  # Most major crypto starts ~2014-2017
    if not end_date:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/1/day/"
            f"{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    body = http_get(url, timeout=30)
    if not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    if d.get("status") not in ("OK", "DELAYED") or not d.get("results"):
        print(f"[chart] Polygon crypto {ticker}: status={d.get('status')} count={d.get('resultsCount')}")
        return None
    obs = []
    for bar in d["results"]:
        try:
            ts = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc)
            obs.append({"time": ts.strftime("%Y-%m-%d"), "value": float(bar["c"])})
        except (KeyError, ValueError, TypeError):
            continue
    return obs


# ─── Polygon stocks/ETFs ─────────────────────────────────────────────────────
def fetch_polygon_stock(ticker, start_date=None, end_date=None):
    """Daily aggregate bars from Polygon.
    
    Free tier: ~2 years history. Premium: full history.
    """
    if not start_date:
        start_date = "2003-01-01"
    if not end_date:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/1/day/"
            f"{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    body = http_get(url, timeout=30)
    if not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    if d.get("status") not in ("OK", "DELAYED") or not d.get("results"):
        return None
    obs = []
    for bar in d["results"]:
        try:
            ts = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc)
            obs.append({"time": ts.strftime("%Y-%m-%d"), "value": float(bar["c"])})
        except (KeyError, ValueError, TypeError):
            continue
    return obs


# ─── OFR Primary Dealer fails ────────────────────────────────────────────────
def fetch_ofr(mnemonic):
    url = f"https://data.financialresearch.gov/v1/series/timeseries?mnemonic={mnemonic}"
    body = http_get(url, timeout=20)
    if not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    if isinstance(d, list):
        obs = []
        for pair in d:
            if isinstance(pair, list) and len(pair) >= 2 and pair[1] is not None:
                try:
                    obs.append({"time": pair[0], "value": float(pair[1])})
                except (ValueError, TypeError):
                    continue
        return obs
    return None


# ─── Internal S3 composites (read history fields if present) ─────────────────
INTERNAL_SERIES_MAP = {
    "khalid_index": ("data/khalid-index.json", "history", "score"),
    "ka_score": ("data/khalid-index.json", "history", "score"),
    "macro_nowcast": ("data/macro-nowcast-v2.json", "history", "score"),
    "eurodollar_stress": ("data/eurodollar-stress.json", None, "composite_score"),
    "plumbing_stress": ("data/plumbing-stress.json", None, "composite_score"),
    "auction_crisis": ("data/auction-crisis.json", "history", "crisis_score"),
    "compound_signals": ("data/compound-signals.json", "history", "score"),
    "yield_curve": ("data/yield-curve.json", "history", "value"),
}


def fetch_internal(series_id):
    """Pull from internal S3 composite. Tries history array first, falls back to single-point."""
    if series_id not in INTERNAL_SERIES_MAP:
        return None
    s3_key, history_field, value_field = INTERNAL_SERIES_MAP[series_id]
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=s3_key)
        d = json.loads(obj["Body"].read())
    except Exception:
        return None

    if history_field and history_field in d and isinstance(d[history_field], list):
        obs = []
        for entry in d[history_field]:
            t = entry.get("date") or entry.get("ts") or entry.get("time")
            v = entry.get(value_field)
            if t and v is not None:
                try:
                    # Normalize ISO timestamps to date-only
                    if "T" in str(t):
                        t = str(t).split("T")[0]
                    obs.append({"time": str(t), "value": float(v)})
                except (ValueError, TypeError):
                    continue
        if obs:
            obs.sort(key=lambda o: o["time"])
            return obs
    # Single-point fallback (current value only)
    v = d.get(value_field)
    t = d.get("as_of") or d.get("date") or datetime.now(timezone.utc).isoformat()
    if "T" in str(t):
        t = str(t).split("T")[0]
    if v is not None:
        try:
            return [{"time": str(t), "value": float(v)}]
        except (ValueError, TypeError):
            return None
    return None


# ─── Series catalog (for the picker UI) ──────────────────────────────────────
CATALOG = {
    "rates_curves": {"label": "Rates & Yield Curve", "series": [
        ("DGS10", "10-Year Treasury Yield", "FRED"),
        ("DGS2", "2-Year Treasury Yield", "FRED"),
        ("DGS3MO", "3-Month Treasury Yield", "FRED"),
        ("DGS30", "30-Year Treasury Yield", "FRED"),
        ("T10Y2Y", "10Y-2Y Curve", "FRED"),
        ("T10Y3M", "10Y-3M Curve", "FRED"),
        ("DFF", "Effective Federal Funds Rate", "FRED"),
        ("DGS5", "5-Year Treasury", "FRED"),
        ("DGS1", "1-Year Treasury", "FRED"),
    ]},
    "credit": {"label": "Credit Spreads", "series": [
        ("BAMLH0A0HYM2", "ICE BofA HY Composite Spread", "FRED"),
        ("BAMLC0A0CM", "ICE BofA IG Composite Spread", "FRED"),
        ("BAMLH0A1HYBB", "ICE BofA HY BB", "FRED"),
        ("BAMLH0A2HYB", "ICE BofA HY B", "FRED"),
        ("BAMLH0A3HYC", "ICE BofA HY CCC", "FRED"),
        ("BAMLC0A1CAAA", "ICE BofA IG AAA", "FRED"),
        ("BAMLC0A4CBBB", "ICE BofA IG BBB", "FRED"),
        ("BAMLEMCBPIOAS", "ICE BofA EM Corporate Spread", "FRED"),
    ]},
    "equity_indices": {"label": "Equity Indices & ETFs", "series": [
        ("SPY", "S&P 500 ETF", "Polygon"),
        ("QQQ", "Nasdaq 100 ETF", "Polygon"),
        ("IWM", "Russell 2000 ETF", "Polygon"),
        ("DIA", "Dow Jones ETF", "Polygon"),
        ("XLF", "Financial Sector ETF", "Polygon"),
        ("XLE", "Energy Sector ETF", "Polygon"),
        ("XLK", "Technology Sector ETF", "Polygon"),
        ("GLD", "Gold ETF", "Polygon"),
        ("TLT", "20Y+ Treasury ETF", "Polygon"),
        ("VXX", "VIX Short-Term Futures ETF", "Polygon"),
        ("IBIT", "Bitcoin ETF (BlackRock)", "Polygon"),
    ]},
    "crypto": {"label": "Cryptocurrencies", "series": [
        ("X:BTCUSD", "Bitcoin (BTC/USD)", "Crypto"),
        ("X:ETHUSD", "Ethereum (ETH/USD)", "Crypto"),
        ("X:SOLUSD", "Solana (SOL/USD)", "Crypto"),
        ("X:XRPUSD", "XRP (XRP/USD)", "Crypto"),
        ("X:DOGEUSD", "Dogecoin (DOGE/USD)", "Crypto"),
        ("X:AVAXUSD", "Avalanche (AVAX/USD)", "Crypto"),
        ("X:LINKUSD", "Chainlink (LINK/USD)", "Crypto"),
        ("X:ADAUSD", "Cardano (ADA/USD)", "Crypto"),
    ]},
    "volatility": {"label": "Volatility Indices", "series": [
        ("VIXCLS", "VIX (S&P 500 IV)", "FRED"),
        ("VXNCLS", "VXN (Nasdaq IV)", "FRED"),
        ("RVXCLS", "RVX (Russell 2000 IV)", "FRED"),
        ("VXDCLS", "VXD (Dow IV)", "FRED"),
        ("GVZCLS", "GVZ (Gold IV)", "FRED"),
        ("OVXCLS", "OVX (Oil IV)", "FRED"),
        ("VXVCLS", "3M VIX Term Structure", "FRED"),
    ]},
    "dollar": {"label": "Dollar Indices", "series": [
        ("DTWEXBGS", "Broad Dollar (Nominal)", "FRED"),
        ("RTWEXBGS", "Broad Dollar (Real)", "FRED"),
        ("DTWEXEMEGS", "EM Dollar (Nominal)", "FRED"),
        ("RTWEXEMEGS", "EM Dollar (Real)", "FRED"),
        ("DTWEXAFEGS", "AFE Dollar (Nominal)", "FRED"),
        ("RTWEXAFEGS", "AFE Dollar (Real)", "FRED"),
    ]},
    "liquidity": {"label": "Fed Liquidity & Repo", "series": [
        ("WALCL", "Fed Balance Sheet Total", "FRED"),
        ("RRPONTSYD", "Reverse Repo (RRP)", "FRED"),
        ("WTREGEN", "Treasury General Account (TGA)", "FRED"),
        ("SOFR", "SOFR Overnight Rate", "FRED"),
        ("EFFR", "Effective Fed Funds Rate", "FRED"),
        ("RPONTSYD", "Repo Operations", "FRED"),
        ("SWP1690", "Fed Liquidity Swaps 16-90d", "FRED"),
        ("STLFSI4", "St Louis Fed FSI v4", "FRED"),
        ("NFCI", "Chicago Fed NFCI", "FRED"),
    ]},
    "european_stress": {"label": "ECB Stress (CISS / SovCISS)", "series": [
        ("ciss_us", "ECB CISS US", "ECB"),
        ("ciss_ea", "ECB CISS Euro Area", "ECB"),
        ("ciss_cn", "ECB CISS China", "ECB"),
        ("ciss_de", "ECB CISS Germany", "ECB"),
        ("ciss_fr", "ECB CISS France", "ECB"),
        ("ciss_it", "ECB CISS Italy", "ECB"),
        ("ciss_gb", "ECB CISS UK", "ECB"),
        ("sovciss_de", "SovCISS Germany", "ECB"),
        ("sovciss_fr", "SovCISS France", "ECB"),
        ("sovciss_it", "SovCISS Italy", "ECB"),
        ("ilm_claims_fx", "ECB Claims in Foreign Currency", "ECB"),
        ("ilm_liab_eur", "ECB Liabilities in EUR (non-EA)", "ECB"),
    ]},
    "ofr_primary_dealer": {"label": "OFR Primary Dealer Stats", "series": [
        ("NYPD-PD_AFtD_TOT-A", "PD Aggregate Fails to Deliver: Total", "OFR"),
        ("NYPD-PD_AFtR_T-A", "PD Aggregate Fails to Receive: Treasury", "OFR"),
    ]},
    "labor": {"label": "Labor Market", "series": [
        ("UNRATE", "Unemployment Rate", "FRED"),
        ("ICSA", "Initial Jobless Claims", "FRED"),
        ("PAYEMS", "Total Nonfarm Payrolls", "FRED"),
        ("UEMP27OV", "Unemployed 27+ Weeks", "FRED"),
        ("LNS13025699", "Job Losers Not on Layoff", "FRED"),
        ("TEMPHELPS", "Temp Help Services Employment", "FRED"),
        ("USPBS", "Professional/Business Services", "FRED"),
        ("JTSJOL", "Job Openings (JOLTS)", "FRED"),
        ("JTSQUR", "Quits Rate", "FRED"),
    ]},
    "inflation": {"label": "Inflation", "series": [
        ("CPIAUCSL", "CPI All Items", "FRED"),
        ("CPILFESL", "Core CPI", "FRED"),
        ("PCE", "PCE", "FRED"),
        ("PCEPILFE", "Core PCE", "FRED"),
        ("T5YIE", "5-Year Breakeven Inflation", "FRED"),
        ("T10YIE", "10-Year Breakeven Inflation", "FRED"),
    ]},
    "macro": {"label": "Macro / Activity", "series": [
        ("GDP", "Real GDP", "FRED"),
        ("INDPRO", "Industrial Production", "FRED"),
        ("MCUMFN", "Manufacturing Capacity Utilization", "FRED"),
        ("HOUST", "Housing Starts", "FRED"),
        ("MORTGAGE30US", "30-Year Mortgage Rate", "FRED"),
        ("RSAFS", "Retail Sales", "FRED"),
        ("BOGZ1FL663067003Q", "Broker-Dealer Margin Loans", "FRED"),
    ]},
    "internal": {"label": "JustHodl Composites", "series": [
        ("khalid_index", "Khalid Index (Composite)", "Internal"),
        ("macro_nowcast", "Macro Nowcast Score", "Internal"),
        ("eurodollar_stress", "Eurodollar Stress Composite", "Internal"),
        ("plumbing_stress", "Plumbing & Stress Composite", "Internal"),
        ("auction_crisis", "Treasury Auction Crisis Score", "Internal"),
    ]},
    "recession": {"label": "Recession Indicator", "series": [
        ("USREC", "NBER Recession Indicator", "FRED"),
    ]},
}


# Build catalog-aware source lookup at module load time
_SOURCE_LOOKUP = {}
for cat in CATALOG.values():
    for sid, label, source in cat["series"]:
        s = source.lower()
        if "polygon" in s:
            _SOURCE_LOOKUP[sid] = "stock"
        elif "crypto" in s:
            _SOURCE_LOOKUP[sid] = "crypto"
        elif "fred" in s:
            _SOURCE_LOOKUP[sid] = "fred"
        elif "ecb" in s:
            _SOURCE_LOOKUP[sid] = "ecb"
        elif "ofr" in s:
            _SOURCE_LOOKUP[sid] = "ofr"
        elif "internal" in s:
            _SOURCE_LOOKUP[sid] = "internal"


# ─── Universal series resolver ───────────────────────────────────────────────
def fetch_series(series_id, kind=None, start_date=None, end_date=None):
    """Auto-route to the right fetcher based on series_id pattern or kind hint.
    
    Resolution order:
      1. Explicit kind param wins
      2. Catalog lookup (covers all known indicators)
      3. Pattern heuristics
    """
    if not kind:
        if series_id in _SOURCE_LOOKUP:
            kind = _SOURCE_LOOKUP[series_id]
        elif series_id.upper().startswith("X:"):
            kind = "crypto"
        elif series_id.startswith("ciss_") or series_id.startswith("sovciss_") \
             or series_id.startswith("clifs_") or series_id.startswith("ilm_"):
            kind = "ecb"
        elif series_id.startswith("NYPD-"):
            kind = "ofr"
        elif series_id in INTERNAL_SERIES_MAP:
            kind = "internal"
        else:
            kind = "fred"

    if kind == "fred":
        return fetch_fred(series_id, start_date, end_date), "FRED"
    if kind == "stock":
        return fetch_polygon_stock(series_id, start_date, end_date), "Polygon"
    if kind == "crypto":
        return fetch_polygon_crypto(series_id, start_date, end_date), "Crypto"
    if kind == "ecb":
        return fetch_ecb(series_id), "ECB"
    if kind == "ofr":
        return fetch_ofr(series_id), "OFR"
    if kind == "internal":
        return fetch_internal(series_id), "Internal"
    return None, "Unknown"


# ─── Lambda handler ──────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()
    origin = (event.get("headers") or {}).get("origin", "")
    headers = cors_headers(origin)

    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method == "OPTIONS":
        return {"statusCode": 204, "headers": headers, "body": ""}

    qs = event.get("queryStringParameters") or {}
    path = event.get("rawPath", "/")

    # Catalog endpoint
    if path == "/catalog" or qs.get("catalog") == "1":
        return {
            "statusCode": 200, "headers": headers,
            "body": json.dumps({"catalog": CATALOG}),
        }

    series_param = qs.get("series") or qs.get("multi", "")
    if not series_param:
        return {
            "statusCode": 400, "headers": headers,
            "body": json.dumps({"error": "Missing 'series' param. Use ?series=DGS10 or ?multi=DGS10,DGS2"}),
        }

    series_list = [s.strip() for s in series_param.split(",") if s.strip()]
    kind_hint = qs.get("kind")
    start = qs.get("from") or qs.get("start")
    end = qs.get("to") or qs.get("end")

    # Multi-series fetch in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=6) as exe:
        futures = {
            exe.submit(fetch_series, sid, kind_hint, start, end): sid
            for sid in series_list[:10]  # cap at 10 to prevent abuse
        }
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                data, source = fut.result()
                if data:
                    results[sid] = {
                        "series_id": sid, "source": source,
                        "n_obs": len(data), "data": data,
                    }
                else:
                    results[sid] = {"series_id": sid, "source": source, "error": "no data"}
            except Exception as e:
                results[sid] = {"series_id": sid, "error": str(e)[:100]}

    duration_s = round(time.time() - started, 2)

    # If single series, return flat. Multi → keyed.
    if len(series_list) == 1:
        body = results[series_list[0]]
        body["duration_s"] = duration_s
    else:
        body = {"series": results, "n": len(results), "duration_s": duration_s}

    return {
        "statusCode": 200, "headers": headers,
        "body": json.dumps(body, default=str),
    }
