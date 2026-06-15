"""Finviz Elite export toolkit — whole-universe screener pulls merged by ticker.

One authenticated call per view returns the entire US universe (~11.3k rows) as CSV.
Token in SSM /justhodl/finviz/auth-token (SecureString). Bundled into every Lambda
via aws/shared, so any engine can `import finviz as FV`.

Primary entry points:
  FV.build_universe()        -> {ticker: {merged fields}}  (live pull, all views)
  FV.fetch_view("ownership") -> [ {raw col: val}, ... ]     (single view, live)
  FV.load_universe()         -> cached data/finviz-universe.json by_ticker
  FV.load_short()            -> cached data/finviz-short.json (slim short-float index)
"""
import csv
import io
import json
import urllib.request
import urllib.error
import boto3

_s3 = boto3.client("s3", region_name="us-east-1")
_ssm = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
UNIVERSE_KEY = "data/finviz-universe.json"
SHORT_KEY = "data/finviz-short.json"
BASE = "https://elite.finviz.com/export.ashx"

VIEWS = {"overview": "111", "valuation": "121", "ownership": "131",
         "performance": "141", "technical": "171"}

_TOKEN = None


def _token():
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = _ssm.get_parameter(Name="/justhodl/finviz/auth-token",
                                    WithDecryption=True)["Parameter"]["Value"]
    return _TOKEN


def _num(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if s in ("", "-", "NaN", "null"):
        return None
    pct = s.endswith("%")
    if pct:
        s = s[:-1]
    mult = 1.0
    if s and s[-1] in "BMK":
        mult = {"B": 1e9, "M": 1e6, "K": 1e3}[s[-1]]
        s = s[:-1]
    try:
        return round(float(s) * mult, 4)
    except Exception:
        return None


def _txt(x):
    s = (x or "").strip()
    return s or None


# raw Finviz CSV column name -> (clean_key, parser)
COLMAP = {
    "Ticker": ("ticker", _txt), "Company": ("company", _txt), "Sector": ("sector", _txt),
    "Industry": ("industry", _txt), "Country": ("country", _txt),
    "Market Cap": ("market_cap", _num), "P/E": ("pe", _num), "Forward P/E": ("fwd_pe", _num),
    "PEG": ("peg", _num), "P/S": ("ps", _num), "P/B": ("pb", _num),
    "P/Cash": ("p_cash", _num), "P/Free Cash Flow": ("p_fcf", _num),
    "EPS Growth This Year": ("eps_growth_ty", _num), "EPS Growth Next Year": ("eps_growth_ny", _num),
    "EPS Growth Past 5 Years": ("eps_growth_5y", _num), "Sales Growth Past 5 Years": ("sales_growth_5y", _num),
    "Shares Outstanding": ("shares_out", _num), "Shares Float": ("float_shares", _num),
    "Short Float": ("short_float_pct", _num), "Short Ratio": ("short_ratio", _num),
    "Insider Ownership": ("insider_own_pct", _num), "Insider Transactions": ("insider_trans_pct", _num),
    "Institutional Ownership": ("inst_own_pct", _num), "Institutional Transactions": ("inst_trans_pct", _num),
    "Average Volume": ("avg_volume", _num), "Relative Volume": ("rel_volume", _num),
    "Beta": ("beta", _num), "Average True Range": ("atr", _num),
    "20-Day Simple Moving Average": ("sma20_pct", _num), "50-Day Simple Moving Average": ("sma50_pct", _num),
    "200-Day Simple Moving Average": ("sma200_pct", _num),
    "52-Week High": ("off_52w_high_pct", _num), "52-Week Low": ("off_52w_low_pct", _num),
    "Relative Strength Index (14)": ("rsi", _num),
    "Performance (Week)": ("perf_w", _num), "Performance (Month)": ("perf_m", _num),
    "Performance (Quarter)": ("perf_q", _num), "Performance (Half Year)": ("perf_h", _num),
    "Performance (YTD)": ("perf_ytd", _num), "Performance (Year)": ("perf_y", _num),
    "Volatility (Week)": ("volatility_w", _num), "Volatility (Month)": ("volatility_m", _num),
    "Analyst Recom": ("analyst_recom", _num), "Target Price": ("target_price", _num),
    "Price": ("price", _num), "Change": ("change_pct", _num), "Volume": ("volume", _num),
    # --- expanded to full 72-column custom surface ---
    "Dividend Yield": ("div_yield", _num), "Payout Ratio": ("payout_ratio", _num),
    "EPS (ttm)": ("eps_ttm", _num),
    "EPS Growth Next 5 Years": ("eps_growth_n5y", _num),
    "EPS Growth Quarter Over Quarter": ("eps_growth_qoq", _num),
    "Sales Growth Quarter Over Quarter": ("sales_growth_qoq", _num),
    "Return on Assets": ("roa", _num), "Return on Equity": ("roe", _num),
    "Return on Invested Capital": ("roic", _num),
    "Current Ratio": ("current_ratio", _num), "Quick Ratio": ("quick_ratio", _num),
    "LT Debt/Equity": ("lt_debt_eq", _num), "Total Debt/Equity": ("debt_eq", _num),
    "Gross Margin": ("gross_margin", _num), "Operating Margin": ("oper_margin", _num),
    "Profit Margin": ("profit_margin", _num),
    "50-Day High": ("off_50d_high_pct", _num), "50-Day Low": ("off_50d_low_pct", _num),
    "Change from Open": ("change_open_pct", _num), "Gap": ("gap_pct", _num),
    "Earnings Date": ("earnings_date", _txt), "IPO Date": ("ipo_date", _txt),
    "After-Hours Close": ("ah_close", _num),
}


def fetch_view(view, filt=None, timeout=60):
    """Return list of raw-CSV dict rows for one Finviz view across the (filtered) universe."""
    v = VIEWS.get(view, view)
    url = "%s?v=%s%s&auth=%s" % (BASE, v, ("&f=" + filt) if filt else "", _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    return list(csv.DictReader(io.StringIO(body)))


CUSTOM_COLS = ",".join(str(i) for i in range(72))  # full 72-column surface


def fetch_custom(filt=None, cols=CUSTOM_COLS, timeout=90):
    """One authenticated custom-column export = ALL fields for the whole (filtered) universe in a single call."""
    url = "%s?v=152&c=%s%s&auth=%s" % (BASE, cols, ("&f=" + filt) if filt else "", _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    head = body.split("\n", 1)[0]
    if "Ticker" not in head:
        raise RuntimeError("finviz custom export non-CSV (auth/tier/rate-limit): " + head[:80])
    return list(csv.DictReader(io.StringIO(body)))


def build_universe(filt=None, **_legacy):
    """Whole (filtered) universe as clean keyed records from ONE custom-column export call
    (replaces the old 5-view merge: complete 72-field surface, fewer API hits, no rate-limit)."""
    uni = {}
    try:
        rows = fetch_custom(filt)
    except Exception as e:
        print("[finviz] custom export failed, falling back to views: %s" % str(e)[:90])
        rows = []
        for v in ("overview", "ownership", "technical", "performance", "valuation"):
            try:
                rows.extend(fetch_view(v, filt))
            except Exception as e2:
                print("[finviz] view %s failed: %s" % (v, str(e2)[:80]))
    for raw in rows:
        tk = (raw.get("Ticker") or "").strip().upper()
        if not tk:
            continue
        rec = uni.setdefault(tk, {"ticker": tk})
        for rawk, val in raw.items():
            m = COLMAP.get(rawk.strip())
            if not m or m[0] == "ticker":
                continue
            key, parse = m
            pv = parse(val)
            if pv is not None and rec.get(key) in (None, ""):
                rec[key] = pv
    return uni


def load_universe():
    """Cached full universe (by_ticker) from S3."""
    try:
        return json.loads(_s3.get_object(Bucket=BUCKET, Key=UNIVERSE_KEY)["Body"].read()).get("by_ticker", {})
    except Exception:
        return {}


def load_short():
    """Cached slim short-float index from S3: {ticker: {short_float_pct, short_ratio, float_shares, rel_volume, avg_volume}}."""
    try:
        return json.loads(_s3.get_object(Bucket=BUCKET, Key=SHORT_KEY)["Body"].read()).get("by_ticker", {})
    except Exception:
        return {}
