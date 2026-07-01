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
    # ── extended columns (Finviz exposes 151; these are the high-value adds) ──
    "Index": ("index_membership", _txt),
    "EPS Surprise": ("eps_surprise", _num), "Revenue Surprise": ("rev_surprise", _num),
    "Short Interest": ("short_interest_sh", _num), "Float %": ("float_pct", _num),
    "All-Time High": ("off_ath_pct", _num), "All-Time Low": ("off_atl_pct", _num),
    "Enterprise Value": ("ev", _num), "EV/EBITDA": ("ev_ebitda", _num), "EV/Sales": ("ev_sales", _num),
    "Income": ("income", _num), "Sales": ("sales", _num),
    "Book/sh": ("book_sh", _num), "Cash/sh": ("cash_sh", _num), "Employees": ("employees", _num),
    "Dividend TTM": ("div_ttm", _num), "Dividend Ex Date": ("div_ex_date", _txt),
    "EPS Year Over Year TTM": ("eps_yoy_ttm", _num), "Sales Year Over Year TTM": ("sales_yoy_ttm", _num),
    "EPS Growth Past 3 Years": ("eps_g_3y", _num), "Sales Growth Past 3 Years": ("sales_g_3y", _num),
    "EPS Next Q": ("eps_next_q", _num), "52-Week Range": ("range_52w", _txt), "Exchange": ("exchange", _txt),
    "Optionable": ("optionable", _txt), "Shortable": ("shortable", _txt), "Prev Close": ("prev_close", _num),
    "Asset Type": ("asset_type", _txt), "ETF Type": ("etf_type", _txt),
    "Net Expense Ratio": ("expense_ratio", _num), "Total Holdings": ("n_holdings", _num),
    "Assets Under Management": ("aum", _num), "Net Asset Value": ("nav", _num),
    "Net Flows (1 Month)": ("flows_1m", _num), "Net Flows % (1 Month)": ("flows_1m_pct", _num),
    "Net Flows (3 Month)": ("flows_3m", _num), "Net Flows (YTD)": ("flows_ytd", _num),
    "Net Flows (1 Year)": ("flows_1y", _num),
    "Return 1 Year": ("ret_1y", _num), "Return 3 Year": ("ret_3y", _num), "Return 5 Year": ("ret_5y", _num),
}


def fetch_view(view, filt=None, timeout=60):
    """Return list of raw-CSV dict rows for one Finviz view across the (filtered) universe."""
    v = VIEWS.get(view, view)
    url = "%s?v=%s%s&auth=%s" % (BASE, v, ("&f=" + filt) if filt else "", _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    return list(csv.DictReader(io.StringIO(body)))


CUSTOM_COLS = ",".join(str(i) for i in range(151))  # full 151-column custom surface



_COLMAP_NORM = None
def _cmap(rawk):
    """Header-normalized COLMAP lookup (custom-export headers can differ in case/spacing)."""
    global _COLMAP_NORM
    if _COLMAP_NORM is None:
        _COLMAP_NORM = {k.strip().lower(): v for k, v in COLMAP.items()}
    return _COLMAP_NORM.get((rawk or "").strip().lower())

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
            m = _cmap(rawk)
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


def fetch_screen(qs, view="152", cols="1,2,3,6,43,46,63,65,66,67,61", timeout=40):
    """Generic screen pull for signals: qs is a raw fragment like 's=ta_newhigh' or 'f=ta_sma50_cross200a'.
    Default custom cols = Ticker,Company,Sector,MktCap,PerfMonth,PerfYTD,RelVol,Price,Change,Volume,Recom.
    Returns slim list[dict] of normalized rows. Caller should space calls (Finviz 429s on rapid bursts)."""
    url = "%s?v=%s&c=%s&%s&auth=%s" % (BASE, view, cols, qs, _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    head = body.split("\n", 1)[0]
    if "Ticker" not in head:
        raise RuntimeError("finviz screen non-CSV (auth/rate-limit): " + head[:80])
    out = []
    for raw in csv.DictReader(io.StringIO(body)):
        tk = (raw.get("Ticker") or "").strip().upper()
        if not tk:
            continue
        rec = {"ticker": tk}
        for rawk, val in raw.items():
            m = COLMAP.get(rawk)
            if not m or m[0] == "ticker":
                continue
            pv = m[1](val)
            if pv is not None:
                rec[m[0]] = pv
        out.append(rec)
    return out


GROUP_BASE = "https://elite.finviz.com/grp_export.ashx"
GROUP_COLMAP = {
    "Name": ("name", _txt),
    "Performance (Week)": ("perf_w", _num), "Performance (Month)": ("perf_m", _num),
    "Performance (Quarter)": ("perf_q", _num), "Performance (Half Year)": ("perf_h", _num),
    "Performance (Year)": ("perf_y", _num), "Performance (Year To Date)": ("perf_ytd", _num),
    "Average Volume": ("avg_volume", _num), "Relative Volume": ("rel_volume", _num),
    "Change": ("change", _num), "Volume": ("volume", _num),
    "Market Cap": ("mktcap", _num), "P/E": ("pe", _num), "Forward P/E": ("fwd_pe", _num),
    "PEG": ("peg", _num), "P/S": ("ps", _num), "P/B": ("pb", _num), "P/C": ("p_cash", _num),
    "P/Free Cash Flow": ("p_fcf", _num), "EPS growth past 5 years": ("eps_g_5y", _num),
    "EPS growth next 5 years": ("eps_g_n5y", _num), "Sales growth past 5 years": ("sales_g_5y", _num),
}


def fetch_group(g, v=140, timeout=40):
    """Finviz group aggregates. g in {sector,industry,country,capitalization}; v=140 perf / v=120 valuation.
    Returns list of {name, ...normalized}. Space calls (Finviz 429s on rapid bursts)."""
    url = "%s?g=%s&v=%s&auth=%s" % (GROUP_BASE, g, v, _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    if "Name" not in body.split("\n", 1)[0]:
        raise RuntimeError("finviz group non-CSV: " + body[:80])
    out = []
    for raw in csv.DictReader(io.StringIO(body)):
        rec = {}
        for k, val in raw.items():
            m = GROUP_COLMAP.get(k.strip())
            if m:
                pv = m[1](val)
                if pv is not None:
                    rec[m[0]] = pv
        if rec.get("name"):
            out.append(rec)
    return out


NEWS_BASE = "https://elite.finviz.com/news_export.ashx"


def fetch_news(v=3, timeout=40):
    """Finviz news (v=3) / blogs (v=4) export. Returns list of {title,source,date,url,category,ticker}."""
    url = "%s?v=%s&auth=%s" % (NEWS_BASE, v, _token())
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "ignore")
    if "Title" not in body.split("\n", 1)[0]:
        raise RuntimeError("finviz news non-CSV: " + body[:80])
    out = []
    for raw in csv.DictReader(io.StringIO(body)):
        out.append({"title": (raw.get("Title") or "").strip(),
                    "source": (raw.get("Source") or "").strip(),
                    "date": (raw.get("Date") or "").strip(),
                    "url": (raw.get("Url") or "").strip(),
                    "category": (raw.get("Category") or "").strip(),
                    "ticker": (raw.get("Ticker") or "").strip().upper()})
    return out
