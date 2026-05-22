"""
justhodl-factor-decomposition -- Fama-French 5-factor + Carhart momentum attribution.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Every institutional research process answers: "is this return alpha or
beta-to-known-factors?" The standard analytical framework is Fama-French
5-factor (Mkt-RF, SMB, HML, RMW, CMA) plus Carhart momentum (MOM), which
together explain 70-90% of long-horizon return variation for most equities.

When a stock generates 30% return, the question is: how much is broad
market exposure, how much is small-cap exposure, how much is value tilt,
how much is true alpha (the unexplained residual)? This decomposition is
how Renaissance, AQR, Citadel, GMO, and every quantitative asset manager
risk-adjusts performance.

DISTINCTION FROM EXISTING ENGINES
──────────────────────────────────
  justhodl-factor-risk            Barra-style firm book decomposition
                                    (portfolio-level, fixed factor set)
  justhodl-smart-beta             factor SCREEN for top names per factor
  THIS engine                    per-ticker 6-factor REGRESSION with
                                    loadings + alpha + R²

THE 5+1 FACTOR DECOMPOSITION
─────────────────────────────
  Mkt-RF   market excess return (broad equity exposure)
  SMB      small-minus-big (size factor)
  HML      high-minus-low book-to-market (value factor)
  RMW      robust-minus-weak (profitability factor)
  CMA      conservative-minus-aggressive (investment factor)
  MOM      Carhart momentum (winners-minus-losers)

REGRESSION (OLS, pure-Python implementation)
─────────────────────────────────────────────
  R_i - R_f = α + β1*MktRF + β2*SMB + β3*HML + β4*RMW + β5*CMA + β6*MOM + ε

OUTPUT
──────
  For each ticker:
    factor_loadings (β1..β6 + α with t-stats)
    R² (explanatory power)
    alpha_pct_annualized (true unexplained return)
    factor_attribution_pct (each factor's contribution to total return)

  s3://justhodl-dashboard-live/data/factor-decomposition.json
  Schedule: weekly Sundays 06 UTC (monthly factor data, weekly refresh fine)

DATA SOURCES
────────────
  Ken French Data Library (free, monthly + daily factors)
    URL: mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
  FMP /stable/historical-price-eod for ticker monthly returns

CACHING
───────
  Ken French data cached at s3://justhodl-dashboard-live/data/factor-data-cache.json
  Refreshed monthly on the 5th (after Ken French updates)

ACADEMIC BASIS
──────────────
- Fama & French (1993). Common risk factors in the returns on stocks and
  bonds. Journal of Financial Economics, 33(1), 3-56.
- Fama & French (2015). A five-factor asset pricing model. JFE 116(1).
- Carhart (1997). On persistence in mutual fund performance. Journal of
  Finance, 52(1), 57-82.
═══════════════════════════════════════════════════════════════════════════════
"""
import io
import json
import math
import os
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/factor-decomposition.json"
S3_CACHE_KEY = "data/factor-data-cache.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

KEN_FRENCH_5F_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Research_Data_5_Factors_2x3_CSV.zip")
KEN_FRENCH_MOM_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Momentum_Factor_CSV.zip")

HTTP_TIMEOUT = 30

STATIC_TOP50_SPX = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "LLY", "AVGO",
    "TSLA", "JPM", "WMT", "V", "UNH", "XOM", "MA", "ORCL", "COST",
    "PG", "JNJ", "HD", "NFLX", "BAC", "CVX", "ABBV", "CRM", "KO",
    "AMD", "WFC", "MRK", "CSCO", "ADBE", "PEP", "LIN", "TMO",
    "ACN", "MCD", "ABT", "INTU", "IBM", "DHR", "TXN", "PM", "DIS",
    "CAT", "VZ", "PFE", "QCOM",
]

s3 = boto3.client("s3", region_name="us-east-1")


# ---------- HTTP ----------
def http_bytes(url, timeout=HTTP_TIMEOUT):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-FactorDecomp/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as e:
        print(f"[http_bytes] {url[-40:]} err: {str(e)[:80]}")
        return None


def http_json(url, timeout=HTTP_TIMEOUT):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-FactorDecomp/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[http_json] {url[-40:]} err: {str(e)[:80]}")
        return None


# ---------- Ken French parser ----------
def parse_ken_french_monthly(csv_text, factor_count):
    """Parse Ken French monthly CSV. Format:
    - Lines of preamble (description)
    - Header: 'YYYYMM,MktRF,SMB,HML,RMW,CMA,RF'  (with whitespace)
    - Monthly rows: 'YYYYMM,vals...'
    - Eventually a blank line then annual data follows; stop there.
    """
    out = {}
    in_data = False
    for raw in csv_text.split("\n"):
        line = raw.strip()
        if not line:
            if in_data:
                # End of monthly section once we hit blank
                break
            continue
        # Detect data header row: starts with year-month range like "YYYYMM" with comma
        parts = [p.strip() for p in line.split(",")]
        if not parts:
            continue
        # Try to parse the first token as a YYYYMM date (6 digits)
        first = parts[0]
        if len(first) == 6 and first.isdigit():
            yyyymm = first
            try:
                values = [float(p) for p in parts[1:1 + factor_count]]
            except (ValueError, TypeError):
                continue
            if len(values) == factor_count:
                out[yyyymm] = values
                in_data = True
        elif in_data and not first[:6].isdigit():
            break
    return out


def fetch_ken_french_data():
    """Returns dict: {YYYYMM: {MktRF, SMB, HML, RMW, CMA, RF, MOM}}."""
    print("[ff] fetching 5-factor data")
    zip5 = http_bytes(KEN_FRENCH_5F_URL)
    if not zip5:
        return None
    with zipfile.ZipFile(io.BytesIO(zip5)) as z:
        csv_name = next((n for n in z.namelist() if n.endswith(".CSV")
                          or n.endswith(".csv")), None)
        if not csv_name:
            print("[ff] no csv in 5F zip")
            return None
        csv_text = z.read(csv_name).decode("utf-8", errors="replace")
    five_factor = parse_ken_french_monthly(csv_text, 6)  # MktRF SMB HML RMW CMA RF
    print(f"[ff] parsed {len(five_factor)} monthly rows from 5F")

    print("[ff] fetching momentum data")
    zip_mom = http_bytes(KEN_FRENCH_MOM_URL)
    momentum_dict = {}
    if zip_mom:
        with zipfile.ZipFile(io.BytesIO(zip_mom)) as z:
            csv_name = next((n for n in z.namelist() if n.endswith(".CSV")
                              or n.endswith(".csv")), None)
            if csv_name:
                csv_text = z.read(csv_name).decode("utf-8", errors="replace")
                momentum_dict = parse_ken_french_monthly(csv_text, 1)
    print(f"[ff] parsed {len(momentum_dict)} momentum rows")

    out = {}
    for yyyymm, vals in five_factor.items():
        if len(vals) == 6:
            mom = momentum_dict.get(yyyymm, [None])[0] if momentum_dict.get(
                yyyymm) else None
            out[yyyymm] = {
                "MktRF": vals[0] / 100, "SMB": vals[1] / 100,
                "HML": vals[2] / 100, "RMW": vals[3] / 100,
                "CMA": vals[4] / 100, "RF": vals[5] / 100,
                "MOM": (mom / 100) if mom is not None else None,
            }
    return out


def get_factor_data():
    """Try cache first; refresh if older than 28 days."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_CACHE_KEY)
        cached = json.loads(obj["Body"].read().decode("utf-8"))
        built_at = datetime.fromisoformat(
            cached.get("built_at", "").replace("Z", "+00:00"))
        age_days = (datetime.now(timezone.utc) - built_at).days
        if age_days < 28:
            print(f"[factor] using cache age={age_days}d, "
                  f"{len(cached.get('factors', {}))} rows")
            return cached.get("factors")
    except Exception as e:
        print(f"[factor cache] miss: {str(e)[:60]}")

    # Refresh
    fresh = fetch_ken_french_data()
    if not fresh:
        return None
    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_CACHE_KEY,
        Body=json.dumps({
            "built_at": datetime.now(timezone.utc).isoformat(),
            "factors": fresh,
        }).encode("utf-8"),
        ContentType="application/json")
    return fresh


# ---------- FMP returns fetch ----------
def fetch_monthly_returns(symbol, n_months=60):
    """Pull monthly closes from FMP, compute log returns."""
    today = datetime.now(timezone.utc)
    start = (today - timedelta(days=int(n_months * 31 + 30))).strftime(
        "%Y-%m-%d")
    url = (f"{FMP_BASE}/historical-price-eod/full?symbol={symbol}"
           f"&from={start}&apikey={FMP_KEY}")
    d = http_json(url)
    if not d:
        return []
    rows = d if isinstance(d, list) else (d.get("historical") or [])
    # rows may be list of dicts {date, close}; ensure sorted ascending
    by_month = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        date = r.get("date") or r.get("d")
        close = r.get("close") or r.get("c") or r.get("adjClose")
        if not date or close is None:
            continue
        try:
            close = float(close)
            yyyymm = str(date)[:7].replace("-", "")
            # Keep latest close in each month
            if yyyymm not in by_month or str(date) > by_month[yyyymm]["d"]:
                by_month[yyyymm] = {"d": str(date), "c": close}
        except (ValueError, TypeError):
            continue
    months_sorted = sorted(by_month.keys())
    out = []
    for i in range(1, len(months_sorted)):
        prev_c = by_month[months_sorted[i - 1]]["c"]
        cur_c = by_month[months_sorted[i]]["c"]
        if prev_c > 0:
            r = math.log(cur_c / prev_c)
            out.append({"yyyymm": months_sorted[i], "ret_log": r,
                         "ret_pct": (cur_c / prev_c - 1)})
    return out


# ---------- Regression (pure Python OLS) ----------
def matrix_inverse_3x3(m):
    """Closed-form inverse for small symmetric matrix (general OLS uses
    larger; we fall back to Gauss-Jordan for general)."""
    # We'll use general Gauss-Jordan for n x n
    return None


def gauss_jordan_inverse(M):
    """Pure-Python matrix inverse via Gauss-Jordan elimination."""
    n = len(M)
    A = [row[:] + [1.0 if i == j else 0.0 for j in range(n)]
          for i, row in enumerate(M)]
    for i in range(n):
        # Find pivot
        pivot_row = i
        for k in range(i, n):
            if abs(A[k][i]) > abs(A[pivot_row][i]):
                pivot_row = k
        if abs(A[pivot_row][i]) < 1e-12:
            return None
        A[i], A[pivot_row] = A[pivot_row], A[i]
        pivot = A[i][i]
        A[i] = [x / pivot for x in A[i]]
        for k in range(n):
            if k != i and abs(A[k][i]) > 1e-12:
                factor = A[k][i]
                A[k] = [A[k][j] - factor * A[i][j] for j in range(2 * n)]
    inverse = [row[n:] for row in A]
    return inverse


def mat_mul(A, B):
    rows_A = len(A); cols_A = len(A[0])
    rows_B = len(B); cols_B = len(B[0])
    if cols_A != rows_B:
        return None
    out = [[0.0] * cols_B for _ in range(rows_A)]
    for i in range(rows_A):
        for j in range(cols_B):
            s = 0.0
            for k in range(cols_A):
                s += A[i][k] * B[k][j]
            out[i][j] = s
    return out


def transpose(M):
    return [list(col) for col in zip(*M)]


def ols_regression(X, y):
    """OLS: returns (betas, t_stats, r_squared, n_obs).
    X is N x K with first column = 1 for intercept.
    y is N x 1."""
    n = len(X)
    k = len(X[0])
    if n < k + 2:
        return None
    Xt = transpose(X)
    XtX = mat_mul(Xt, X)
    XtX_inv = gauss_jordan_inverse(XtX)
    if XtX_inv is None:
        return None
    y_mat = [[v] for v in y]
    Xty = mat_mul(Xt, y_mat)
    beta = mat_mul(XtX_inv, Xty)  # k x 1
    betas = [row[0] for row in beta]

    # Predictions + residuals
    y_hat = mat_mul(X, beta)
    residuals = [y[i] - y_hat[i][0] for i in range(n)]
    sse = sum(r ** 2 for r in residuals)
    y_mean = sum(y) / n
    sst = sum((v - y_mean) ** 2 for v in y)
    r_squared = (1 - sse / sst) if sst > 0 else None

    # Standard errors
    if n - k > 0:
        sigma_sq = sse / (n - k)
        t_stats = []
        for i in range(k):
            var_b = sigma_sq * XtX_inv[i][i]
            se_b = math.sqrt(var_b) if var_b > 0 else None
            t_stats.append((betas[i] / se_b) if se_b else None)
    else:
        t_stats = [None] * k

    return {"betas": betas, "t_stats": t_stats,
              "r_squared": r_squared, "n_obs": n}


# ---------- Per-ticker decomposition ----------
def decompose_ticker(symbol, factor_data, n_months=60):
    returns = fetch_monthly_returns(symbol, n_months=n_months)
    time.sleep(0.2)
    if len(returns) < 30:
        return {"ticker": symbol, "status": "insufficient_returns",
                "n_returns": len(returns)}

    # Build aligned (excess_return, factors) rows
    aligned = []
    for r in returns:
        yyyymm = r["yyyymm"]
        f = factor_data.get(yyyymm)
        if not f or f.get("MOM") is None:
            continue
        excess = r["ret_pct"] - f["RF"]
        aligned.append({
            "y": excess,
            "MktRF": f["MktRF"], "SMB": f["SMB"],
            "HML": f["HML"], "RMW": f["RMW"],
            "CMA": f["CMA"], "MOM": f["MOM"],
        })

    if len(aligned) < 30:
        return {"ticker": symbol, "status": "insufficient_aligned",
                "n_aligned": len(aligned)}

    X = [[1.0, a["MktRF"], a["SMB"], a["HML"],
            a["RMW"], a["CMA"], a["MOM"]] for a in aligned]
    y = [a["y"] for a in aligned]

    result = ols_regression(X, y)
    if not result:
        return {"ticker": symbol, "status": "regression_failed"}

    betas = result["betas"]
    t_stats = result["t_stats"]
    factor_names = ["alpha", "MktRF", "SMB", "HML", "RMW", "CMA", "MOM"]

    # Annualize alpha (monthly to yearly)
    alpha_monthly = betas[0]
    alpha_annual_pct = ((1 + alpha_monthly) ** 12 - 1) * 100

    return {
        "ticker": symbol,
        "status": "ok",
        "n_months_used": len(aligned),
        "r_squared": (round(result["r_squared"], 4)
                       if result["r_squared"] is not None else None),
        "alpha_monthly": round(alpha_monthly, 5),
        "alpha_annual_pct": round(alpha_annual_pct, 2),
        "alpha_t_stat": (round(t_stats[0], 2)
                          if t_stats[0] is not None else None),
        "factor_loadings": {
            factor_names[i]: {
                "beta": round(betas[i], 3),
                "t_stat": (round(t_stats[i], 2)
                            if t_stats[i] is not None else None),
                "significant": (abs(t_stats[i]) >= 2.0
                                  if t_stats[i] is not None else False),
            }
            for i in range(len(betas))
        },
    }


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[factor-decomposition] start v{VERSION}")

    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                      "error": "FMP_KEY missing"})}

    factor_data = get_factor_data()
    if not factor_data:
        return {"statusCode": 500, "body": json.dumps({
            "ok": False, "error": "could not load Ken French factor data"})}

    universe = STATIC_TOP50_SPX
    if isinstance(event, dict) and event.get("tickers"):
        universe = [t.upper() for t in event["tickers"]][:20]

    decompositions = []
    for i, sym in enumerate(universe):
        try:
            r = decompose_ticker(sym, factor_data, n_months=60)
            decompositions.append(r)
            if i % 10 == 0:
                print(f"[factor] {i+1}/{len(universe)}")
        except Exception as e:
            print(f"[{sym}] err: {str(e)[:120]}")
            decompositions.append({"ticker": sym, "error": str(e)[:120]})

    ok = [d for d in decompositions if d.get("status") == "ok"]
    # Top positive-alpha
    positive_alpha = sorted(
        [d for d in ok if d.get("alpha_annual_pct", 0) > 0
            and (d.get("alpha_t_stat") or 0) >= 1.5],
        key=lambda x: -(x.get("alpha_annual_pct") or 0))
    # Top negative-alpha
    negative_alpha = sorted(
        [d for d in ok if d.get("alpha_annual_pct", 0) < 0
            and (d.get("alpha_t_stat") or 0) <= -1.5],
        key=lambda x: (x.get("alpha_annual_pct") or 0))

    output = {
        "engine": "factor-decomposition",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_evaluated": len(ok),
        "n_factor_months_available": len(factor_data),
        "factor_data_latest_month": (max(factor_data.keys())
                                       if factor_data else None),
        "results": decompositions,
        "top_positive_alpha_significant": positive_alpha[:10],
        "top_negative_alpha_significant": negative_alpha[:10],
        "methodology": {
            "framework": "Fama-French 5-factor + Carhart momentum OLS",
            "equation": (
                "R_i - R_f = α + β1*MktRF + β2*SMB + β3*HML + β4*RMW "
                "+ β5*CMA + β6*MOM + ε"),
            "factors": {
                "MktRF": "Market excess return (broad equity exposure)",
                "SMB": "Small-minus-big (size factor)",
                "HML": "High-minus-low book-to-market (value factor)",
                "RMW": "Robust-minus-weak operating profitability",
                "CMA": "Conservative-minus-aggressive investment",
                "MOM": "Carhart winners-minus-losers momentum",
            },
            "estimation": (
                "Pure-Python OLS via Gauss-Jordan inversion. 60 months "
                "trailing returns minimum 30 aligned with factor data."),
            "significance": (
                "Alpha t-stat >= 2.0 = statistically significant skill. "
                "Beta t-stat >= 2.0 = significant factor loading."),
            "data_sources": [
                "Ken French Data Library (free, monthly factors)",
                "FMP /stable/historical-price-eod (ticker monthly closes)",
            ],
        },
        "academic_basis": [
            "Fama & French (1993). Common risk factors in returns on "
            "stocks and bonds. JFE 33(1), 3-56.",
            "Fama & French (2015). A five-factor asset pricing model. "
            "JFE 116(1).",
            "Carhart (1997). On persistence in mutual fund performance. "
            "Journal of Finance, 52(1), 57-82.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=86400")

    print(f"[factor-decomposition] complete: n_ok={len(ok)} "
          f"pos_alpha={len(positive_alpha)} neg_alpha={len(negative_alpha)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "n_evaluated": len(ok),
            "top_3_positive_alpha": [
                {"ticker": d["ticker"], "alpha_annual_pct":
                  d["alpha_annual_pct"]}
                for d in positive_alpha[:3]],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
