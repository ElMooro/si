"""
justhodl-factor-risk -- the firm Factor Risk Model.
===================================================
WHY THIS EXISTS
---------------
The platform now has seven desks, an allocator, a consolidated firm book
and a risk-monitor that POLICES that book against hard mandate limits.
But limit-checking only answers "are we inside the box". It does not
answer the question a real risk desk lives on: WHAT is the book actually
exposed to, and WHERE is the risk concentrated?

At Millennium / Citadel / AQR / Two Sigma the risk department runs a
multi-factor risk model (Barra / Axioma style) on top of limit-checking.
It decomposes the book into a handful of systematic factors, splits risk
into systematic vs idiosyncratic, estimates VaR / Expected Shortfall,
stress-tests the book against scenarios, and -- because the factors are
tradable -- sizes the hedges that would neutralise an unwanted bet.

This engine is that model. The seven desks could all be unknowingly
long the same momentum or small-cap bet; nothing in the stack would see
it until this engine measured it.

THE FACTOR SET  (tradable ETF proxies -- so every exposure maps to a
real hedge)
  MKT      market                       SPY
  SIZE     small minus big              IWM - SPY
  VALUE    value minus growth           IWD - IWF
  MOM      momentum minus market        MTUM - SPY
  QUALITY  quality minus market         QUAL - SPY
  LOWVOL   low-vol minus market         USMV - SPY
The five style factors are built as market-neutral spreads, so they are
only weakly collinear with MKT -- a clean decomposition.

METHOD
  1. Pull ~1y daily adjusted bars (Polygon) for the 7 ETFs and every
     name in the firm book. Build the 6 factor return series.
  2. For each name, time-series OLS of daily return on the 6 factors ->
     factor betas + residual (idiosyncratic) variance. Loadings are
     cached (data/factor-loadings-cache.json) and only refreshed when
     stale, so steady-state runs are cheap. Names without enough history
     (e.g. merger-arb targets) fall back to sector-average loadings.
  3. Book factor exposure b_k = sum_i w_i * beta_ik, w_i = signed weight.
  4. Risk:  systematic var = b' Sigma_F b ;  idiosyncratic var =
     sum_i w_i^2 * resid_var_i ;  total = sum.  Report the split, the
     annualised vol, parametric VaR/ES and a historical VaR.
  5. Marginal contribution to risk per factor and per name.
  6. Scenario stress P&L = b . shock for named historical analogues plus
     one empirical "worst rolling 20-day window" drawn from real data.
  7. Hedge sizing: for each material factor exposure, the ETF trade that
     neutralises it.

OUTPUT   data/factor-risk.json          SCHEDULE  daily 02:30 UTC
Reads the firm book + Polygon. The risk-analytics layer above the
risk-monitor's limit-checking; distinct from portfolio-risk (user book).
"""
import json
import math
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/factor-risk.json"
FIRM_KEY = "data/firm-book.json"
CACHE_KEY = "data/factor-loadings-cache.json"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
SCHEMA = "1.0"

FACTOR_NAMES = ["MKT", "SIZE", "VALUE", "MOM", "QUALITY", "LOWVOL"]
# each factor = primary leg minus hedge leg (hedge None -> raw leg)
FACTOR_LEGS = {
    "MKT": ("SPY", None),
    "SIZE": ("IWM", "SPY"),
    "VALUE": ("IWD", "IWF"),
    "MOM": ("MTUM", "SPY"),
    "QUALITY": ("QUAL", "SPY"),
    "LOWVOL": ("USMV", "SPY"),
}
ALL_ETFS = ["SPY", "IWM", "IWD", "IWF", "MTUM", "QUAL", "USMV"]
# the single ETF a desk would trade to express each factor
FACTOR_HEDGE_ETF = {"MKT": "SPY", "SIZE": "IWM", "VALUE": "IWD",
                    "MOM": "MTUM", "QUALITY": "QUAL", "LOWVOL": "USMV"}

TRADING_DAYS = 252
HISTORY_DAYS = 400          # calendar days -> ~252 trading bars
MIN_OBS = 60                # min aligned returns to trust a regression
CACHE_STALE_DAYS = 10       # refresh a name's loadings after this many days
FETCH_BUDGET_S = 720        # stop fetching new names past this; proxy rest

# z-multipliers for a normal tail
Z95, Z99 = 1.645, 2.326
ES95_MULT = 2.063           # E[loss | loss > VaR95] for a normal

# stylised historical-analogue factor-shock vectors (cumulative episode
# moves in the same spread-factor space the model regresses on). Labelled
# as analogues -- a desk recalibrates these; they are well-grounded.
SCENARIOS = [
    {"name": "2008 GFC (Sep-Nov 2008)",
     "shock": {"MKT": -0.30, "SIZE": -0.08, "VALUE": -0.05,
               "MOM": 0.05, "QUALITY": 0.06, "LOWVOL": 0.08}},
    {"name": "2020 COVID crash (Feb-Mar 2020)",
     "shock": {"MKT": -0.34, "SIZE": -0.12, "VALUE": -0.10,
               "MOM": 0.04, "QUALITY": 0.05, "LOWVOL": 0.07}},
    {"name": "2022 rate shock (H1 2022)",
     "shock": {"MKT": -0.20, "SIZE": -0.05, "VALUE": 0.08,
               "MOM": -0.06, "QUALITY": 0.02, "LOWVOL": 0.04}},
    {"name": "Aug 2007 quant/momentum crash",
     "shock": {"MKT": -0.02, "SIZE": -0.01, "VALUE": 0.04,
               "MOM": -0.12, "QUALITY": -0.03, "LOWVOL": 0.01}},
    {"name": "Q4 2018 selloff",
     "shock": {"MKT": -0.14, "SIZE": -0.06, "VALUE": -0.03,
               "MOM": -0.04, "QUALITY": 0.03, "LOWVOL": 0.05}},
    {"name": "Generic risk-off (-10% tape)",
     "shock": {"MKT": -0.10, "SIZE": -0.04, "VALUE": -0.02,
               "MOM": 0.02, "QUALITY": 0.03, "LOWVOL": 0.04}},
]

s3 = boto3.client("s3", region_name="us-east-1")


# ---- small utilities -------------------------------------------------------
def get_json(key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def put_json(key, obj):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="no-cache")


def poly_daily_closes(ticker, retries=1):
    """Return {date_iso: adjusted_close} for ~HISTORY_DAYS, or {} on miss."""
    to_d = datetime.now(timezone.utc).date()
    from_d = to_d - timedelta(days=HISTORY_DAYS)
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
           "?adjusted=true&sort=asc&limit=500&apiKey=%s"
           % (ticker, from_d, to_d, POLYGON_KEY))
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-factor-risk"})
            with urllib.request.urlopen(req, timeout=25) as r:
                body = json.loads(r.read().decode("utf-8"))
            results = body.get("results") or []
            if results:
                out = {}
                for bar in results:
                    d = datetime.fromtimestamp(
                        bar["t"] / 1000, timezone.utc).date().isoformat()
                    out[d] = bar.get("c")
                return out
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(2.0)
                continue
            return {}
        except Exception:
            if attempt < retries:
                time.sleep(1.0)
                continue
            return {}
        time.sleep(0.6)
    return {}


def returns_from_closes(closes):
    """Ordered list of (date, simple_return) from a {date: close} map."""
    dates = sorted(closes.keys())
    out = []
    for i in range(1, len(dates)):
        p0, p1 = closes[dates[i - 1]], closes[dates[i]]
        if p0 and p1 and p0 > 0:
            out.append((dates[i], p1 / p0 - 1.0))
    return out


# ---- linear algebra (pure python, small systems) ---------------------------
def solve(A, b):
    """Solve A x = b by Gauss-Jordan with partial pivoting. None if singular."""
    n = len(A)
    M = [list(A[i]) + [b[i]] for i in range(n)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(M[r][col]))
        if abs(M[piv][col]) < 1e-13:
            return None
        M[col], M[piv] = M[piv], M[col]
        d = M[col][col]
        M[col] = [v / d for v in M[col]]
        for r in range(n):
            if r != col:
                f = M[r][col]
                M[r] = [M[r][k] - f * M[col][k] for k in range(n + 1)]
    return [M[i][n] for i in range(n)]


def ols(X, y):
    """OLS of y on design matrix X (rows already include the intercept).

    Returns (beta, resid_var) or (None, None) if the system is singular.
    """
    n, p = len(X), len(X[0])
    XtX = [[sum(X[t][i] * X[t][j] for t in range(n)) for j in range(p)]
           for i in range(p)]
    Xty = [sum(X[t][i] * y[t] for t in range(n)) for i in range(p)]
    beta = solve(XtX, Xty)
    if beta is None:
        return None, None
    ssr = 0.0
    for t in range(n):
        fit = sum(beta[j] * X[t][j] for j in range(p))
        e = y[t] - fit
        ssr += e * e
    return beta, ssr / max(n - p, 1)


def cov_matrix(rows):
    """Sample covariance of a list of equal-length row vectors."""
    n, k = len(rows), len(rows[0])
    mu = [sum(rows[t][j] for t in range(n)) / n for j in range(k)]
    C = [[0.0] * k for _ in range(k)]
    for a in range(k):
        for b in range(a, k):
            s = sum((rows[t][a] - mu[a]) * (rows[t][b] - mu[b])
                    for t in range(n))
            C[a][b] = C[b][a] = s / max(n - 1, 1)
    return C, mu


def matvec(M, v):
    return [sum(M[i][j] * v[j] for j in range(len(v)))
            for i in range(len(M))]


def quantile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(math.floor(idx))
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


# ---- firm-book input -------------------------------------------------------
def load_equity_book():
    fb = get_json(FIRM_KEY) or {}
    eb = fb.get("equity_book")
    rows = None
    if isinstance(eb, dict):
        rows = eb.get("entries") or eb.get("book") or eb.get("positions")
    elif isinstance(eb, list):
        rows = eb
    if rows is None:
        rows = fb.get("equity") or fb.get("equity_entries") or []
    out = []
    for r in rows or []:
        sym = (r.get("symbol") or "").upper().strip()
        w = r.get("net_pct")
        if sym and isinstance(w, (int, float)):
            out.append({
                "symbol": sym,
                "name": r.get("name") or sym,
                "sector": r.get("sector") or "Unknown",
                "weight": w / 100.0,            # percent -> fraction
            })
    return out, (fb.get("generated_at") or "")


# ---- main ------------------------------------------------------------------
def lambda_handler(event, context):
    t_start = time.time()
    book, fb_asof = load_equity_book()
    if not book:
        out = {"schema": SCHEMA, "error": "firm book unavailable",
               "generated_at": datetime.now(timezone.utc).isoformat()}
        put_json(OUT_KEY, out)
        return {"statusCode": 200, "body": json.dumps(out)}

    # ---- 1) factor return series from the 7 ETFs ----
    etf_closes = {}
    for t in ALL_ETFS:
        etf_closes[t] = poly_daily_closes(t, retries=1)
    etf_rets = {}
    for t in ALL_ETFS:
        etf_rets[t] = dict(returns_from_closes(etf_closes[t]))

    common = None
    for t in ALL_ETFS:
        ds = set(etf_rets[t].keys())
        common = ds if common is None else (common & ds)
    common = sorted(common or [])
    if len(common) < MIN_OBS:
        out = {"schema": SCHEMA, "error": "insufficient ETF history",
               "n_common_days": len(common),
               "generated_at": datetime.now(timezone.utc).isoformat()}
        put_json(OUT_KEY, out)
        return {"statusCode": 200, "body": json.dumps(out)}

    # factor series aligned on common dates
    fac_series = []        # list over days of [MKT,SIZE,VALUE,MOM,QUAL,LV]
    for d in common:
        spy = etf_rets["SPY"][d]
        row = [
            spy,                                   # MKT
            etf_rets["IWM"][d] - spy,              # SIZE
            etf_rets["IWD"][d] - etf_rets["IWF"][d],  # VALUE
            etf_rets["MTUM"][d] - spy,             # MOM
            etf_rets["QUAL"][d] - spy,             # QUALITY
            etf_rets["USMV"][d] - spy,             # LOWVOL
        ]
        fac_series.append(row)
    fac_date = {common[i]: fac_series[i] for i in range(len(common))}

    sigma_daily, _ = cov_matrix(fac_series)        # 6x6 daily factor cov

    # ---- 2) per-name loadings (cache + fetch + regress) ----
    cache = get_json(CACHE_KEY) or {"schema": SCHEMA, "loadings": {}}
    loadings = cache.get("loadings", {})
    today = datetime.now(timezone.utc).date()

    def cache_fresh(sym):
        e = loadings.get(sym)
        if not e or "asof" not in e:
            return False
        try:
            asof = datetime.fromisoformat(e["asof"]).date()
        except Exception:
            return False
        return (today - asof).days <= CACHE_STALE_DAYS

    order = sorted(book, key=lambda r: -abs(r["weight"]))
    n_fetched = n_cached = n_failed = 0
    for r in order:
        sym = r["symbol"]
        if cache_fresh(sym):
            n_cached += 1
            continue
        if time.time() - t_start > FETCH_BUDGET_S:
            continue                               # proxy this run; cache later
        closes = poly_daily_closes(sym, retries=1)
        rets = dict(returns_from_closes(closes))
        aligned = [d for d in common if d in rets]
        if len(aligned) < MIN_OBS:
            n_failed += 1
            continue
        X = [[1.0] + fac_date[d] for d in aligned]
        y = [rets[d] for d in aligned]
        beta, rvar = ols(X, y)
        if beta is None:
            n_failed += 1
            continue
        loadings[sym] = {
            "betas": {FACTOR_NAMES[i]: beta[i + 1] for i in range(6)},
            "alpha": beta[0],
            "resid_var": rvar,
            "n_obs": len(aligned),
            "asof": today.isoformat(),
        }
        n_fetched += 1

    cache["loadings"] = loadings
    cache["schema"] = SCHEMA
    cache["updated"] = datetime.now(timezone.utc).isoformat()
    try:
        put_json(CACHE_KEY, cache)
    except Exception:
        pass

    # ---- 3) sector-proxy loadings for names with no usable history ----
    sector_acc = {}
    for r in book:
        e = loadings.get(r["symbol"])
        if not e:
            continue
        sec = r["sector"]
        a = sector_acc.setdefault(sec, {"betas": {f: 0.0 for f in
                                        FACTOR_NAMES}, "rv": 0.0, "n": 0})
        for f in FACTOR_NAMES:
            a["betas"][f] += e["betas"].get(f, 0.0)
        a["rv"] += e["resid_var"]
        a["n"] += 1
    sector_proxy = {}
    for sec, a in sector_acc.items():
        if a["n"]:
            sector_proxy[sec] = {
                "betas": {f: a["betas"][f] / a["n"] for f in FACTOR_NAMES},
                "resid_var": a["rv"] / a["n"]}
    # all-book average as a last resort
    if sector_acc:
        tot = {f: 0.0 for f in FACTOR_NAMES}
        trv = tn = 0
        for sec, a in sector_acc.items():
            for f in FACTOR_NAMES:
                tot[f] += a["betas"][f]
            trv += a["rv"]
            tn += a["n"]
        book_avg = {"betas": {f: tot[f] / tn for f in FACTOR_NAMES},
                    "resid_var": trv / tn}
    else:
        # nothing regressed at all -- conservative market-only fallback
        book_avg = {"betas": {f: (1.0 if f == "MKT" else 0.0)
                              for f in FACTOR_NAMES}, "resid_var": 0.10}

    # ---- 4) book factor exposure + risk decomposition ----
    b = {f: 0.0 for f in FACTOR_NAMES}
    idio_var = 0.0
    n_direct = n_proxy = 0
    name_load = {}
    for r in book:
        sym, w = r["symbol"], r["weight"]
        e = loadings.get(sym)
        if e:
            betas, rv = e["betas"], e["resid_var"]
            n_direct += 1
            src = "direct"
        else:
            p = sector_proxy.get(r["sector"], book_avg)
            betas, rv = p["betas"], p["resid_var"]
            n_proxy += 1
            src = "proxy"
        name_load[sym] = {"betas": betas, "resid_var": rv,
                          "weight": w, "src": src,
                          "sector": r["sector"], "name": r["name"]}
        for f in FACTOR_NAMES:
            b[f] += w * betas.get(f, 0.0)
        idio_var += w * w * rv

    bvec = [b[f] for f in FACTOR_NAMES]
    sig_b_daily = matvec(sigma_daily, bvec)
    sys_var_daily = sum(bvec[i] * sig_b_daily[i] for i in range(6))
    sys_var_daily = max(sys_var_daily, 0.0)
    total_var_daily = sys_var_daily + idio_var
    daily_vol = math.sqrt(max(total_var_daily, 0.0))
    annual_vol = daily_vol * math.sqrt(TRADING_DAYS)
    pct_sys = (sys_var_daily / total_var_daily * 100.0
               if total_var_daily > 0 else 0.0)

    # ---- 5) VaR / ES ----
    var95 = Z95 * daily_vol * 100.0
    var99 = Z99 * daily_vol * 100.0
    es95 = ES95_MULT * daily_vol * 100.0
    # historical VaR: replay the book's systematic daily P&L through history
    hist_pnl = sorted(
        sum(bvec[i] * fac_series[t][i] for i in range(6)) * 100.0
        for t in range(len(fac_series)))
    hist_var95 = -quantile(hist_pnl, 0.05)
    hist_var99 = -quantile(hist_pnl, 0.01)

    # ---- 6) marginal contribution to risk, per factor ----
    factor_rows = []
    for i, f in enumerate(FACTOR_NAMES):
        mctr_var = bvec[i] * sig_b_daily[i]
        factor_rows.append({
            "factor": f,
            "book_exposure_pct": round(b[f] * 100.0, 2),
            "mctr_pct_of_systematic": round(
                mctr_var / sys_var_daily * 100.0, 1)
            if sys_var_daily > 0 else 0.0,
            "factor_vol_annual_pct": round(
                math.sqrt(max(sigma_daily[i][i], 0.0))
                * math.sqrt(TRADING_DAYS) * 100.0, 1),
        })
    factor_rows.sort(key=lambda x: -abs(x["book_exposure_pct"]))

    # ---- per-name risk contribution ----
    contrib = []
    for sym, nl in name_load.items():
        bi = [nl["betas"].get(f, 0.0) for f in FACTOR_NAMES]
        sys_cov = sum(bi[i] * sig_b_daily[i] for i in range(6))
        c = nl["weight"] * sys_cov + nl["weight"] ** 2 * nl["resid_var"]
        contrib.append({
            "symbol": sym, "name": nl["name"], "sector": nl["sector"],
            "weight_pct": round(nl["weight"] * 100.0, 2),
            "risk_contribution_pct": round(
                c / total_var_daily * 100.0, 2)
            if total_var_daily > 0 else 0.0,
            "loading_source": nl["src"],
        })
    contrib.sort(key=lambda x: -abs(x["risk_contribution_pct"]))

    # ---- 7) scenario stress P&L ----
    scen_rows = []
    for sc in SCENARIOS:
        pnl = sum(b[f] * sc["shock"].get(f, 0.0) for f in FACTOR_NAMES)
        scen_rows.append({"scenario": sc["name"],
                          "book_pnl_pct": round(pnl * 100.0, 2),
                          "shock": sc["shock"]})
    # empirical: worst rolling 20-day window for THIS book
    win = 20
    worst = None
    for s in range(0, len(fac_series) - win + 1):
        cum = [0.0] * 6
        for t in range(s, s + win):
            for i in range(6):
                cum[i] += fac_series[t][i]
        pnl = sum(bvec[i] * cum[i] for i in range(6))
        if worst is None or pnl < worst["pnl"]:
            worst = {"pnl": pnl, "cum": cum,
                     "start": common[s], "end": common[s + win - 1]}
    if worst:
        scen_rows.append({
            "scenario": "Worst observed 20-day window (%s to %s)"
                        % (worst["start"], worst["end"]),
            "book_pnl_pct": round(worst["pnl"] * 100.0, 2),
            "shock": {FACTOR_NAMES[i]: round(worst["cum"][i], 4)
                      for i in range(6)},
            "empirical": True})
    scen_rows.sort(key=lambda x: x["book_pnl_pct"])

    # ---- 8) hedge sizing ----
    hedges = []
    for fr in factor_rows:
        f, exp = fr["factor"], fr["book_exposure_pct"]
        if abs(exp) < 3.0:                         # immaterial -> skip
            continue
        side = "SHORT" if exp > 0 else "LONG"
        hedges.append({
            "factor": f,
            "current_exposure_pct": exp,
            "suggested_trade": "%s %.1f%% %s" % (
                side, abs(exp), FACTOR_HEDGE_ETF[f]),
            "note": ("neutralises the book's %s factor bet" % f),
        })

    # ---- headline ----
    biggest = factor_rows[0] if factor_rows else None
    worst_scen = scen_rows[0] if scen_rows else None
    headline = (
        "Firm book annualised vol %.1f%% (%.0f%% systematic). Largest "
        "factor bet: %s %+.1f%%. Worst stress: %s %.1f%%."
        % (annual_vol * 100.0, pct_sys,
           biggest["factor"] if biggest else "-",
           biggest["book_exposure_pct"] if biggest else 0.0,
           worst_scen["scenario"].split(" (")[0] if worst_scen else "-",
           worst_scen["book_pnl_pct"] if worst_scen else 0.0))

    out = {
        "schema": SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "firm_book_asof": fb_asof,
        "headline": headline,
        "firm": {
            "n_equity_names": len(book),
            "annual_vol_pct": round(annual_vol * 100.0, 2),
            "daily_vol_pct": round(daily_vol * 100.0, 3),
            "pct_systematic": round(pct_sys, 1),
            "pct_idiosyncratic": round(100.0 - pct_sys, 1),
            "var_95_1d_pct": round(var95, 2),
            "var_99_1d_pct": round(var99, 2),
            "es_95_1d_pct": round(es95, 2),
            "hist_var_95_1d_pct": round(hist_var95, 2),
            "hist_var_99_1d_pct": round(hist_var99, 2),
            "net_market_beta": round(b["MKT"], 3),
        },
        "factor_exposures": factor_rows,
        "risk_contributors": contrib[:15],
        "scenarios": scen_rows,
        "hedges": hedges,
        "coverage": {
            "n_names": len(book),
            "n_direct_loadings": n_direct,
            "n_proxy_loadings": n_proxy,
            "fetched_this_run": n_fetched,
            "served_from_cache": n_cached,
            "failed_history": n_failed,
            "factor_history_days": len(common),
            "note": ("names without enough Polygon history use "
                     "sector-average loadings; the cache warms over "
                     "successive runs"),
        },
        "method": ("Time-series OLS of each name's daily return on six "
                   "tradable ETF-proxy factors (market + five "
                   "market-neutral style spreads). Systematic risk from "
                   "the factor covariance, idiosyncratic from regression "
                   "residuals."),
        "disclaimer": ("Risk-analytics model for the platform's model "
                       "firm book. Scenario shocks are stylised "
                       "historical analogues. Not investment advice."),
    }
    put_json(OUT_KEY, out)
    return {"statusCode": 200, "body": json.dumps({"ok": True,
            "annual_vol_pct": out["firm"]["annual_vol_pct"],
            "pct_systematic": out["firm"]["pct_systematic"],
            "n_direct": n_direct, "n_proxy": n_proxy})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
