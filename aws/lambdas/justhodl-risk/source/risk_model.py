"""risk_model.py — JustHodl composite risk model (pure stdlib).

Builds two descriptive composites from every long-history risk series the system
already banks (FRED-scoped warm store, CDS-desk long sources, risk-source engine
warm series, Coin Metrics community data):

  STRESS  0–100  — point-in-time percentile composite of stress gauges across
                   nine pillars. High = systemic-stress / capitulation territory.
  FROTH   0–100  — point-in-time percentile composite of complacency, extension
                   and late-cycle gauges. High = conditions historically seen
                   around major equity / crypto peaks.

Every component is converted to a *point-in-time expanding percentile*: the
reading on day t is ranked only against that component's own history up to t.
No look-ahead. Pillar = mean of available component percentiles; composite =
weighted mean of pillars. Weights are fixed constants chosen on the training
window (<= 2012-12-31) and reported separately for the held-out window.

Descriptive only. decision.call None. Nothing here is a forecast.
"""
from __future__ import annotations

import bisect
import csv
import datetime as dt
import io
import json
import math
from collections import OrderedDict

MODEL_VERSION = "1.0.0"
CAL_START = "1990-01-01"
TRAIN_END = "2012-12-31"

# ───────────────────────────────────────────────────────── episode labels ──
# Equity peak→trough windows (NASDAQ/S&P 500 drawdowns >= ~15 %) plus funding
# crises that did not produce a 15 % equity drawdown (LTCM, SVB, yen carry).
# These are the *labels* the scorecard is measured against. Dates are the
# conventional peak and trough closes. Edit here and the whole scorecard recomputes.
EPISODES = [
    ("1990-07-16", "1990-10-11", "Gulf War / S&L recession", "equity"),
    ("1998-07-17", "1998-10-08", "LTCM / Russia default", "equity"),
    ("2000-03-10", "2002-10-09", "Dot-com bust", "equity"),
    ("2007-10-09", "2009-03-09", "Global financial crisis", "equity"),
    ("2010-04-23", "2010-07-02", "Greece / flash crash", "equity"),
    ("2011-07-22", "2011-10-03", "US downgrade / euro crisis", "equity"),
    ("2015-07-20", "2016-02-11", "China devaluation / energy", "equity"),
    ("2018-01-26", "2018-02-08", "Volmageddon", "equity"),
    ("2018-10-03", "2018-12-24", "Q4 2018 tightening", "equity"),
    ("2020-02-19", "2020-03-23", "COVID crash", "equity"),
    ("2022-01-03", "2022-10-12", "2022 hiking bear", "equity"),
    ("2023-03-08", "2023-03-13", "SVB / regional banks", "funding"),
    ("2024-07-16", "2024-08-05", "Yen carry unwind", "equity"),
    ("2025-02-19", "2025-04-08", "Tariff shock", "equity"),
]
# Bitcoin cycle peaks and troughs (close-to-close, Coin Metrics PriceUSD).
BTC_EPISODES = [
    ("2011-06-08", "2011-11-18", "BTC 2011 bubble"),
    ("2013-04-09", "2013-07-05", "BTC Apr-2013 top"),
    ("2013-11-30", "2015-01-14", "BTC 2013-15 bear"),
    ("2017-12-16", "2018-12-15", "BTC 2018 bear"),
    ("2019-06-26", "2020-03-12", "BTC 2019-20 drawdown"),
    ("2021-04-14", "2021-07-20", "BTC May-2021 crash"),
    ("2021-11-08", "2022-11-21", "BTC 2022 bear"),
]

# ───────────────────────────────────────────────────────── component spec ──
# (component id, pillar, label, source key, transform, freshness days, freq)
# transform: level | neg | chg63 | negchg63 | yoy | negyoy | dd252 | ddath | rvol20 |
#            ma200neg (below trend = stress) | ma200 (above trend = froth) | ret252
STRESS_COMPONENTS = [
    # credit
    ("hy_oas", "credit", "ICE BofA US HY OAS", "BAMLH0A0HYM2", "level", 10, "D"),
    ("ig_oas", "credit", "ICE BofA US IG OAS", "BAMLC0A0CM", "level", 10, "D"),
    ("ccc_oas", "credit", "ICE BofA CCC & lower OAS", "BAMLH0A3HYC", "level", 10, "D"),
    ("em_oas", "credit", "ICE BofA EM corporate OAS", "BAMLEMCBPIOAS", "level", 10, "D"),
    ("baa10y", "credit", "Moody's Baa – 10y Treasury", "BAA10Y", "level", 10, "D"),
    ("ebp", "credit", "Excess bond premium (Fed)", "ebp", "level", 75, "M"),
    ("gz_spread", "credit", "Gilchrist–Zakrajšek spread", "gz_spread", "level", 75, "M"),
    ("cmdi", "credit", "NY Fed CMDI (market)", "cmdi_market", "level", 21, "W"),
    ("ofr_credit", "credit", "OFR FSI · credit", "ofr_credit", "level", 10, "D"),
    # volatility
    ("vix", "volatility", "VIX", "VIXCLS", "level", 10, "D"),
    ("vxn", "volatility", "VXN (Nasdaq-100 vol)", "VXNCLS", "level", 10, "D"),
    ("vix_term", "volatility", "VIX – VIX3M (term inversion)", "vix_minus_vxv", "level", 10, "D"),
    ("ovx", "volatility", "OVX (oil vol)", "OVXCLS", "level", 10, "D"),
    ("gvz", "volatility", "GVZ (gold vol)", "GVZCLS", "level", 10, "D"),
    ("rvx", "volatility", "RVX (Russell 2000 vol)", "RVXCLS", "level", 10, "D"),
    ("ofr_vol", "volatility", "OFR FSI · volatility", "ofr_vol", "level", 10, "D"),
    ("ndx_rvol", "volatility", "Nasdaq 20d realised vol", "NASDAQCOM", "rvol20", 10, "D"),
    # funding / liquidity
    ("ofr_funding", "funding", "OFR FSI · funding", "ofr_funding", "level", 10, "D"),
    ("ofr_safe", "funding", "OFR FSI · safe assets", "ofr_safe", "level", 10, "D"),
    ("ted", "funding", "TED spread (to 2022)", "TEDRATE", "level", 10, "D"),
    ("sofr_iorb", "funding", "SOFR – IORB", "sofr_minus_iorb", "level", 10, "D"),
    ("dw_credit", "funding", "Discount-window primary credit (log)", "DPCREDIT", "log", 15, "W"),
    ("rrp_drain", "funding", "ON RRP balance 63d change (negative = drain)", "RRPONTSYD", "negchg63", 10, "D"),
    # systemic (official stress indices)
    ("ofr_fsi", "systemic", "OFR Financial Stress Index", "ofr_fsi", "level", 10, "D"),
    ("stlfsi", "systemic", "St. Louis Fed FSI", "STLFSI4", "level", 15, "W"),
    ("nfci", "systemic", "Chicago Fed NFCI", "NFCI", "level", 15, "W"),
    ("anfci", "systemic", "Chicago Fed adjusted NFCI", "ANFCI", "level", 15, "W"),
    ("kcfsi", "systemic", "Kansas City Fed FSI", "KCFSI", "level", 75, "M"),
    ("ciss_us", "systemic", "ECB CISS · United States", "ciss_US", "level", 10, "D"),
    ("ciss_ea", "systemic", "ECB CISS · euro area", "ciss_U2", "level", 10, "D"),
    ("ciss_cn", "systemic", "ECB CISS · China", "ciss_CN", "level", 10, "D"),
    ("ciss_gb", "systemic", "ECB CISS · United Kingdom", "ciss_GB", "level", 10, "D"),
    # rates / macro
    ("curve_10y3m", "rates", "10y – 3m curve (inverted = stress)", "T10Y3M", "neg", 10, "D"),
    ("curve_10y2y", "rates", "10y – 2y curve (inverted = stress)", "T10Y2Y", "neg", 10, "D"),
    ("sloos", "rates", "SLOOS net tightening, C&I large firms", "DRTSCILM", "level", 200, "Q"),
    ("breakeven_drop", "rates", "10y breakeven 63d change (falling = stress)", "T10YIE", "negchg63", 10, "D"),
    ("real_yield_jump", "rates", "10y real yield 63d change", "DFII10", "chg63", 10, "D"),
    ("umcsent", "rates", "Michigan sentiment (low = stress)", "UMCSENT", "neg", 75, "M"),
    # sovereign / global
    ("sovciss_ea", "sovereign", "ECB SovCISS · euro area", "sovciss_U2", "level", 10, "D"),
    ("sovciss_it", "sovereign", "ECB SovCISS · Italy", "sovciss_IT", "level", 10, "D"),
    ("sovciss_es", "sovereign", "ECB SovCISS · Spain", "sovciss_ES", "level", 10, "D"),
    ("sovciss_fr", "sovereign", "ECB SovCISS · France", "sovciss_FR", "level", 10, "D"),
    ("usd_3m", "sovereign", "Broad dollar 63d change", "DTWEXBGS", "chg63", 10, "D"),
    ("ofr_em", "sovereign", "OFR FSI · emerging markets", "ofr_em", "level", 10, "D"),
    ("ofr_oae", "sovereign", "OFR FSI · other advanced economies", "ofr_oae", "level", 10, "D"),
    ("esma_downgrades", "sovereign", "ESMA sovereign downgrades / month", "esma_downgrades", "level", 45, "M"),
    # banking / household
    ("dq_all", "banking", "Delinquency rate, all bank loans", "DRALACBS", "level", 200, "Q"),
    ("dq_cards", "banking", "Delinquency rate, credit cards", "DRCCLACBS", "level", 200, "Q"),
    ("dq_mortgage", "banking", "Delinquency rate, single-family mortgages", "DRSFRMACBS", "level", 200, "Q"),
    ("hhdc_90", "banking", "NY Fed HHDC 90+ days delinquent", "hhdc_90", "level", 200, "Q"),
    ("fdic_htm", "banking", "FDIC HTM unrealised loss / equity", "fdic_htm", "level", 200, "Q"),
    ("bank_credit", "banking", "Bank credit y/y (contraction = stress)", "TOTBKCR", "negyoy", 15, "W"),
    ("hpi_yoy", "banking", "FHFA house prices y/y (falling = stress)", "fhfa_us", "negyoy", 90, "M"),
    # uncertainty
    ("epu_us", "uncertainty", "US EPU 30d mean", "epu_us30", "level", 10, "D"),
    ("wui", "uncertainty", "World Uncertainty Index", "wui_global", "level", 200, "Q"),
    ("food_yoy", "uncertainty", "FAO food price index y/y", "fao_ffpi", "yoy", 75, "M"),
    # equity
    ("ndx_dd", "equity", "Nasdaq drawdown from 1y high", "NASDAQCOM", "dd252", 10, "D"),
    ("spx_dd", "equity", "S&P 500 drawdown from 1y high", "SP500", "dd252", 10, "D"),
    ("ndx_trend", "equity", "Nasdaq below 200d average", "NASDAQCOM", "ma200neg", 10, "D"),
    ("hy_tr_dd", "equity", "HY total-return drawdown from 1y high", "BAMLHYH0A0HYM2TRIV", "dd252", 10, "D"),
    # crypto
    ("btc_dd", "crypto", "Bitcoin drawdown from all-time high", "btc_price", "ddath", 5, "D"),
    ("btc_rvol", "crypto", "Bitcoin 30d realised vol", "btc_price", "rvol30", 5, "D"),
    ("btc_trend", "crypto", "Bitcoin below 200d average", "btc_price", "ma200neg", 5, "D"),
    ("btc_mvrv_low", "crypto", "Bitcoin MVRV (low = capitulation)", "btc_mvrv", "neg", 5, "D"),
]

FROTH_COMPONENTS = [
    # complacency (inverted stress)
    ("vix_low", "complacency", "VIX (low = complacent)", "VIXCLS", "neg", 10, "D"),
    ("hy_tight", "complacency", "HY OAS (tight = complacent)", "BAMLH0A0HYM2", "neg", 10, "D"),
    ("ccc_bb_tight", "complacency", "CCC – BB spread (compressed)", "ccc_minus_bb", "neg", 10, "D"),
    ("nfci_loose", "complacency", "NFCI (loose conditions)", "NFCI", "neg", 15, "W"),
    ("epu_low", "complacency", "US EPU 30d (low = complacent)", "epu_us30", "neg", 10, "D"),
    ("ofr_eqval", "complacency", "OFR FSI · equity valuation (rich)", "ofr_eqval", "neg", 10, "D"),
    # extension
    ("ndx_1y", "extension", "Nasdaq 1y return", "NASDAQCOM", "ret252", 10, "D"),
    ("ndx_above_ma", "extension", "Nasdaq above 200d average", "NASDAQCOM", "ma200", 10, "D"),
    ("btc_mvrv", "extension", "Bitcoin MVRV", "btc_mvrv", "level", 5, "D"),
    ("btc_above_ma", "extension", "Bitcoin above 200d average", "btc_price", "ma200", 5, "D"),
    ("btc_1y", "extension", "Bitcoin 1y return", "btc_price", "ret252", 5, "D"),
    ("hpi_hot", "extension", "FHFA house prices y/y", "fhfa_us", "yoy", 90, "M"),
    ("umcsent_hi", "extension", "Michigan sentiment (high)", "UMCSENT", "level", 75, "M"),
    # late cycle
    ("curve_inv", "late_cycle", "10y – 3m curve inversion", "T10Y3M", "neg", 10, "D"),
    ("policy_tight", "late_cycle", "Fed funds 1y change", "EFFR", "chg252", 10, "D"),
    ("sloos_easing", "late_cycle", "SLOOS easing (negative tightening)", "DRTSCILM", "neg", 200, "Q"),
    ("credit_boom", "late_cycle", "Bank credit y/y", "TOTBKCR", "yoy", 15, "W"),
    ("oil_1y", "late_cycle", "WTI 1y change", "DCOILWTICO", "ret252", 10, "D"),
]

PILLAR_LABELS = OrderedDict([
    ("credit", "Credit spreads"), ("volatility", "Volatility"), ("funding", "Funding & liquidity"),
    ("systemic", "Official stress indices"), ("rates", "Rates & macro"), ("sovereign", "Sovereign & global"),
    ("banking", "Banking & household"), ("uncertainty", "Uncertainty"), ("equity", "Equity market"),
    ("crypto", "Crypto"),
])
FROTH_LABELS = OrderedDict([("complacency", "Complacency"), ("extension", "Extension"), ("late_cycle", "Late cycle")])

# Weights chosen on the training window (see calibrate()); frozen here.
STRESS_WEIGHTS = {"credit": 3.0, "volatility": 0.5, "funding": 0.5, "systemic": 3.0, "rates": 0.5,
                  "sovereign": 0.5, "banking": 0.5, "uncertainty": 0.5, "equity": 1.0, "crypto": 0.5}
# equity (1.0) and crypto (0.5) were held fixed during calibration: the episode labels are themselves
# equity drawdowns, so letting the equity pillar tune itself would be circular.
FROTH_WEIGHTS = {"complacency": 1.0, "extension": 1.0, "late_cycle": 1.0}
MIN_HISTORY = {"D": 750, "W": 156, "M": 36, "Q": 12}
STRESS_ALERT = 65.0      # train-window (1990–2012) F1 maximiser, see best_threshold()
STRESS_EXTREME = 80.0    # ~95th percentile of the full composite history
FROTH_ALERT = 70.0       # ~95th percentile of the froth composite history
REL_ZONE = 90.0          # 3-year rolling percentile zone for both composites
REL_WINDOW = 756         # business days (~3 years)
BTC_FROTH_IDS = ("btc_mvrv", "btc_above_ma", "btc_1y")


# ───────────────────────────────────────────────────────────── utilities ──
def iso(d):
    return d.isoformat() if isinstance(d, dt.date) else str(d)[:10]


def to_date(s):
    return dt.date.fromisoformat(str(s)[:10])


def fnum(x):
    try:
        if x is None or x == "" or x == ".":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def clean(points):
    """[[date, value], ...] -> sorted, deduplicated, numeric."""
    d = {}
    for p in points or []:
        try:
            k, v = p[0], fnum(p[1])
        except (TypeError, IndexError):
            continue
        if k and v is not None:
            d[str(k)[:10]] = v
    return sorted(d.items())


def fred_points(obj):
    obs = (obj or {}).get("observations") or (obj or {}).get("points") or []
    out = []
    for o in obs:
        if isinstance(o, dict):
            out.append([o.get("date"), o.get("value")])
        elif isinstance(o, (list, tuple)) and len(o) >= 2:
            out.append([o[0], o[1]])
    return clean(out)


def warm_points(obj):
    return clean((obj or {}).get("points") or [])


def parse_fsi_csv(text):
    rows = list(csv.reader(io.StringIO(text)))
    head = rows[0]
    col = {h.strip().lower(): i for i, h in enumerate(head)}
    def pick(name):
        i = col.get(name)
        return clean([[r[0], r[i]] for r in rows[1:] if len(r) > i]) if i is not None else []
    return {"ofr_fsi": pick("ofr fsi"), "ofr_credit": pick("credit"), "ofr_eqval": pick("equity valuation"),
            "ofr_safe": pick("safe assets"), "ofr_funding": pick("funding"), "ofr_vol": pick("volatility"),
            "ofr_us": pick("united states"), "ofr_oae": pick("other advanced economies"),
            "ofr_em": pick("emerging markets")}


def parse_ebp_csv(text):
    rows = list(csv.DictReader(io.StringIO(text)))
    return {"ebp": clean([[r["date"], r["ebp"]] for r in rows]),
            "gz_spread": clean([[r["date"], r["gz_spread"]] for r in rows])}


def parse_ecb_csv(text, prefix):
    out = {}
    for r in csv.DictReader(io.StringIO(text)):
        area = r.get("REF_AREA")
        tp = r.get("TIME_PERIOD") or ""
        if len(tp) == 7:
            tp = tp + "-15"
        out.setdefault(prefix + area, []).append([tp, r.get("OBS_VALUE")])
    return {k: clean(v) for k, v in out.items()}


def parse_coinmetrics_csv(text):
    price, mvrv = [], []
    for r in csv.DictReader(io.StringIO(text)):
        t = r.get("time")
        if r.get("PriceUSD"):
            price.append([t, r["PriceUSD"]])
        if r.get("CapMVRVCur"):
            mvrv.append([t, r["CapMVRVCur"]])
    return {"btc_price": clean(price), "btc_mvrv": clean(mvrv)}


# ─────────────────────────────────────────────────────────── transforms ──
def _series_map(pts):
    return dict(pts)


def transform(pts, kind):
    """Apply a transform to [[date,value]] -> [[date,value]] (risk direction = +)."""
    if not pts:
        return []
    if kind == "level":
        return pts
    if kind == "neg":
        return [[d, -v] for d, v in pts]
    if kind == "log":
        return [[d, math.log(max(v, 1.0))] for d, v in pts]
    vals = [v for _, v in pts]
    n = len(pts)
    out = []
    if kind in ("chg63", "negchg63", "chg252"):
        lag = 252 if kind == "chg252" else 63
        sgn = -1.0 if kind == "negchg63" else 1.0
        for i in range(lag, n):
            out.append([pts[i][0], sgn * (vals[i] - vals[i - lag])])
        return out
    if kind in ("yoy", "negyoy"):
        sgn = -1.0 if kind == "negyoy" else 1.0
        dates = [to_date(d) for d, _ in pts]
        j = 0
        for i in range(n):
            target = dates[i] - dt.timedelta(days=365)
            while j < n and dates[j] < target - dt.timedelta(days=20):
                j += 1
            # nearest observation at or before target
            k = bisect.bisect_right(dates, target) - 1
            if k >= 0 and (target - dates[k]).days <= 45 and vals[k]:
                out.append([pts[i][0], sgn * 100.0 * (vals[i] / vals[k] - 1.0)])
        return out
    if kind == "ret252":
        for i in range(252, n):
            if vals[i - 252]:
                out.append([pts[i][0], 100.0 * (vals[i] / vals[i - 252] - 1.0)])
        return out
    if kind == "dd252":
        from collections import deque
        q = deque()
        for i in range(n):
            while q and vals[q[-1]] <= vals[i]:
                q.pop()
            q.append(i)
            while q[0] <= i - 252:
                q.popleft()
            hi = vals[q[0]]
            if hi:
                out.append([pts[i][0], 100.0 * (1.0 - vals[i] / hi)])
        return out
    if kind == "ddath":
        hi = -1e300
        for i in range(n):
            hi = max(hi, vals[i])
            if hi > 0:
                out.append([pts[i][0], 100.0 * (1.0 - vals[i] / hi)])
        return out
    if kind in ("rvol20", "rvol30"):
        w = 20 if kind == "rvol20" else 30
        ann = math.sqrt(252 if kind == "rvol20" else 365)
        lr = [None] + [math.log(vals[i] / vals[i - 1]) if vals[i] > 0 and vals[i - 1] > 0 else 0.0 for i in range(1, n)]
        s, s2 = 0.0, 0.0
        for i in range(1, n):
            s += lr[i]; s2 += lr[i] * lr[i]
            if i > w:
                s -= lr[i - w]; s2 -= lr[i - w] * lr[i - w]
            if i >= w:
                var = max(s2 / w - (s / w) ** 2, 0.0)
                out.append([pts[i][0], 100.0 * ann * math.sqrt(var)])
        return out
    if kind in ("ma200", "ma200neg"):
        s = 0.0
        for i in range(n):
            s += vals[i]
            if i >= 200:
                s -= vals[i - 200]
            if i >= 199:
                ma = s / 200.0
                if ma:
                    r = 100.0 * (vals[i] / ma - 1.0)
                    out.append([pts[i][0], -r if kind == "ma200neg" else r])
        return out
    raise ValueError("unknown transform " + kind)


def expanding_percentile(pts, min_n):
    """Point-in-time percentile rank of each value against all prior+current values."""
    sorted_vals = []
    out = []
    for d, v in pts:
        bisect.insort(sorted_vals, v)
        n = len(sorted_vals)
        if n >= min_n:
            lo = bisect.bisect_left(sorted_vals, v)
            hi = bisect.bisect_right(sorted_vals, v)
            out.append([d, 100.0 * (lo + 0.5 * (hi - lo)) / n])
    return out


# ───────────────────────────────────────────────────────────── alignment ──
def business_days(start, end):
    d, e = to_date(start), to_date(end)
    out = []
    while d <= e:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def align(pts, calendar, max_stale_days):
    """Forward-fill `pts` onto `calendar`; None when the latest obs is older than max_stale_days."""
    out = [None] * len(calendar)
    if not pts:
        return out
    dates = [d for d, _ in pts]
    cal_d = [to_date(c) for c in calendar]
    pt_d = [to_date(d) for d in dates]
    for i, c in enumerate(calendar):
        k = bisect.bisect_right(dates, c) - 1
        if k >= 0 and (cal_d[i] - pt_d[k]).days <= max_stale_days:
            out[i] = pts[k][1]
    return out


# ───────────────────────────────────────────────────────── derived inputs ──
def derive_inputs(raw):
    """Add spread/derived raw series used by the component specs."""
    def diff(a, b):
        ma, mb = dict(raw.get(a, [])), dict(raw.get(b, []))
        return sorted([[d, ma[d] - mb[d]] for d in ma if d in mb])
    raw["vix_minus_vxv"] = diff("VIXCLS", "VXVCLS")
    raw["sofr_minus_iorb"] = diff("SOFR", "IORB")
    raw["ccc_minus_bb"] = diff("BAMLH0A3HYC", "BAMLH0A1HYBB") if "BAMLH0A1HYBB" in raw else diff("BAMLH0A3HYC", "BAMLH0A0HYM2")
    return raw


# ────────────────────────────────────────────────────────────── compute ──
def build_components(raw, specs, calendar):
    """Return dict id -> {spec..., 'pct': aligned percentile list, 'raw_last', 'n'}."""
    comps = OrderedDict()
    for cid, pillar, label, src, kind, fresh, freq in specs:
        pts = raw.get(src) or []
        tr = transform(pts, kind) if pts else []
        pct = expanding_percentile(tr, MIN_HISTORY.get(freq, 250)) if tr else []
        comps[cid] = {
            "id": cid, "pillar": pillar, "label": label, "source": src, "transform": kind, "freq": freq,
            "fresh_days": fresh, "n_obs": len(tr),
            "first": tr[0][0] if tr else None, "last": tr[-1][0] if tr else None,
            "raw_last": tr[-1][1] if tr else None,
            "pct_last": pct[-1][1] if pct else None,
            "pct_series": pct,
            "pct_aligned": align(pct, calendar, fresh),
            "raw_aligned": align(tr, calendar, fresh),
        }
    return comps


def pillar_scores(comps, calendar, labels):
    n = len(calendar)
    out = OrderedDict((p, [None] * n) for p in labels)
    counts = OrderedDict((p, [0] * n) for p in labels)
    for p in labels:
        members = [c for c in comps.values() if c["pillar"] == p]
        for i in range(n):
            vals = [c["pct_aligned"][i] for c in members if c["pct_aligned"][i] is not None]
            if vals:
                out[p][i] = sum(vals) / len(vals)
                counts[p][i] = len(vals)
    return out, counts


def composite(pillars, weights, calendar, min_pillars=3):
    n = len(calendar)
    out = [None] * n
    for i in range(n):
        num = den = 0.0
        k = 0
        for p, w in weights.items():
            v = pillars.get(p, [None] * n)[i]
            if v is not None:
                num += w * v; den += w; k += 1
        if k >= min_pillars and den > 0:
            out[i] = num / den
    return out


def rolling_pct(series, win=REL_WINDOW, min_n=252):
    """Percentile of each value within the trailing `win` observations (point-in-time)."""
    out = [None] * len(series)
    buf, q = [], []
    for i, v in enumerate(series):
        if v is None:
            continue
        bisect.insort(buf, v)
        q.append(v)
        if len(q) > win:
            buf.pop(bisect.bisect_left(buf, q.pop(0)))
        if len(q) >= min_n:
            lo, hi = bisect.bisect_left(buf, v), bisect.bisect_right(buf, v)
            out[i] = 100.0 * (lo + 0.5 * (hi - lo)) / len(buf)
    return out


def sub_composite(comps, ids, calendar, min_n=2):
    n = len(calendar)
    out = [None] * n
    for i in range(n):
        vals = [comps[k]["pct_aligned"][i] for k in ids if k in comps and comps[k]["pct_aligned"][i] is not None]
        if len(vals) >= min_n:
            out[i] = sum(vals) / len(vals)
    return out


def breadth(comps, calendar, hi=80.0):
    n = len(calendar)
    out = [None] * n
    for i in range(n):
        vals = [c["pct_aligned"][i] for c in comps.values() if c["pct_aligned"][i] is not None]
        if len(vals) >= 5:
            out[i] = 100.0 * sum(1 for v in vals if v >= hi) / len(vals)
    return out


# ──────────────────────────────────────────────────────────── scorecard ──
def _idx(calendar, date):
    return min(bisect.bisect_left(calendar, date), len(calendar) - 1)


def _window_max(series, calendar, start, end):
    a, b = _idx(calendar, start), _idx(calendar, end)
    vals = [(series[i], calendar[i]) for i in range(a, min(b, len(series) - 1) + 1) if series[i] is not None]
    return max(vals) if vals else (None, None)


def in_windows(calendar, windows):
    flags = [False] * len(calendar)
    for s, e in windows:
        a, b = _idx(calendar, s), _idx(calendar, e)
        for i in range(a, b + 1):
            flags[i] = True
    return flags


def separation(series, flags, calendar, start, end):
    a, b = _idx(calendar, start), _idx(calendar, end)
    ins, outs = [], []
    for i in range(a, b + 1):
        v = series[i]
        if v is None:
            continue
        (ins if flags[i] else outs).append(v)
    if not ins or not outs:
        return None
    return (sum(ins) / len(ins)) - (sum(outs) / len(outs))


def f1_at(series, flags, calendar, thr, start, end):
    a, b = _idx(calendar, start), _idx(calendar, end)
    tp = fp = fn = 0
    for i in range(a, b + 1):
        v = series[i]
        if v is None:
            continue
        alert = v >= thr
        if alert and flags[i]:
            tp += 1
        elif alert:
            fp += 1
        elif flags[i]:
            fn += 1
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return {"thr": thr, "precision": round(100 * prec, 1), "recall": round(100 * rec, 1), "f1": round(100 * f1, 1),
            "alert_days": tp + fp, "episode_days": tp + fn}


def episode_scorecard(stress, froth, calendar, episodes, stress_rel=None, froth_rel=None, lead_days=365,
                      alert=STRESS_ALERT, froth_alert=FROTH_ALERT, rel_zone=REL_ZONE):
    """One row per labelled episode: how the composites behaved before the peak and into the trough.
    Purely retrospective description; thresholds are the frozen constants."""
    rows = []
    n = len(calendar)
    stress_rel = stress_rel or [None] * n
    froth_rel = froth_rel or [None] * n
    for ep in episodes:
        peak, trough, name = ep[0], ep[1], ep[2]
        if peak < calendar[0] or peak > calendar[-1]:
            continue
        ip, it = _idx(calendar, peak), _idx(calendar, trough)
        lead_start = (to_date(peak) - dt.timedelta(days=lead_days)).isoformat()
        s_max, s_max_d = _window_max(stress, calendar, peak, trough)
        sr_max, sr_max_d = _window_max(stress_rel, calendar, peak, trough)
        f_max, f_max_d = _window_max(froth, calendar, lead_start, peak)
        fr_max, fr_max_d = _window_max(froth_rel, calendar, lead_start, peak)
        first_alert = next((calendar[i] for i in range(ip, it + 1) if stress[i] is not None and stress[i] >= alert), None)
        first_froth = next((calendar[i] for i in range(_idx(calendar, lead_start), ip + 1)
                            if froth_rel[i] is not None and froth_rel[i] >= rel_zone), None)
        # where the stress maximum sits relative to the actual trough (+-60 days search)
        lo_s = (to_date(trough) - dt.timedelta(days=60)).isoformat()
        hi_s = (to_date(trough) + dt.timedelta(days=60)).isoformat()
        _, s_lo_d = _window_max(stress, calendar, lo_s, hi_s)
        r = lambda v: None if v is None else round(v, 1)  # noqa: E731
        rows.append({
            "episode": name, "peak": peak, "trough": trough,
            "froth_at_peak": r(froth[ip]), "froth_rel_at_peak": r(froth_rel[ip]),
            "froth_max_pre_peak": r(f_max), "froth_max_date": f_max_d,
            "froth_rel_max_pre_peak": r(fr_max), "froth_rel_max_date": fr_max_d,
            "froth_zone_first": first_froth,
            "froth_zone_lead_days": (to_date(peak) - to_date(first_froth)).days if first_froth else None,
            "stress_at_peak": r(stress[ip]),
            "stress_alert_first": first_alert,
            "stress_alert_lag_days": (to_date(first_alert) - to_date(peak)).days if first_alert else None,
            "stress_max": r(s_max), "stress_max_date": s_max_d,
            "stress_rel_max": r(sr_max), "stress_rel_max_date": sr_max_d,
            "stress_at_trough": r(stress[it]), "stress_rel_at_trough": r(stress_rel[it]),
            "stress_peak_minus_trough_days": (to_date(s_lo_d) - to_date(trough)).days if s_lo_d else None,
            "stress_alert_hit": bool(s_max is not None and s_max >= alert),
            "stress_extreme_hit": bool(s_max is not None and s_max >= STRESS_EXTREME),
            "stress_zone_hit": bool(sr_max is not None and sr_max >= rel_zone),
            "froth_alert_hit": bool(f_max is not None and f_max >= froth_alert),
            "froth_zone_hit": bool(fr_max is not None and fr_max >= rel_zone),
        })
    return rows


def scorecard_summary(rows):
    n = len(rows)
    if not n:
        return {}
    cnt = lambda k: sum(1 for r in rows if r.get(k))  # noqa: E731
    lags = [r["stress_peak_minus_trough_days"] for r in rows if r.get("stress_peak_minus_trough_days") is not None]
    leads = [r["froth_zone_lead_days"] for r in rows if r.get("froth_zone_lead_days") is not None]
    return {"episodes": n,
            "stress_alert_hit": cnt("stress_alert_hit"), "stress_extreme_hit": cnt("stress_extreme_hit"),
            "stress_zone_hit": cnt("stress_zone_hit"),
            "froth_alert_hit": cnt("froth_alert_hit"), "froth_zone_hit": cnt("froth_zone_hit"),
            "median_abs_days_stress_peak_vs_trough": sorted(abs(x) for x in lags)[len(lags) // 2] if lags else None,
            "median_froth_zone_lead_days": sorted(leads)[len(leads) // 2] if leads else None}


def false_alarms(series, flags, calendar, thr, lookahead_days, start, end):
    """Share of alert-days NOT inside an episode and not followed by one within lookahead."""
    a, b = _idx(calendar, start), _idx(calendar, end)
    n = len(calendar)
    nxt = [None] * n  # index of next flagged day
    last = None
    for i in range(n - 1, -1, -1):
        if flags[i]:
            last = i
        nxt[i] = last
    alerts = good = 0
    for i in range(a, b + 1):
        v = series[i]
        if v is None or v < thr:
            continue
        alerts += 1
        j = nxt[i]
        if j is not None and (to_date(calendar[j]) - to_date(calendar[i])).days <= lookahead_days:
            good += 1
    return {"alert_days": alerts, "followed_or_inside": good,
            "false_alarm_pct": round(100.0 * (alerts - good) / alerts, 1) if alerts else None}


def calibrate(pillars, flags, calendar, labels, train_end=TRAIN_END, grid=(0.5, 1.0, 1.5, 2.0, 3.0), passes=3):
    """Coordinate-ascent over pillar weights maximising train-window separation.
    Pure description of what fitted the past; frozen into STRESS_WEIGHTS by hand."""
    weights = {p: 1.0 for p in labels}
    start = calendar[0]
    best = separation(composite(pillars, weights, calendar), flags, calendar, start, train_end) or -1e9
    for _ in range(passes):
        improved = False
        for p in labels:
            for g in grid:
                trial = dict(weights); trial[p] = g
                sc = separation(composite(pillars, trial, calendar), flags, calendar, start, train_end)
                if sc is not None and sc > best + 1e-9:
                    best, weights, improved = sc, trial, True
        if not improved:
            break
    return weights, best


def best_threshold(series, flags, calendar, start, end, grid=range(50, 96, 5)):
    rows = [f1_at(series, flags, calendar, float(t), start, end) for t in grid]
    return max(rows, key=lambda r: r["f1"]), rows
