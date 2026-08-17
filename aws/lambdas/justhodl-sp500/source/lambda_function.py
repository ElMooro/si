"""justhodl-sp500 v1.1.0 — THE S&P 500 AS A SINGLE STOCK.

Khalid (2026-08-16): "give me all the sp500 metrics as a whole — its p/e,
forward p/e, yield, everything — so when I buy a stock I can compare its
metrics to the index to see if it's worth buying."

Every metric an individual name carries, computed for the index itself,
plus the member distribution behind it so any stock can be percentile-
ranked against the S&P in one read.

MATH (index-as-a-stock, the same construction S&P/Bloomberg use):
  index ratio = SIGMA(cap_i) / SIGMA(component_i). Components are backed
  out per member from the fundamental-census matrix at its snapshot
  (earnings_i = cap_i x earnings_yield_i — sign-preserving, so LOSERS ARE
  IN THE DENOMINATOR like the real index), then caps are repriced daily
  with the spx-ma member-closes ledger (fixed fundamentals x moving
  price). Aggregate = harmonic cap-weighting by identity; median/deciles
  are computed across members (valuation ratios: positive-denominator
  members only, standard convention — stated in diag).

SOURCES (all real, all in-house — zero FMP calls, immune to the 401):
  data/fundamental-census-matrix.json   fundamentals per member (mcap,
    ratios, est_* NTM analyst consensus) — cadence 1st+15th 06:00 UTC
  spx-ma/member-closes.json             daily member closes (reprice)
  data/spx-ma.json                      ^GSPC level + MA regime
  FRED API (FRED_API_KEY env)           DGS10/DGS2/CPIAUCSL -> ERP,
                                        Rule-of-20, yield gap, 2s10s

OUT  data/sp500.json          full doc: index / valuation / forward /
                              yield / quality / growth / balance /
                              macro_cross / members / diag / provenance
     data/sp500-history.json  daily headline ledger, merge-on-write
                              upsert by date — PERMANENT, never deleted
MODES  {}                     full run (scheduled daily 21:45 UTC)
       {"compare":"NVDA"}     stock vs index side-by-side + percentiles
Real data only; every layer degrades independently and reports itself.
"""
import gzip
import json
import math
import os
import urllib.request
from bisect import bisect_left
from datetime import datetime, timezone

import boto3

VERSION = "1.1.0"
MARKER = "sp500 v1.1.0"
BUCKET = "justhodl-dashboard-live"
MATRIX_KEY = "data/fundamental-census-matrix.json"
LEDGER_KEY = "spx-ma/member-closes.json"
SPXMA_KEY = "data/spx-ma.json"
OUT_KEY = "data/sp500.json"
HIST_KEY = "data/sp500-history.json"

s3 = boto3.client("s3")


# ---------------------------------------------------------------- io --
def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        print("[io]", key, type(e).__name__, str(e)[:80])
        return None


def _fred(series, n, diag):
    k = os.environ.get("FRED_API_KEY")
    if not k:
        diag.setdefault("fred", "FRED_API_KEY absent")
        return []
    url = ("https://api.stlouisfed.org/fred/series/observations?series_id="
           "%s&api_key=%s&file_type=json&sort_order=desc&limit=%d"
           % (series, k, n))
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            obs = json.loads(r.read()).get("observations") or []
        out = []
        for o in obs:
            try:
                out.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                continue
        return out
    except Exception as e:  # noqa: BLE001
        diag.setdefault("fred", "%s: %s" % (series, str(e)[:70]))
        return []


# ------------------------------------------------------------- maths --
def fnum(x):
    return x if isinstance(x, (int, float)) and math.isfinite(x) else None


def _q(sv, p):
    n = len(sv)
    if not n:
        return None
    if n == 1:
        return sv[0]
    k = (n - 1) * p
    f = int(k)
    c = min(f + 1, n - 1)
    return sv[f] + (sv[c] - sv[f]) * (k - f)


def dist(vals, dp=2):
    sv = sorted(v for v in vals if fnum(v) is not None)
    if len(sv) < 20:
        return {"n": len(sv)}
    return {"n": len(sv),
            "p10": round(_q(sv, .10), dp), "p25": round(_q(sv, .25), dp),
            "median": round(_q(sv, .50), dp),
            "p75": round(_q(sv, .75), dp), "p90": round(_q(sv, .90), dp)}


def pctile(sv, x):
    if not sv or fnum(x) is None:
        return None
    return round(100.0 * bisect_left(sv, x) / len(sv), 1)


# ------------------------------------------------------------- build --
NEED = ["mcap", "pe_ttm", "earnings_yield_pct", "ps_ttm", "pb",
        "ev_sales_ttm", "ev_ebitda_ttm", "fcf_yield_pct",
        "dividend_yield_pct", "net_buyback_yield_pct",
        "shareholder_yield_pct", "payout_ratio_pct", "peg_ttm",
        "pe_fwd", "ps_fwd", "ev_ebitda_fwd",
        "roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
        "operating_margin_pct", "net_margin_pct", "fcf_margin_pct",
        "revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
        "eps_cagr_3y_pct", "debt_to_equity", "netdebt_to_ebitda_ttm",
        "current_ratio", "interest_coverage_ttm", "altman_z",
        "piotroski_f", "income_quality", "sbc_to_revenue_pct",
        "beta_2y", "mom_6m_pct", "mom_12_1_pct"]


def load_scales(tickers, census_iso, diag):
    """s_i = px_today / px_at_census_date per member, from the spx-ma
    daily-closes ledger. Missing member -> 1.0 (counted honestly)."""
    led = _g(LEDGER_KEY) or {}
    dates = led.get("dates") or []
    closes = led.get("closes") or {}
    if not dates or not closes:
        diag["reprice"] = "ledger unavailable -> census-date prices"
        return {t: 1.0 for t in tickers}, None, 0, {}
    cdate = (census_iso or "")[:10]
    ci = 0
    for i, d in enumerate(dates):
        if d <= cdate:
            ci = i
    li = len(dates) - 1
    scales, pxc, hit = {}, {}, 0
    for t in tickers:
        row = closes.get(t) or []
        a = fnum(row[ci]) if len(row) > ci else None
        b = fnum(row[li]) if len(row) > li else None
        if a and b and a > 0:
            scales[t] = b / a
            pxc[t] = a
            hit += 1
        else:
            scales[t] = 1.0
    diag["reprice"] = {"census_px_date": dates[ci], "now_px_date":
                       dates[li], "members_repriced": hit,
                       "members_flat": len(tickers) - hit}
    return scales, dates[li], hit, pxc


def compute(diag):
    mx = _g(MATRIX_KEY)
    if not mx or not mx.get("tickers"):
        raise RuntimeError("fundamental-census-matrix unavailable")
    tickers = mx["tickers"]
    cols = mx.get("cols") or {}
    sectors = mx.get("sectors") or [None] * len(tickers)
    gen = mx.get("generated_at") or ""
    missing = [k for k in NEED if k not in cols]
    diag["census"] = {"generated_at": gen, "n_tickers": len(tickers),
                      "cols_missing": missing}
    n = len(tickers)

    def C(k):
        v = cols.get(k) or [None] * n
        return [fnum(x) for x in v]

    scales, px_date, _, pxc = load_scales(tickers, gen, diag)
    mcap, ey, pe, ps, pb = (C("mcap"), C("earnings_yield_pct"),
                            C("pe_ttm"), C("ps_ttm"), C("pb"))
    evs, eve, fcfy = C("ev_sales_ttm"), C("ev_ebitda_ttm"), C("fcf_yield_pct")
    dy, bby, shy = (C("dividend_yield_pct"), C("net_buyback_yield_pct"),
                    C("shareholder_yield_pct"))
    # NTM components backed out of the fundamental-graphs derived
    # forward ratios (real analyst consensus; est_* raw is _lv-excluded)
    pef_c, psf_c, evef_c = C("pe_fwd"), C("ps_fwd"), C("ev_ebitda_fwd")

    # per-member components at census snapshot + repriced cap ---------
    cap_c = [m if (m and m > 0) else None for m in mcap]
    sc = [scales.get(t, 1.0) for t in tickers]
    cap_n = [c * sc[i] if c else None for i, c in enumerate(cap_c)]
    total_cap = sum(c for c in cap_n if c)

    def comp_yield(ycol, null_zero=False):
        out, nz = [], 0
        for i, c in enumerate(cap_c):
            y = ycol[i]
            if c is None:
                out.append(None)
            elif y is None:
                out.append(0.0 if null_zero else None)
                nz += null_zero
            else:
                out.append(c * y / 100.0)
        return out, nz

    ni, _ = comp_yield(ey)                       # signed: losers included
    for i in range(n):                            # fallback via pe>0
        if ni[i] is None and cap_c[i] and pe[i] and pe[i] > 0:
            ni[i] = cap_c[i] / pe[i]
    fcf, _ = comp_yield(fcfy)
    div, dz = comp_yield(dy, null_zero=True)      # non-payer = 0 (index)
    bb, bz = comp_yield(bby, null_zero=True)
    diag["null_as_zero"] = {"dividend": dz, "buyback": bz}
    rev = [cap_c[i] / ps[i] if cap_c[i] and ps[i] and ps[i] > 0 else None
           for i in range(n)]
    book = [cap_c[i] / pb[i] if cap_c[i] and pb[i] and pb[i] > 0 else None
            for i in range(n)]
    ev_c = [evs[i] * rev[i] if evs[i] and rev[i] else None
            for i in range(n)]
    ebitda = [ev_c[i] / eve[i] if ev_c[i] and eve[i] and eve[i] > 0
              else None for i in range(n)]
    ev_n = [ev_c[i] + cap_c[i] * (sc[i] - 1.0)
            if ev_c[i] is not None and cap_c[i] else None
            for i in range(n)]
    # (evsl_m defined after R below)
    ebd_f = [ev_c[i] / evef_c[i] if ev_c[i] and evef_c[i]
             and evef_c[i] > 0 else None for i in range(n)]
    ni_f = [cap_c[i] / pef_c[i] if cap_c[i] and pef_c[i]
            and pef_c[i] > 0 else None for i in range(n)]
    rev_f = [cap_c[i] / psf_c[i] if cap_c[i] and psf_c[i]
             and psf_c[i] > 0 else None for i in range(n)]
    diag["forward_source"] = ("matrix pe_fwd/ps_fwd/ev_ebitda_fwd "
                              "(NTM consensus, %d members w/ pe_fwd)"
                              % sum(1 for v in ni_f if v is not None))

    # ---------------------------------------------------- aggregates --
    def AGG(nums, dens, dp=2, floor=None):
        sn = sd = cv = 0.0
        k = 0
        for i in range(n):
            a, b = nums[i], dens[i]
            if a is None or b is None:
                continue
            sn += a
            sd += b
            cv += cap_n[i] or 0
            k += 1
        if k < 50 or sd == 0:
            return None, k, 0.0
        v = sn / sd
        if floor is not None and v < floor:
            return None, k, 0.0
        return round(v, dp), k, round(100 * cv / total_cap, 1)

    def M(agg_t, per_member, unit, method, dp=2):
        a, k, cov = agg_t
        d = dist(per_member, dp)
        d.update({"agg": a, "n_agg": k, "cap_cov_pct": cov,
                  "unit": unit, "method": method})
        return d

    # per-member repriced ratios (distributions + members block) ------
    def R(num_c, den_c, inv=False, pos=True):
        out = []
        for i in range(n):
            a, b = num_c[i], den_c[i]
            if a is None or b is None or (pos and b <= 0):
                out.append(None)
            else:
                out.append(b / a * 100 if inv else a / b)
        return out

    pe_m = R(cap_n, ni)
    ey_m = R(cap_n, ni, inv=True, pos=False)
    fpe_m = R(cap_n, ni_f)
    ps_m = R(cap_n, rev)
    pb_m = R(cap_n, book)
    pfcf_m = R(cap_n, fcf)
    fcfy_m = R(cap_n, fcf, inv=True, pos=False)
    dy_m = [div[i] / cap_n[i] * 100 if cap_n[i] and div[i] is not None
            else None for i in range(n)]
    eve_m = R(ev_n, ebitda)
    fevebd_m = R(ev_n, ebd_f)
    fps_m = R(cap_n, rev_f)
    bby_m = [bb[i] / cap_n[i] * 100 if cap_n[i] and bb[i] is not None
             else None for i in range(n)]
    shy_m = [((dy_m[i] or 0.0) + (bby_m[i] or 0.0))
             if dy_m[i] is not None or bby_m[i] is not None else None
             for i in range(n)]
    evsl_m = R(ev_n, rev)
    ntmg_m = [round((pe_m[i] / fpe_m[i] - 1) * 100, 1)
              if pe_m[i] and fpe_m[i] else None for i in range(n)]

    pe_agg = AGG(cap_n, ni, floor=0)
    fpe_agg = AGG(cap_n, ni_f, floor=0)
    ey_a = round(100.0 / pe_agg[0], 2) if pe_agg[0] else None
    fey_a = round(100.0 / fpe_agg[0], 2) if fpe_agg[0] else None
    ntm_g = (round((fpe_agg and pe_agg[0] / fpe_agg[0] - 1) * 100, 1)
             if pe_agg[0] and fpe_agg[0] else None)

    valuation = {
        "pe_ttm": M(pe_agg, pe_m, "x",
                    "SIGMA cap / SIGMA earnings (losers included)"),
        "earnings_yield_pct": {"agg": ey_a, **dist(ey_m),
                               "unit": "%", "method": "100/pe_agg"},
        "ps_ttm": M(AGG(cap_n, rev), ps_m, "x", "SIGMA cap/SIGMA revenue"),
        "pb": M(AGG(cap_n, book), pb_m, "x", "SIGMA cap/SIGMA book"),
        "p_fcf_ttm": M(AGG(cap_n, fcf, floor=0), pfcf_m, "x",
                       "SIGMA cap/SIGMA FCF"),
        "fcf_yield_pct": M((round(100 * sum(f for f in fcf if f) /
                                  sum(cap_n[i] for i in range(n)
                                      if fcf[i] is not None and cap_n[i]),
                                  2) if any(fcf) else None,
                            sum(1 for f in fcf if f is not None), None),
                           fcfy_m, "%", "SIGMA FCF/SIGMA cap"),
        "ev_ebitda_ttm": M(AGG(ev_n, ebitda, floor=0), eve_m, "x",
                           "SIGMA EV/SIGMA EBITDA"),
        "ev_sales_ttm": M(AGG(ev_n, rev), evsl_m, "x",
                          "SIGMA EV/SIGMA revenue"),
        "peg_ttm": M((None, 0, None), C("peg_ttm"), "x",
                     "member distribution (agg n/a)"),
    }
    forward = {
        "pe_fwd": M(fpe_agg, fpe_m, "x",
                    "SIGMA cap / SIGMA NTM consensus net income"),
        "earnings_yield_fwd_pct": {"agg": fey_a, "unit": "%",
                                   "method": "100/pe_fwd"},
        "ps_fwd": M(AGG(cap_n, rev_f), fps_m, "x",
                    "SIGMA cap/SIGMA NTM consensus revenue"),
        "ev_ebitda_fwd": M(AGG(ev_n, ebd_f, floor=0), fevebd_m, "x",
                           "SIGMA EV/SIGMA NTM consensus EBITDA"),
        "ntm_earnings_growth_pct": {"agg": ntm_g, "unit": "%",
                                    "method": "pe_ttm/pe_fwd - 1"},
    }
    dagg = (round(100 * sum(d for d in div if d is not None) /
                  sum(cap_n[i] for i in range(n)
                      if div[i] is not None and cap_n[i]), 2)
            if total_cap else None)
    bagg = (round(100 * sum(b for b in bb if b is not None) /
                  sum(cap_n[i] for i in range(n)
                      if bb[i] is not None and cap_n[i]), 2)
            if total_cap else None)
    po = AGG([div[i] for i in range(n)], ni, dp=1, floor=0)
    yld = {
        "dividend_yield_pct": {"agg": dagg, **dist(dy_m),
                               "unit": "%",
                               "method": "SIGMA dividends/SIGMA cap "
                                         "(non-payers=0)"},
        "net_buyback_yield_pct": {"agg": bagg, **dist(C(
            "net_buyback_yield_pct")), "unit": "%",
            "method": "SIGMA net buybacks/SIGMA cap"},
        "shareholder_yield_pct": {"agg": (round(dagg + bagg, 2)
                                          if dagg is not None and
                                          bagg is not None else None),
                                  **dist(C("shareholder_yield_pct")),
                                  "unit": "%", "method": "div + buyback"},
        "payout_ratio_pct": {"agg": (round(po[0] * 100, 1)
                                     if po[0] else None),
                             **dist(C("payout_ratio_pct"), 1),
                             "unit": "%",
                             "method": "SIGMA dividends/SIGMA earnings"},
    }

    def WM(key, dp=1):
        col = C(key)
        sv = sorted(v for v in col if v is not None)
        lo = _q(sv, .02) if len(sv) >= 50 else None
        hi = _q(sv, .98) if len(sv) >= 50 else None
        sn = sw = 0.0
        for i in range(n):
            if col[i] is not None and cap_n[i]:
                v = col[i]
                if lo is not None:
                    v = min(max(v, lo), hi)
                sn += v * cap_n[i]
                sw += cap_n[i]
        a = round(sn / sw, dp) if sw else None
        d = dist(col, dp)
        d.update({"agg": a, "cap_cov_pct":
                  round(100 * sw / total_cap, 1) if total_cap else 0,
                  "unit": "%", "method": "cap-weighted mean, winsorized p2-p98"})
        return d

    quality = {k: WM(k) for k in
               ("roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
                "operating_margin_pct", "net_margin_pct",
                "fcf_margin_pct", "income_quality",
                "sbc_to_revenue_pct")}
    quality["piotroski_f"] = WM("piotroski_f", 2)
    # component-correct index aggregates (ratio-of-sums, not mean):
    for name, num_a, den_a in (("roe_pct", ni, book),
                               ("net_margin_pct", ni, rev),
                               ("fcf_margin_pct", fcf, rev)):
        a, k, cov = AGG(num_a, den_a, dp=4)
        if a is not None:
            quality[name]["agg"] = round(a * 100, 1)
            quality[name]["cap_cov_pct"] = cov
            quality[name]["method"] = ("SIGMA %s / SIGMA %s x100"
                                       % (("NI", "book")
                                          if name == "roe_pct" else
                                          ("NI", "revenue")
                                          if name == "net_margin_pct"
                                          else ("FCF", "revenue")))
    growth = {k: WM(k) for k in
              ("revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
               "eps_cagr_3y_pct")}
    balance = {k: WM(k, 2) for k in
               ("debt_to_equity", "netdebt_to_ebitda_ttm",
                "current_ratio", "interest_coverage_ttm", "altman_z",
                "beta_2y")}

    mem_spec = [
        ("px_chg_since_census_pct",
         [round((sc[i] - 1) * 100, 1) for i in range(n)], None),
        ("sector", sectors, None),
        ("mcap", cap_n, 1),
        ("pe_ttm", pe_m, 1), ("pe_fwd", fpe_m, 1),
        ("peg_ttm", C("peg_ttm"), 2),
        ("ps_ttm", ps_m, 2), ("ps_fwd", fps_m, 2), ("pb", pb_m, 2),
        ("ev_ebitda_ttm", eve_m, 1), ("ev_ebitda_fwd", fevebd_m, 1),
        ("ev_sales_ttm", evsl_m, 2),
        ("earnings_yield_pct", ey_m, 2), ("fcf_yield_pct", fcfy_m, 2),
        ("div_yield_pct", dy_m, 2), ("buyback_yield_pct", bby_m, 2),
        ("shareholder_yield_pct", shy_m, 2),
        ("payout_ratio_pct", C("payout_ratio_pct"), 1),
        ("roe_pct", C("roe_pct"), 1), ("roic_pct", C("roic_pct"), 1),
        ("roa_pct", C("roa_pct"), 1),
        ("gross_margin_pct", C("gross_margin_pct"), 1),
        ("operating_margin_pct", C("operating_margin_pct"), 1),
        ("net_margin_pct", C("net_margin_pct"), 1),
        ("fcf_margin_pct", C("fcf_margin_pct"), 1),
        ("income_quality", C("income_quality"), 2),
        ("sbc_to_revenue_pct", C("sbc_to_revenue_pct"), 2),
        ("piotroski_f", C("piotroski_f"), 2),
        ("altman_z", C("altman_z"), 2),
        ("revenue_yoy_pct", C("revenue_yoy_pct"), 1),
        ("eps_yoy_pct", C("eps_yoy_pct"), 1),
        ("revenue_cagr_3y_pct", C("revenue_cagr_3y_pct"), 1),
        ("eps_cagr_3y_pct", C("eps_cagr_3y_pct"), 1),
        ("ntm_growth_pct", ntmg_m, 1),
        ("debt_to_equity", C("debt_to_equity"), 2),
        ("netdebt_to_ebitda_ttm", C("netdebt_to_ebitda_ttm"), 2),
        ("current_ratio", C("current_ratio"), 2),
        ("interest_coverage_ttm", C("interest_coverage_ttm"), 1),
        ("beta_2y", C("beta_2y"), 2),
        ("mom_6m_pct", C("mom_6m_pct"), 1),
        ("mom_12_1_pct", C("mom_12_1_pct"), 1),
    ]
    assert [x[0] for x in mem_spec] == MEMBER_FIELDS, "spec/fields drift"
    members = {}
    for i, t in enumerate(tickers):
        if not cap_n[i]:
            continue
        row = []
        for _nm, lst, dp in mem_spec:
            v = lst[i]
            if dp is not None and v is not None:
                v = round(fnum(v), dp) if fnum(v) is not None else None
            row.append(v)
        members[t] = row
    return {"tickers": tickers, "total_cap": total_cap,
            "px_date": px_date, "gen": gen,
            "valuation": valuation, "forward": forward, "yield": yld,
            "quality": quality, "growth": growth, "balance": balance,
            "members": members,
            "_arrays": {nm: lst for nm, lst, _ in mem_spec
                        if nm != "sector"}}


def macro_cross(val, fwd, yld, diag):
    d10 = _fred("DGS10", 8, diag)
    d02 = _fred("DGS2", 8, diag)
    cpi = _fred("CPIAUCSL", 14, diag)
    g10 = d10[0][1] if d10 else None
    g02 = d02[0][1] if d02 else None
    yoy = (round((cpi[0][1] / cpi[12][1] - 1) * 100, 2)
           if len(cpi) >= 13 and cpi[12][1] else None)
    ey = val["earnings_yield_pct"].get("agg")
    fey = fwd["earnings_yield_fwd_pct"].get("agg")
    dy = yld["dividend_yield_pct"].get("agg")
    pe = val["pe_ttm"].get("agg")
    out = {"us10y_pct": g10, "us2y_pct": g02,
           "curve_2s10s_bp": (round((g10 - g02) * 100)
                              if g10 is not None and g02 is not None
                              else None),
           "cpi_yoy_pct": yoy,
           "erp_ttm_pct": (round(ey - g10, 2)
                           if ey is not None and g10 is not None
                           else None),
           "erp_fwd_pct": (round(fey - g10, 2)
                           if fey is not None and g10 is not None
                           else None),
           "rule_of_20": (round(pe + yoy, 1)
                          if pe is not None and yoy is not None
                          else None),
           "div_yield_minus_10y_pct": (round(dy - g10, 2)
                                       if dy is not None and
                                       g10 is not None else None),
           "asof": {"dgs10": d10[0][0] if d10 else None,
                    "cpi": cpi[0][0] if cpi else None}}
    return out


def bank_history(doc):
    """Merge-on-write upsert by date. PERMANENT — union only."""
    led = _g(HIST_KEY) or {}
    day = doc["as_of"][:10]
    led[day] = {"spx": doc["index"].get("level"),
                "pe": doc["valuation"]["pe_ttm"].get("agg"),
                "fpe": doc["forward"]["pe_fwd"].get("agg"),
                "ey": doc["valuation"]["earnings_yield_pct"].get("agg"),
                "dy": doc["yield"]["dividend_yield_pct"].get("agg"),
                "erp": doc["macro_cross"].get("erp_ttm_pct"),
                "r20": doc["macro_cross"].get("rule_of_20"),
                "cap_t": round(doc["index"].get("total_mcap") or 0, 1)}
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(led, separators=(",", ":")).encode(),
                  ContentType="application/json")
    return len(led)


MEMBER_FIELDS = [
    "px_chg_since_census_pct", "sector", "mcap",
    "pe_ttm", "pe_fwd", "peg_ttm", "ps_ttm", "ps_fwd", "pb",
    "ev_ebitda_ttm", "ev_ebitda_fwd", "ev_sales_ttm",
    "earnings_yield_pct", "fcf_yield_pct",
    "div_yield_pct", "buyback_yield_pct", "shareholder_yield_pct",
    "payout_ratio_pct",
    "roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
    "operating_margin_pct", "net_margin_pct", "fcf_margin_pct",
    "income_quality", "sbc_to_revenue_pct", "piotroski_f", "altman_z",
    "revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
    "eps_cagr_3y_pct", "ntm_growth_pct",
    "debt_to_equity", "netdebt_to_ebitda_ttm", "current_ratio",
    "interest_coverage_ttm", "beta_2y",
    "mom_6m_pct", "mom_12_1_pct"]


def run():
    diag = {}
    cx = compute(diag)
    spx = _g(SPXMA_KEY) or {}
    idx = spx.get("index") or spx
    level = fnum(idx.get("price")) or fnum(spx.get("price"))
    macro = macro_cross(cx["valuation"], cx["forward"], cx["yield"], diag)
    doc = {"ok": True, "engine": "justhodl-sp500", "engine_v": VERSION,
           "marker": MARKER,
           "as_of": datetime.now(timezone.utc).isoformat(),
           "index": {"level": level,
                     "regime": idx.get("regime") or spx.get("regime"),
                     "members": len(cx["members"]),
                     "total_mcap": round(cx["total_cap"], 1),
                     "px_date": cx["px_date"],
                     "fundamentals_asof": cx["gen"][:10]},
           "valuation": cx["valuation"], "forward": cx["forward"],
           "yield": cx["yield"], "quality": cx["quality"],
           "growth": cx["growth"], "balance": cx["balance"],
           "macro_cross": macro,
           "member_fields": MEMBER_FIELDS, "members": cx["members"],
           "diag": diag,
           "provenance": {
               "sources": [MATRIX_KEY, LEDGER_KEY, SPXMA_KEY,
                           "FRED DGS10/DGS2/CPIAUCSL"],
               "method": "index-as-a-stock: SIGMA cap/SIGMA component; "
                         "census fundamentals repriced by daily member "
                         "closes; losers included in earnings; "
                         "valuation distributions over positive-"
                         "denominator members (convention)."}}
    doc["hist_days"] = bank_history(doc)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print("[sp500] pe=%s fpe=%s ey=%s dy=%s erp=%s members=%d" %
          (doc["valuation"]["pe_ttm"].get("agg"),
           doc["forward"]["pe_fwd"].get("agg"),
           doc["valuation"]["earnings_yield_pct"].get("agg"),
           doc["yield"]["dividend_yield_pct"].get("agg"),
           macro.get("erp_ttm_pct"), len(cx["members"])))
    return {"ok": True, "pe": doc["valuation"]["pe_ttm"].get("agg"),
            "members": len(cx["members"])}


COMPARE_MAP = [
    ("pe_ttm", "valuation", "pe_ttm", "low"),
    ("pe_fwd", "forward", "pe_fwd", "low"),
    ("peg_ttm", "valuation", "peg_ttm", "low"),
    ("ps_ttm", "valuation", "ps_ttm", "low"),
    ("ps_fwd", "forward", "ps_fwd", "low"),
    ("pb", "valuation", "pb", "low"),
    ("ev_ebitda_ttm", "valuation", "ev_ebitda_ttm", "low"),
    ("ev_ebitda_fwd", "forward", "ev_ebitda_fwd", "low"),
    ("ev_sales_ttm", "valuation", "ev_sales_ttm", "low"),
    ("earnings_yield_pct", "valuation", "earnings_yield_pct", "high"),
    ("fcf_yield_pct", "valuation", "fcf_yield_pct", "high"),
    ("div_yield_pct", "yield", "dividend_yield_pct", "high"),
    ("buyback_yield_pct", "yield", "net_buyback_yield_pct", "high"),
    ("shareholder_yield_pct", "yield", "shareholder_yield_pct", "high"),
    ("payout_ratio_pct", "yield", "payout_ratio_pct", None),
    ("roe_pct", "quality", "roe_pct", "high"),
    ("roic_pct", "quality", "roic_pct", "high"),
    ("roa_pct", "quality", "roa_pct", "high"),
    ("gross_margin_pct", "quality", "gross_margin_pct", "high"),
    ("operating_margin_pct", "quality", "operating_margin_pct", "high"),
    ("net_margin_pct", "quality", "net_margin_pct", "high"),
    ("fcf_margin_pct", "quality", "fcf_margin_pct", "high"),
    ("income_quality", "quality", "income_quality", "high"),
    ("sbc_to_revenue_pct", "quality", "sbc_to_revenue_pct", "low"),
    ("piotroski_f", "quality", "piotroski_f", "high"),
    ("altman_z", "balance", "altman_z", "high"),
    ("revenue_yoy_pct", "growth", "revenue_yoy_pct", "high"),
    ("eps_yoy_pct", "growth", "eps_yoy_pct", "high"),
    ("revenue_cagr_3y_pct", "growth", "revenue_cagr_3y_pct", "high"),
    ("eps_cagr_3y_pct", "growth", "eps_cagr_3y_pct", "high"),
    ("ntm_growth_pct", "forward", "ntm_earnings_growth_pct", "high"),
    ("debt_to_equity", "balance", "debt_to_equity", "low"),
    ("netdebt_to_ebitda_ttm", "balance", "netdebt_to_ebitda_ttm",
     "low"),
    ("current_ratio", "balance", "current_ratio", "high"),
    ("interest_coverage_ttm", "balance", "interest_coverage_ttm",
     "high"),
    ("beta_2y", "balance", "beta_2y", "low"),
    ("mom_6m_pct", None, None, "high"),
    ("mom_12_1_pct", None, None, "high"),
]

PILLARS = {
    "valuation": ["pe_ttm", "pe_fwd", "peg_ttm", "ps_ttm", "pb",
                  "ev_ebitda_ttm", "ev_sales_ttm",
                  "earnings_yield_pct", "fcf_yield_pct"],
    "quality": ["roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
                "operating_margin_pct", "net_margin_pct",
                "fcf_margin_pct", "income_quality", "piotroski_f",
                "sbc_to_revenue_pct"],
    "growth": ["revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
               "eps_cagr_3y_pct", "ntm_growth_pct"],
    "balance": ["debt_to_equity", "netdebt_to_ebitda_ttm",
                "current_ratio", "interest_coverage_ttm", "altman_z",
                "beta_2y"],
    "momentum": ["mom_6m_pct", "mom_12_1_pct"],
}
PILLAR_MIN_N = {"valuation": 3, "quality": 4, "growth": 2,
                "balance": 3, "momentum": 1}
PILLAR_W = {"valuation": .30, "quality": .25, "growth": .25,
            "balance": .10, "momentum": .10}


def _median(sv):
    if not sv:
        return None
    m = len(sv) // 2
    return sv[m] if len(sv) % 2 else (sv[m - 1] + sv[m]) / 2


def compare(ticker):
    diag = {}
    cx = compute(diag)
    t = str(ticker).upper()
    try:
        i = cx["tickers"].index(t)
    except ValueError:
        return {"ok": False, "error": "%s not in the S&P census" % t}
    rows, pcts = [], {}
    for field, grp, ik, better in COMPARE_MAP:
        arr = cx["_arrays"][field]
        sv = sorted(v for v in (fnum(x) for x in arr) if v is not None)
        x = fnum(arr[i])
        if grp:
            ref = cx[grp][ik]
            agg, med = ref.get("agg"), ref.get("median")
        else:
            agg = None
            med = round(_median(sv), 2) if sv else None
        pc = pctile(sv, x)
        pcts[field] = (pc, better)
        prem = verdict = None
        if x is not None and med not in (None, 0):
            prem = (x / med - 1) * 100
            if better == "low":
                verdict = ("CHEAP vs index" if prem < -10 else
                           "RICH vs index" if prem > 10 else "IN LINE")
            elif better == "high":
                verdict = ("STRONGER than index" if prem > 10 else
                           "WEAKER than index" if prem < -10 else
                           "IN LINE")
            prem = round(prem, 1)
        rows.append({"metric": field, "group": grp or "momentum",
                     "stock": x, "spx_agg": agg, "spx_median": med,
                     "premium_vs_median_pct": prem,
                     "percentile_in_spx": pc, "verdict": verdict})
    pillars, comp, wsum = {}, 0.0, 0.0
    for pname, fields in PILLARS.items():
        vals = []
        for f in fields:
            pc, better = pcts.get(f, (None, None))
            if pc is None:
                continue
            vals.append(100 - pc if better == "low" else pc)
        if len(vals) >= PILLAR_MIN_N[pname]:
            scv = round(sum(vals) / len(vals), 1)
            pillars[pname] = {"score": scv, "n": len(vals)}
            comp += scv * PILLAR_W[pname]
            wsum += PILLAR_W[pname]
    composite = round(comp / wsum, 1) if wsum else None
    tagmap = {"valuation": ("cheaper than the index",
                            "richer than the index"),
              "quality": ("higher quality", "lower quality"),
              "growth": ("faster growth", "slower growth"),
              "balance": ("stronger balance sheet",
                          "weaker balance sheet"),
              "momentum": ("stronger momentum", "weaker momentum")}
    tags = []
    for pn, pv in pillars.items():
        if pv["score"] >= 60:
            tags.append(tagmap[pn][0])
        elif pv["score"] <= 40:
            tags.append(tagmap[pn][1])
    if composite is None:
        vtxt = "INSUFFICIENT COVERAGE to score vs SPX"
    elif composite >= 65:
        vtxt = ("STRONG CANDIDATE vs owning SPX -- clears the index "
                "bar on the weighted read")
    elif composite >= 55:
        vtxt = "MODEST EDGE vs SPX"
    elif composite >= 45:
        vtxt = "NO CLEAR EDGE -- SPX is the default"
    else:
        vtxt = "PREFER SPX over this name on these numbers"
    return {"ok": True, "ticker": t, "as_of":
            datetime.now(timezone.utc).isoformat(), "rows": rows,
            "pillars": pillars,
            "composite": {"score": composite,
                          "weights": {k: PILLAR_W[k] for k in pillars},
                          "scale": "0-100 percentile-based, "
                                   "direction-adjusted"},
            "verdict": vtxt, "tags": tags,
            "note": "percentile = rank within S&P members; row verdict "
                    "threshold +/-10% vs member median; SPX is the "
                    "default -- the stock must earn its place"}


def lambda_handler(event, context):
    event = event or {}
    if event.get("compare"):
        return compare(event["compare"])
    return run()
