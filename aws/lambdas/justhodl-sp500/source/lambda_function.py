"""justhodl-sp500 v1.0.0 — THE S&P 500 AS A SINGLE STOCK.

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

VERSION = "1.0.0"
MARKER = "sp500 v1.0.0"
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
        "est_net_income_avg", "est_revenue_avg", "est_ebitda_avg",
        "roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
        "operating_margin_pct", "net_margin_pct", "fcf_margin_pct",
        "revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
        "eps_cagr_3y_pct", "debt_to_equity", "netdebt_to_ebitda_ttm",
        "current_ratio", "interest_coverage_ttm", "altman_z",
        "piotroski_f", "income_quality", "sbc_to_revenue_pct",
        "beta_2y"]


def load_scales(tickers, census_iso, diag):
    """s_i = px_today / px_at_census_date per member, from the spx-ma
    daily-closes ledger. Missing member -> 1.0 (counted honestly)."""
    led = _g(LEDGER_KEY) or {}
    dates = led.get("dates") or []
    closes = led.get("closes") or {}
    if not dates or not closes:
        diag["reprice"] = "ledger unavailable -> census-date prices"
        return {t: 1.0 for t in tickers}, None, 0
    cdate = (census_iso or "")[:10]
    ci = 0
    for i, d in enumerate(dates):
        if d <= cdate:
            ci = i
    li = len(dates) - 1
    scales, hit = {}, 0
    for t in tickers:
        row = closes.get(t) or []
        a = fnum(row[ci]) if len(row) > ci else None
        b = fnum(row[li]) if len(row) > li else None
        if a and b and a > 0:
            scales[t] = b / a
            hit += 1
        else:
            scales[t] = 1.0
    diag["reprice"] = {"census_px_date": dates[ci], "now_px_date":
                       dates[li], "members_repriced": hit,
                       "members_flat": len(tickers) - hit}
    return scales, dates[li], hit


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

    scales, px_date, _ = load_scales(tickers, gen, diag)
    mcap, ey, pe, ps, pb = (C("mcap"), C("earnings_yield_pct"),
                            C("pe_ttm"), C("ps_ttm"), C("pb"))
    evs, eve, fcfy = C("ev_sales_ttm"), C("ev_ebitda_ttm"), C("fcf_yield_pct")
    dy, bby, shy = (C("dividend_yield_pct"), C("net_buyback_yield_pct"),
                    C("shareholder_yield_pct"))
    ni_f, rev_f, ebd_f = (C("est_net_income_avg"), C("est_revenue_avg"),
                          C("est_ebitda_avg"))

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
        "ev_sales_ttm": M(AGG(ev_n, rev), R(ev_n, rev), "x",
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
        sn = sw = 0.0
        for i in range(n):
            if col[i] is not None and cap_n[i]:
                sn += col[i] * cap_n[i]
                sw += cap_n[i]
        a = round(sn / sw, dp) if sw else None
        d = dist(col, dp)
        d.update({"agg": a, "cap_cov_pct":
                  round(100 * sw / total_cap, 1) if total_cap else 0,
                  "unit": "%", "method": "cap-weighted mean"})
        return d

    quality = {k: WM(k) for k in
               ("roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
                "operating_margin_pct", "net_margin_pct",
                "fcf_margin_pct", "income_quality",
                "sbc_to_revenue_pct")}
    quality["piotroski_f"] = WM("piotroski_f", 2)
    growth = {k: WM(k) for k in
              ("revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
               "eps_cagr_3y_pct")}
    balance = {k: WM(k, 2) for k in
               ("debt_to_equity", "netdebt_to_ebitda_ttm",
                "current_ratio", "interest_coverage_ttm", "altman_z",
                "beta_2y")}

    members = {}
    for i, t in enumerate(tickers):
        if not cap_n[i]:
            continue
        members[t] = [
            round((sc[i] - 1) * 100, 1), sectors[i],
            round(cap_n[i], 1) if cap_n[i] else None,
            round(pe_m[i], 1) if pe_m[i] else None,
            round(fpe_m[i], 1) if fpe_m[i] else None,
            round(ps_m[i], 2) if ps_m[i] else None,
            round(pb_m[i], 2) if pb_m[i] else None,
            round(eve_m[i], 1) if eve_m[i] else None,
            round(dy_m[i], 2) if dy_m[i] is not None else None,
            round(fcfy_m[i], 2) if fcfy_m[i] is not None else None,
            fnum(C("peg_ttm")[i]),
            fnum(C("roe_pct")[i]), fnum(C("net_margin_pct")[i]),
            fnum(C("revenue_yoy_pct")[i]), fnum(C("eps_yoy_pct")[i])]
    return {"tickers": tickers, "total_cap": total_cap,
            "px_date": px_date, "gen": gen,
            "valuation": valuation, "forward": forward, "yield": yld,
            "quality": quality, "growth": growth, "balance": balance,
            "members": members,
            "_arrays": {"pe": pe_m, "fpe": fpe_m, "ps": ps_m,
                        "pb": pb_m, "eve": eve_m, "dy": dy_m,
                        "fcfy": fcfy_m, "peg": C("peg_ttm"),
                        "roe": C("roe_pct"), "nm": C("net_margin_pct"),
                        "rg": C("revenue_yoy_pct"),
                        "eg": C("eps_yoy_pct")}}


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


MEMBER_FIELDS = ["px_chg_since_census_pct", "sector", "mcap", "pe_ttm",
                 "pe_fwd", "ps_ttm", "pb", "ev_ebitda_ttm",
                 "div_yield_pct", "fcf_yield_pct", "peg_ttm", "roe_pct",
                 "net_margin_pct", "revenue_yoy_pct", "eps_yoy_pct"]


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


COMPARE_MAP = [("pe_ttm", "pe", "valuation", "pe_ttm", True),
               ("pe_fwd", "fpe", "forward", "pe_fwd", True),
               ("ps_ttm", "ps", "valuation", "ps_ttm", True),
               ("pb", "pb", "valuation", "pb", True),
               ("ev_ebitda_ttm", "eve", "valuation", "ev_ebitda_ttm",
                True),
               ("div_yield_pct", "dy", "yield", "dividend_yield_pct",
                False),
               ("fcf_yield_pct", "fcfy", "valuation", "fcf_yield_pct",
                False),
               ("peg_ttm", "peg", "valuation", "peg_ttm", True),
               ("roe_pct", "roe", "quality", "roe_pct", False),
               ("net_margin_pct", "nm", "quality", "net_margin_pct",
                False),
               ("revenue_yoy_pct", "rg", "growth", "revenue_yoy_pct",
                False),
               ("eps_yoy_pct", "eg", "growth", "eps_yoy_pct", False)]


def compare(ticker):
    diag = {}
    cx = compute(diag)
    t = str(ticker).upper()
    try:
        i = cx["tickers"].index(t)
    except ValueError:
        return {"ok": False, "error": "%s not in the S&P census" % t}
    rows = []
    for name, ak, grp, mk, lower_rich in COMPARE_MAP:
        arr = cx["_arrays"][ak]
        sv = sorted(v for v in arr if fnum(v) is not None)
        x = fnum(arr[i])
        ref = cx[grp][mk]
        med = ref.get("median")
        pc = pctile(sv, x)
        verdict = None
        if x is not None and med not in (None, 0):
            prem = (x / med - 1) * 100
            cheap = prem < -10 if lower_rich else prem > 10
            rich = prem > 10 if lower_rich else prem < -10
            verdict = ("CHEAP vs index" if cheap else
                       "RICH vs index" if rich else "IN LINE")
            prem = round(prem, 1)
        else:
            prem = None
        rows.append({"metric": name, "stock": x,
                     "spx_agg": ref.get("agg"), "spx_median": med,
                     "premium_vs_median_pct": prem,
                     "percentile_in_spx": pc, "verdict": verdict})
    return {"ok": True, "ticker": t, "as_of":
            datetime.now(timezone.utc).isoformat(), "rows": rows,
            "note": "percentile = rank within S&P members; verdict "
                    "threshold +/-10% vs member median"}


def lambda_handler(event, context):
    event = event or {}
    if event.get("compare"):
        return compare(event["compare"])
    return run()
