"""justhodl-industry-rotation -- Market regime -> industry-ETF leadership
-> strongest stocks inside the strongest ETFs.

Doctrine (Khalid, 2026-07-07): industry momentum subsumes most individual
stock momentum (Moskowitz-Grinblatt 1999; AQR industry-momentum work).
Find the strongest army first, then the strongest soldiers. The premium
signal is DIVERGENCE UNDER WEAKNESS: SPY below its short MAs while an
industry ETF holds its 50/100/200 ladder with a RISING ETF/SPY ratio --
that is institutional absorption, not luck.

Layers (all real data, zero LLM):
  1. Market regime: SPY vs SMA20/50 -> STRONG / NEUTRAL / WEAK, enriched
     with risk-regime score (failure-isolated).
  2. 33-ETF ladder (11 SPDR sectors + 22 industries): SMA50/100/200
     state, ETF/SPY ratio vs its own 50d MA, 20d ratio slope, 3m
     relative-momentum percentile -> leadership_score 0-100.
  3. Tags: ABSORPTION (weak tape, strong ladder, rising ratio),
     BREAKDOWN (strong tape, broken ladder, falling ratio).
  4. Self-accruing score history -> rank_delta_20d (honest WARMING
     until 21 sessions accrue).
  5. Soldiers: top-5 leader ETFs get holdings via FMP /stable/etf-holder
     (graceful skip) + resilient-name join from data/resilience.json.
  6. by_sector_name map so best-setups can join on harvested sector
     strings.

Out: data/industry-rotation.json (CacheControl 900s).
"""
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-rotation.json"

import os
POLY = (os.environ.get("POLYGON_API_KEY")
        or os.environ.get("POLYGON_KEY") or "")
FMP = (os.environ.get("FMP_API_KEY")
       or os.environ.get("FMP_KEY") or "")

SECTORS = {
    "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
    "XLV": "Health Care", "XLI": "Industrials",
    "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
    "XLU": "Utilities", "XLB": "Materials",
    "XLC": "Communication Services", "XLRE": "Real Estate"}
INDUSTRIES = {
    "SMH": ("Semiconductors", "Technology"),
    "IGV": ("Software", "Technology"),
    "FDN": ("Internet", "Communication Services"),
    "CIBR": ("Cybersecurity", "Technology"),
    "XBI": ("Biotech", "Health Care"),
    "IBB": ("Biotech Large", "Health Care"),
    "ITB": ("Homebuilders", "Consumer Discretionary"),
    "KRE": ("Regional Banks", "Financials"),
    "KBE": ("Banks", "Financials"),
    "XOP": ("Oil E&P", "Energy"),
    "OIH": ("Oil Services", "Energy"),
    "GDX": ("Gold Miners", "Materials"),
    "XME": ("Metals & Mining", "Materials"),
    "XRT": ("Retail", "Consumer Discretionary"),
    "IYT": ("Transports", "Industrials"),
    "ITA": ("Aerospace & Defense", "Industrials"),
    "JETS": ("Airlines", "Industrials"),
    "TAN": ("Solar", "Energy"),
    "URA": ("Uranium", "Energy"),
    "KWEB": ("China Internet", "Communication Services"),
    "LIT": ("Lithium & Battery", "Materials"),
    "PAVE": ("Infrastructure", "Industrials"),
    "XES": ("Oil Equipment & Svcs", "Energy"),
    "COPX": ("Copper Miners", "Materials"),
    "XAR": ("Aerospace & Defense Eq", "Industrials"),
    "GRID": ("Smart Grid / Electrification", "Utilities"),
    "WCLD": ("Cloud Software", "Technology"),
    "XTN": ("Transportation Eq", "Industrials"),
    "XHB": ("Homebuilders Broad", "Consumer Discretionary")}
UNIVERSE = list(SECTORS) + list(INDUSTRIES)


def _http(url, timeout=25, tries=2):
    for a in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-industry-rotation"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0 + a)
    return None


def polygon_daily(tkr, days=520):
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    frm = (datetime.now(timezone.utc) - timedelta(days=days)
           ).strftime("%Y-%m-%d")
    d = _http("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
              "%s/%s?adjusted=true&sort=asc&limit=5000&apiKey=%s"
              % (tkr, frm, to, POLY))
    return [float(b["c"]) for b in (d or {}).get("results") or []]


def s3_json(key):
    try:
        return json.loads(
            S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def sma(v, n):
    return sum(v[-n:]) / n if len(v) >= n else None


def pct_rank(x, pop):
    if x is None or not pop:
        return None
    return round(100.0 * sum(1 for p in pop if p <= x) / len(pop), 1)


def beta_to(etf, spy, n=252):
    m = min(len(etf), len(spy), n + 1)
    if m < 130:
        return None
    re = [etf[-m + i + 1] / etf[-m + i] - 1 for i in range(m - 1)
          if etf[-m + i]]
    rs = [spy[-m + i + 1] / spy[-m + i] - 1 for i in range(m - 1)
          if spy[-m + i]]
    k = min(len(re), len(rs))
    re, rs = re[-k:], rs[-k:]
    mu_e, mu_s = sum(re) / k, sum(rs) / k
    var = sum((x - mu_s) ** 2 for x in rs)
    if var <= 0:
        return None
    return sum((a - mu_e) * (b - mu_s) for a, b in zip(re, rs)) / var


def _ret(c, a, b):
    """% return from index -a to index -b (a>b), None if short."""
    if len(c) < a or not c[-a]:
        return None
    return (c[-b] / c[-a] - 1.0) * 100


def alpha_horizons(etf, spy, b):
    """Jegadeesh-Titman skip-month + beta-adjusted (BAB separation):
    alpha_h = ETF_h - beta*SPY_h, horizons 3m raw, 6m and 12m each
    SKIPPING the most recent month (reversal noise)."""
    if b is None:
        b = 1.0
    out = {}
    for key, a, e in (("3m", 64, 1), ("6m_skip", 148, 22),
                      ("12m1_skip", 274, 22)):
        re_, rs_ = _ret(etf, a, e), _ret(spy, a, e)
        out[key] = (round(re_ - b * rs_, 2)
                    if None not in (re_, rs_) else None)
    w = {"3m": 0.5, "6m_skip": 0.3, "12m1_skip": 0.2}
    parts = [(w[k], v) for k, v in out.items() if v is not None]
    out["blend"] = (round(sum(wt * v for wt, v in parts)
                          / sum(wt for wt, _ in parts), 2)
                    if parts else None)
    return out


def dump_rebound(etf, spy, look=126):
    """The email's weakness + rebound tests, QUANTIFIED: mean excess
    return (bps/day) on SPY's worst-decile and best-decile days in the
    trailing window. Positive dump excess = refuses to break = hidden
    demand."""
    m = min(len(etf), len(spy), look + 1)
    if m < 60:
        return None, None
    re = [etf[-m + i + 1] / etf[-m + i] - 1 for i in range(m - 1)]
    rs = [spy[-m + i + 1] / spy[-m + i] - 1 for i in range(m - 1)]
    srt = sorted(rs)
    k = max(6, len(rs) // 10)
    lo, hi = srt[k - 1], srt[-k]
    dumps = [(a - b) for a, b in zip(re, rs) if b <= lo]
    pops = [(a - b) for a, b in zip(re, rs) if b >= hi]
    d = round(sum(dumps) / len(dumps) * 10000) if len(dumps) >= 6 else None
    r = round(sum(pops) / len(pops) * 10000) if len(pops) >= 6 else None
    return d, r


def crowded_flag(etf, spy):
    """De Bondt-Thaler long-horizon caution: 2y relative-strength line
    at its 95th+ percentile of its own range AND 20d ratio slope now
    negative -> extended + decelerating = CROWDED."""
    m = min(len(etf), len(spy))
    if m < 300:
        return False
    r = [etf[-m + i] / spy[-m + i] for i in range(m) if spy[-m + i]]
    cur = r[-1]
    peak = max(r[-21:])
    cur_p = 100.0 * sum(1 for x in r if x <= cur) / len(r)
    peak_p = 100.0 * sum(1 for x in r if x <= peak) / len(r)
    slope20 = (r[-1] / r[-21] - 1.0) if len(r) >= 21 and r[-21] else 0
    # peak at extreme highs within the month, still elevated now,
    # rolling over -- but NOT already broken (that's BREAKDOWN's job)
    return peak_p >= 95.0 and cur_p >= 85.0 and slope20 < 0


def regime_of(spy):
    px = spy[-1]
    s20, s50 = sma(spy, 20), sma(spy, 50)
    if s20 and s50:
        if px > s20 and px > s50:
            return "STRONG"
        if px < s20 and px < s50:
            return "WEAK"
    return "NEUTRAL"


def ladder_row(tkr, closes, spy, regime, blend_pop):
    px = closes[-1]
    s50, s100, s200 = sma(closes, 50), sma(closes, 100), sma(closes, 200)
    ab50 = bool(s50 and px > s50)
    ab100 = bool(s100 and px > s100)
    ab200 = bool(s200 and px > s200)
    n = min(len(closes), len(spy))
    ratio = [closes[-n + i] / spy[-n + i] for i in range(n)
             if spy[-n + i]]
    r50 = sma(ratio, 50)
    ratio_above = bool(r50 and ratio[-1] > r50)
    slope = (round((ratio[-1] / ratio[-21] - 1.0) * 100, 2)
             if len(ratio) >= 21 and ratio[-21] else None)
    mom3 = (round((closes[-1] / closes[-64] - 1.0)
                  * 100, 2) if len(closes) >= 64 else None)
    rel3 = (round(mom3 - ((spy[-1] / spy[-64] - 1.0) * 100), 2)
            if mom3 is not None and len(spy) >= 64 else None)
    b = beta_to(closes, spy)
    ah = alpha_horizons(closes, spy, b)
    dump_x, reb_x = dump_rebound(closes, spy)
    crowded = crowded_flag(closes, spy)
    score = 0
    score += 20 if ab50 else 0
    score += 15 if ab100 else 0
    score += 15 if ab200 else 0
    score += 15 if ratio_above else 0
    score += 15 if (slope or 0) > 0 else 0
    pr = pct_rank(ah.get("blend"), blend_pop)
    score += round((pr or 0) / 5.0)          # 0-20
    score = min(100, score)
    ratio_3m_high = bool(len(ratio) >= 63
                         and ratio[-1] >= max(ratio[-63:]))
    if dump_x is not None and reb_x is not None:
        res_read = ("RESILIENT_LEADER" if dump_x > 0 and reb_x > 0
                    else "DEFENSIVE_ONLY" if dump_x > 0
                    else "HIGH_BETA_PROFILE" if reb_x > 0
                    else "WEAK_BOTH_WAYS")
    else:
        res_read = None
    tag = None
    if regime == "WEAK" and ab50 and (slope or 0) > 0 and score >= 65:
        tag = "ABSORPTION"
    elif regime != "WEAK" and not ab50 and (slope or 0) < 0 \
            and score <= 25:
        tag = "BREAKDOWN"
    name, parent = (SECTORS.get(tkr, ""), None) if tkr in SECTORS \
        else INDUSTRIES[tkr]
    return {"etf": tkr, "name": name or SECTORS.get(tkr),
            "kind": "SECTOR" if tkr in SECTORS else "INDUSTRY",
            "parent_sector": parent or SECTORS.get(tkr),
            "price": round(px, 2),
            "above_sma50": ab50, "above_sma100": ab100,
            "above_sma200": ab200, "ratio_above_50d": ratio_above,
            "ratio_slope_20d_pct": slope,
            "rel_mom_3m_pp": rel3, "rel_mom_pctile": pr,
            "beta_spy_1y": round(b, 2) if b is not None else None,
            "alpha_mom": ah,
            "dump_day_excess_bps": dump_x,
            "rebound_excess_bps": reb_x,
            "crowded": crowded,
            "ratio_3m_high": ratio_3m_high,
            "resilience_read": res_read,
            "leadership_score": score, "tag": tag}


def fmp_holdings(tkr):
    """Real ETF constituents (what the fund HOLDS), not etf-holder
    (legacy: who owns ETF SHARES -- backwards). /stable/etf/holdings is
    the current FMP endpoint; returns (holdings, reason) so failures
    are diagnosable, never silent."""
    if not FMP:
        return None, "no FMP key in env"
    d = _http("https://financialmodelingprep.com/stable/etf/holdings"
              "?symbol=%s&apikey=%s" % (tkr, FMP))
    if not isinstance(d, list) or not d:
        return None, ("etf/holdings empty/non-list for %s (plan-gated "
                      "or wrong symbol) -- resp head: %s"
                      % (tkr, json.dumps(d)[:120]))
    rows = sorted(
        (r for r in d if (r.get("asset") or r.get("symbol"))),
        key=lambda r: -(r.get("weightPercentage")
                        or r.get("weightPercent") or 0))[:12]
    out = [{"ticker": (r.get("asset") or r.get("symbol")),
           "weight_pct": round(r.get("weightPercentage")
                               or r.get("weightPercent") or 0, 2)}
          for r in rows]
    return (out, None) if out else (None, "zero usable rows for %s" % tkr)


def resilience_index(doc):
    """Ground-truth schema (read from justhodl-resilience source, not
    guessed): rows live under about_to_boom / all_resilient / top_picks,
    keyed by ticker, with fields resilience (0-100) + stage
    (ABSORBING/COILED/IGNITING). No sector field exists anywhere in the
    row -- so joining happens on TICKER MEMBERSHIP in real ETF holdings,
    never on a sector-name string match."""
    idx = {}
    if not doc:
        return idx
    for k in ("all_resilient", "top_picks", "about_to_boom"):
        for r in (doc.get(k) or []):
            t = r.get("ticker")
            if t and t not in idx:
                idx[t] = {"ticker": t, "stage": r.get("stage"),
                          "resilience": r.get("resilience")}
    return idx


def soldiers_of(holdings, res_idx):
    """Holdings of the leading ETF that ALSO show up on the resilience
    engine's watchlist -- genuinely idiosyncratically strong, not just
    riding the ETF's own beta."""
    if not holdings:
        return []
    hits = [dict(res_idx[h["ticker"]], weight_pct=h["weight_pct"])
            for h in holdings if h["ticker"] in res_idx]
    return hits[:8]


def fmp_scores(tkr):
    if not FMP:
        return None
    d = _http("https://financialmodelingprep.com/stable/"
              "financial-scores?symbol=%s&apikey=%s" % (tkr, FMP))
    if isinstance(d, list) and d:
        d = d[0]
    if not isinstance(d, dict):
        return None
    z = d.get("altmanZScore")
    try:
        return float(z) if z is not None else None
    except Exception:
        return None


def industry_credit(etfs, holdings_by_etf, warns):
    """Khalid's CDS doctrine with honest free data: real single-name
    CDS is paywalled ($20K+/yr, per justhodl-cds-proxy), so the
    per-company credit proxy is Altman Z (the standard distress score
    already used fleet-wide). An industry is IN DANGER when many of
    its own companies screen distressed -- exactly the many-high-CDS
    rule, on the balance-sheet proxy. Z<1.8 distress, 1.8-3 grey.
    Financials excluded per-name (Altman is invalid for banks --
    standard practice; sector-level OAS from cds-proxy covers them).
    """
    out = {}
    fin_skip = {"XLF", "KRE", "KBE"}
    for etf in etfs:
        holds = holdings_by_etf.get(etf) or []
        if not holds:
            continue
        if etf in fin_skip:
            out[etf] = {"read": "N/A_FINANCIALS",
                        "note": "Altman invalid for banks; see "
                                "cds-proxy sector OAS"}
            continue
        zs = []
        for h in holds[:8]:
            try:
                z = fmp_scores(h["ticker"])
            except Exception:
                z = None
            if z is not None and -10 < z < 100:
                zs.append(z)
            time.sleep(0.1)
        if len(zs) < 4:
            out[etf] = {"read": "INSUFFICIENT",
                        "n_scored": len(zs)}
            continue
        zs.sort()
        med = zs[len(zs) // 2]
        distress = round(100.0 * sum(1 for z in zs if z < 1.8)
                         / len(zs))
        grey = round(100.0 * sum(1 for z in zs if 1.8 <= z < 3.0)
                     / len(zs))
        read = ("DANGER" if distress >= 30 or med < 1.8 else
                "WATCH" if distress >= 15 or med < 2.5 else "OK")
        out[etf] = {"n_scored": len(zs), "median_z": round(med, 2),
                    "distress_pct": distress, "grey_pct": grey,
                    "read": read}
    return out


def lambda_handler(event=None, context=None):
    warns = []
    spy = polygon_daily("SPY")
    if len(spy) < 210:
        raise RuntimeError("SPY history short: %d (poly_key=%s)"
                           % (len(spy), bool(POLY)))
    regime = regime_of(spy)
    rr = s3_json("data/risk-regime.json") or {}

    closes = {}
    for t in UNIVERSE:
        closes[t] = polygon_daily(t)
        if len(closes[t]) < 210:
            warns.append("%s: %d bars" % (t, len(closes[t])))
        time.sleep(0.14)

    blend_pop = []
    for t, c in closes.items():
        if len(c) >= 148:
            v = alpha_horizons(c, spy, beta_to(c, spy)).get("blend")
            if v is not None:
                blend_pop.append(v)

    rows = [ladder_row(t, c, spy, regime, blend_pop)
            for t, c in closes.items() if len(c) >= 210]
    rows.sort(key=lambda r: -r["leadership_score"])

    # self-accruing history -> 20d rank delta
    prev = s3_json(OUT_KEY) or {}
    hist = prev.get("score_history") or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {r["etf"]: r["leadership_score"] for r in rows}
    hist = dict(sorted(hist.items())[-45:])
    dates = sorted(hist)
    rank_note = None
    if len(dates) >= 21:
        old = hist[dates[-21]]
        old_rank = {e: i for i, (e, _) in enumerate(
            sorted(old.items(), key=lambda kv: -kv[1]))}
        for i, r in enumerate(rows):
            if r["etf"] in old_rank:
                r["rank_delta_20d"] = old_rank[r["etf"]] - i
    else:
        rank_note = ("WARMING_UP: rank_delta_20d activates after 21 "
                     "sessions (%d/21 accrued)" % len(dates))

    # soldiers for the top-5 leaders
    res_doc = s3_json("data/resilience.json")
    res_idx = resilience_index(res_doc)
    warns.append("resilience_index: %d tickers loaded" % len(res_idx))
    pead_pos = set()
    try:
        et = s3_json("data/earnings-tracker.json") or {}
        for r_ in (et.get("pead_signals") or []):
            if (r_.get("eps_surprise_pct") or 0) > 0 and r_.get("ticker"):
                pead_pos.add(r_["ticker"].upper())
    except Exception:
        pass
    hold_cache = {}

    def breadth_of(holds):
        """Hong-Stein diffusion / email step 5: % of the army's own
        soldiers above their 50d. BROAD>=60, NARROW<40 (one-megacap
        warning)."""
        if not holds:
            return None
        above = tot = 0
        for h in holds:
            t = h["ticker"]
            if t not in hold_cache:
                hold_cache[t] = polygon_daily(t, 220)
                time.sleep(0.12)
            c = hold_cache[t]
            s50 = sma(c, 50)
            if s50 and c:
                tot += 1
                above += 1 if c[-1] > s50 else 0
        if not tot:
            return None
        pct = round(100.0 * above / tot)
        return {"pct_above_50d": pct, "n_priced": tot,
                "read": ("BROAD" if pct >= 60
                         else "NARROW" if pct < 40 else "MIXED")}

    leaders = []
    for r in rows[:5]:
        holds, reason = None, "not attempted"
        try:
            holds, reason = fmp_holdings(r["etf"])
        except Exception as e:
            reason = str(e)[:100]
        if holds is None:
            warns.append("holdings skip %s: %s" % (r["etf"], reason))
        sold = soldiers_of(holds, res_idx)
        etf_3m = _ret(closes.get(r["etf"], []), 64, 1)
        spy_3m = _ret(spy, 64, 1)
        for x in sold:
            x["pead"] = x["ticker"].upper() in pead_pos
            t = x["ticker"]
            if t not in hold_cache:
                hold_cache[t] = polygon_daily(t, 220)
                time.sleep(0.12)
            s3m = _ret(hold_cache[t], 64, 1)
            if None not in (s3m, etf_3m, spy_3m):
                x["vs_etf_3m_pp"] = round(s3m - etf_3m, 1)
                x["vs_spy_3m_pp"] = round(s3m - spy_3m, 1)
                x["chain_intact"] = bool(s3m > etf_3m > spy_3m)
        leaders.append(dict(
            r, holdings_top=holds,
            holdings_reason=(None if holds else reason),
            breadth=breadth_of(holds),
            resilient_names=sold))

    # ── industry credit-danger: leaders + breakdown cluster ──
    holdings_by_etf = {l["etf"]: l.get("holdings_top") for l in leaders}
    bkdn_rows = [r for r in rows if r["tag"] == "BREAKDOWN"][:8]
    for r_ in bkdn_rows:
        if r_["etf"] not in holdings_by_etf:
            try:
                h_, why_ = fmp_holdings(r_["etf"])
            except Exception as e:
                h_, why_ = None, str(e)[:80]
            if h_ is None:
                warns.append("bkdn holdings skip %s: %s"
                             % (r_["etf"], why_))
            holdings_by_etf[r_["etf"]] = h_
    credit = {}
    try:
        credit = industry_credit(list(holdings_by_etf),
                                 holdings_by_etf, warns)
    except Exception as e:
        warns.append("industry_credit: %s" % str(e)[:100])
    # sector-level OAS enrichment from the existing cds-proxy engine
    try:
        cp = s3_json("data/cds-proxy.json") or {}
        for etf, sec in (("XLF", "financials"), ("XLE", "energy"),
                         ("XLK", "tech"), ("XLRE", "reits")):
            row_ = (cp.get("sectors") or {}).get(sec)
            if row_ and etf in credit:
                credit[etf]["sector_oas"] = row_
            elif row_:
                credit[etf] = {"read": "SECTOR_OAS_ONLY",
                               "sector_oas": row_}
    except Exception:
        pass
    for r_ in rows:
        c = credit.get(r_["etf"])
        if c:
            r_["credit_read"] = c.get("read")
            if r_["tag"] == "BREAKDOWN" and c.get("read") == "DANGER":
                r_["tag"] = "CONFIRMED_DETERIORATION"

    fv = s3_json("data/finviz-groups.json") or {}
    by_sector = {}
    for r in rows:
        if r["kind"] == "SECTOR":
            by_sector[r["name"]] = {
                "etf": r["etf"],
                "leadership_score": r["leadership_score"],
                "tag": r["tag"]}

    out = {
        "engine": "justhodl-industry-rotation", "version": "2.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "doctrine": "regime -> industry leadership -> strongest "
                    "soldiers; divergence under weakness = absorption "
                    "(industry momentum subsumes stock momentum: "
                    "Moskowitz-Grinblatt 1999, AQR)",
        "market_regime": {
            "state": regime,
            "spy": round(spy[-1], 2),
            "spy_above_sma20": spy[-1] > (sma(spy, 20) or 9e9),
            "spy_above_sma50": spy[-1] > (sma(spy, 50) or 9e9),
            "risk_regime_score": rr.get("risk_regime_score"),
            "risk_regime": rr.get("risk_regime")},
        "method": {
            "leadership_score": "SMA50 +20, SMA100 +15, SMA200 +15, "
                                "ratio>50dMA +15, ratio 20d slope>0 "
                                "+15, alpha-momentum blend percentile "
                                "/5 (0-20)",
            "alpha_momentum": "beta-adjusted (1y OLS) excess returns; "
                              "blend = 0.5*3m + 0.3*6m-skip-month + "
                              "0.2*12m-minus-1m (Jegadeesh-Titman skip "
                              "+ BAB separation of RS from beta)",
            "dump_rebound": "mean excess bps/day on SPY worst-decile / "
                            "best-decile days, trailing 126d -- the "
                            "weakness and rebound tests quantified",
            "industry_credit": "per-name Altman-Z aggregation (Z<1.8 distress); DANGER when >=30% distressed or median<1.8 -- the many-high-CDS rule on the honest free proxy; banks excluded (Altman invalid), covered by cds-proxy sector OAS",
            "crowded": "2y RS line >=95th own-percentile + 20d ratio "
                       "slope negative (long-horizon reversal caution)",
            "breadth": "leaders: % of holdings above own 50d; "
                       "BROAD>=60 / NARROW<40",
            "absorption": "regime WEAK + above SMA50 + rising ratio + "
                          "score>=65", "universe_n": len(rows)},
        "ladder": rows,
        "leaders": leaders,
        "absorption_watch": [r["etf"] for r in rows
                             if r["tag"] == "ABSORPTION"],
        "breakdown_watch": [r["etf"] for r in rows
                            if r["tag"] == "BREAKDOWN"],
        "by_sector_name": by_sector,
        "industry_credit": credit,
        "finviz_sector_perf_attached": bool(fv),
        "score_history": hist,
        "rank_note": rank_note,
        "warns": warns[:20]}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "regime": regime,
                                "top": rows[0]["etf"] if rows else None,
                                "absorption": len(out["absorption_watch"]),
                                "warns": len(warns)})}
