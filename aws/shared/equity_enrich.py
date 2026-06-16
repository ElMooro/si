"""
equity_enrich.py — shared equity-research enrichment toolkit.
Page-agnostic building blocks reused by per-page research engines
(bottleneck-research pattern): 10yr financials + margins, FCF, shares,
cash-conversion/accruals, solvency (net-debt/EBITDA, coverage, current ratio),
valuation (P/E vs industry + own range, PEG, EV/EBITDA), price momentum,
earnings beats, revenue concentration, insider activity; confirmation feeds
(short interest, 13F, fwd estimates, rotation chains); a parameterized
plain-English thesis+bear generator; and a forward track-record grader vs SPY.
Each engine supplies its own page-specific framing (thesis prompt, scorecard).
"""
import json
import os
import math
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3
from llm_router import complete

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"


def fmp(path, params):
    p = dict(params); p["apikey"] = FMP_KEY
    url = f"{BASE}/{path}?" + urllib.parse.urlencode(p)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def num(v):
    try:
        return float(v)
    except Exception:
        return None


def fetch_financials(tk):
    prof = fmp("profile", {"symbol": tk})
    inc = fmp("income-statement", {"symbol": tk, "period": "annual", "limit": 15})
    cf = fmp("cash-flow-statement", {"symbol": tk, "period": "annual", "limit": 15})
    rat = fmp("ratios-ttm", {"symbol": tk})
    earn = fmp("earnings", {"symbol": tk, "limit": 12})
    bs = fmp("balance-sheet-statement", {"symbol": tk, "period": "annual", "limit": 2})
    seg = fmp("revenue-product-segmentation", {"symbol": tk})
    ins = fmp("insider-trading/search", {"symbol": tk, "limit": 80})
    p = (prof[0] if isinstance(prof, list) and prof else {}) or {}
    r = (rat[0] if isinstance(rat, list) and rat else {}) or {}

    fcf_by_year = {}; ocf_by_year = {}; acq_by_year = {}
    for row in (cf if isinstance(cf, list) else []):
        yr = (row.get("calendarYear") or str(row.get("date", ""))[:4])
        fcf_by_year[yr] = num(row.get("freeCashFlow"))
        ocf_by_year[yr] = num(row.get("operatingCashFlow")) or num(row.get("netCashProvidedByOperatingActivities"))
        acq_by_year[yr] = num(row.get("acquisitionsNet"))

    fins = []
    for row in (inc if isinstance(inc, list) else [])[:15]:
        yr = (row.get("calendarYear") or str(row.get("date", ""))[:4])
        rev = num(row.get("revenue")); ni = num(row.get("netIncome"))
        gp = num(row.get("grossProfit")); oi = num(row.get("operatingIncome"))
        fcf = fcf_by_year.get(yr)
        sh = num(row.get("weightedAverageShsOutDil")) or num(row.get("weightedAverageShsOut"))
        fins.append({
            "year": yr, "revenue": rev, "netIncome": ni,
            "eps": num(row.get("epsdiluted")) or num(row.get("eps")),
            "gm": round(gp / rev * 100, 1) if (rev and gp is not None) else None,
            "om": round(oi / rev * 100, 1) if (rev and oi is not None) else None,
            "nm": round(ni / rev * 100, 1) if (rev and ni is not None) else None,
            "fcf": fcf,
            "fcfm": round(fcf / rev * 100, 1) if (rev and fcf is not None) else None,
            "shares": sh,
        })
    fins.reverse()  # oldest -> newest for charting

    # next earnings date (the bottleneck catalyst)
    today = datetime.now(timezone.utc).date().isoformat()
    futs = sorted(s for s in (str(x.get("date", ""))[:10] for x in (earn if isinstance(earn, list) else []))
                  if s and s >= today)
    next_earn = futs[0] if futs else None

    # 52-week position (asymmetry / timing)
    price = num(p.get("price")); rng = p.get("range"); off_high = off_low = None
    if rng and "-" in str(rng) and price:
        try:
            lo, hi = [float(x) for x in str(rng).split("-")[:2]]
            if hi:
                off_high = round((price / hi - 1) * 100, 1)
            if lo:
                off_low = round((price / lo - 1) * 100, 1)
        except Exception:
            pass

    # gross-margin trend: latest year vs the average of prior years (pricing power)
    gms = [f["gm"] for f in fins if f.get("gm") is not None]
    gm_trend = None
    if len(gms) >= 3:
        gm_trend = round(gms[-1] - (sum(gms[:-1]) / len(gms[:-1])), 1)
    # share-count trend: latest vs earliest (dilution check)
    shs = [f["shares"] for f in fins if f.get("shares")]
    share_chg = None
    if len(shs) >= 2 and shs[0]:
        share_chg = round((shs[-1] / shs[0] - 1) * 100, 1)
    latest = fins[-1] if fins else {}

    # --- price momentum + sparkline (recent ~75 trading days) ---
    hist = fmp("historical-price-eod/light", {"symbol": tk})
    rows = hist if isinstance(hist, list) else (hist.get("historical") if isinstance(hist, dict) else [])
    closes = []
    for row in (rows or [])[:80]:
        c = num(row.get("price")) if row.get("price") is not None else num(row.get("close"))
        if c is not None:
            closes.append((str(row.get("date", ""))[:10], c))
    closes.sort()  # oldest -> newest
    price_now = closes[-1][1] if closes else price

    def _ret(nb):
        if len(closes) > nb and closes[-1 - nb][1]:
            return round((closes[-1][1] / closes[-1 - nb][1] - 1) * 100, 1)
        return None
    ret_1m, ret_3m = _ret(21), _ret(63)
    spk = [c for _, c in closes]
    if len(spk) > 15:
        step = len(spk) / 15.0
        spk = [spk[int(i * step)] for i in range(15)] + [spk[-1]]

    # --- valuation vs its OWN history (P/E percentile) ---
    rat_hist = fmp("ratios", {"symbol": tk, "period": "annual", "limit": 15})
    pes = []
    for row in (rat_hist if isinstance(rat_hist, list) else []):
        pv = num(row.get("priceToEarningsRatio")) or num(row.get("peRatio"))
        if pv is not None and pv > 0:
            pes.append(pv)
    pe_low = pe_high = pe_pctile = None
    cur_pe = num(p.get("pe")) or num(r.get("priceToEarningsRatioTTM"))
    if pes:
        pe_low, pe_high = round(min(pes), 1), round(max(pes), 1)
        if cur_pe and pe_high != pe_low:
            pe_pctile = max(0, min(100, round((cur_pe - pe_low) / (pe_high - pe_low) * 100)))

    # --- earnings beat history + next-quarter estimate ---
    beats = tot = 0
    nq_eps = nq_rev = None
    for row in (earn if isinstance(earn, list) else []):
        ea, ee = num(row.get("epsActual")), num(row.get("epsEstimated"))
        if ea is not None and ee is not None:
            tot += 1; beats += 1 if ea > ee else 0
        if str(row.get("date", ""))[:10] == next_earn:
            nq_eps = num(row.get("epsEstimated")); nq_rev = num(row.get("revenueEstimated"))
    beat_rate = round(beats / tot * 100) if tot else None

    # --- financial quality: cash conversion + accruals ---
    ni_l = latest.get("netIncome"); fcf_l = latest.get("fcf"); yr_l = latest.get("year"); rev_l = latest.get("revenue")
    cash_conv = round(fcf_l / ni_l * 100) if (ni_l and fcf_l is not None and ni_l > 0) else None
    ocf_l = ocf_by_year.get(yr_l)
    bs0 = (bs[0] if isinstance(bs, list) and bs else {}) or {}
    tot_assets = num(bs0.get("totalAssets"))
    accruals = round((ni_l - ocf_l) / tot_assets * 100, 1) if (ni_l is not None and ocf_l is not None and tot_assets) else None

    # --- solvency / balance sheet ---
    cur_ratio = num(r.get("currentRatioTTM"))
    if cur_ratio is None:
        ca = num(bs0.get("totalCurrentAssets")); cl = num(bs0.get("totalCurrentLiabilities"))
        cur_ratio = round(ca / cl, 2) if (ca and cl) else None
    inc0 = (inc[0] if isinstance(inc, list) and inc else {}) or {}
    debt = num(bs0.get("totalDebt")); cash = num(bs0.get("cashAndCashEquivalents"))
    ebitda = num(inc0.get("ebitda"))
    if ebitda is None:
        _oi = num(inc0.get("operatingIncome")); _da = num(inc0.get("depreciationAndAmortization"))
        ebitda = (_oi + _da) if (_oi is not None and _da is not None) else None
    net_debt_ebitda = num(r.get("netDebtToEBITDATTM"))
    if net_debt_ebitda is None and debt is not None and ebitda:
        net_debt_ebitda = round((debt - (cash or 0)) / ebitda, 2)
    # interest coverage = operating income / interest expense
    op_inc = num(inc0.get("operatingIncome")); int_exp = num(inc0.get("interestExpense"))
    int_cov = None
    if op_inc is not None and int_exp and abs(int_exp) > 0:
        int_cov = round(op_inc / abs(int_exp), 1)
    # --- valuation depth (EV/EBITDA, PEG) computed from statements ---
    mc = num(p.get("mktCap") or p.get("marketCap"))
    ev_ebitda = round((mc + (debt or 0) - (cash or 0)) / ebitda, 1) if (ebitda and mc is not None) else None
    peg = None
    _eps = [f.get("eps") for f in fins if f.get("eps") is not None]
    if cur_pe and len(_eps) >= 2 and _eps[-2] and _eps[-2] > 0 and _eps[-1] is not None:
        _g = (_eps[-1] / _eps[-2] - 1) * 100
        if _g and _g > 0:
            peg = round(cur_pe / _g, 2)

    # --- inorganic-growth flag (large recent acquisitions vs revenue) ---
    acq_l = acq_by_year.get(yr_l)
    acq_pct = round(abs(acq_l) / rev_l * 100) if (acq_l and rev_l) else None
    acq_driven = bool(acq_pct is not None and acq_pct >= 8)

    # --- revenue concentration (top product-segment share) ---
    seg_conc = seg_n = None
    segrows = seg if isinstance(seg, list) else []
    if segrows and isinstance(segrows[0], dict):
        data = segrows[0].get("data")
        if isinstance(data, dict) and data:
            vals = [v for v in data.values() if isinstance(v, (int, float)) and v > 0]
            if vals:
                seg_n = len(vals); seg_conc = round(max(vals) / sum(vals) * 100)

    # --- insider activity (selling into the boom = quiet veto) ---
    ibuys = isells = 0
    for row in (ins if isinstance(ins, list) else [])[:80]:
        tt = str(row.get("transactionType") or "")
        ad = str(row.get("acquisitionOrDisposition") or "")
        if ad == "A" or tt.startswith("P-Purchase"):
            ibuys += 1
        elif ad == "D" or tt.startswith("S-Sale"):
            isells += 1
    insider_net = (ibuys - isells) if (ibuys or isells) else None
    insider_sig = (("buying" if insider_net > 0 else "selling" if insider_net < 0 else "neutral")
                   if insider_net is not None else None)

    return {
        "name": p.get("companyName"),
        "desc": (p.get("description") or "")[:650],
        "ceo": p.get("ceo"), "employees": p.get("fullTimeEmployees"),
        "website": p.get("website"), "exchange": p.get("exchangeShortName"),
        "sector": p.get("sector"), "industry": p.get("industry"),
        "mkt_cap": p.get("mktCap") or p.get("marketCap"),
        "price": price, "range_52w": p.get("range"), "beta": num(p.get("beta")),
        "pe": num(p.get("pe")) or num(r.get("priceToEarningsRatioTTM")),
        "ps": num(r.get("priceToSalesRatioTTM")),
        "pb": num(r.get("priceToBookRatioTTM")),
        "div_yield": (round(num(r.get("dividendYieldTTM")) * 100, 2)
                      if num(r.get("dividendYieldTTM")) is not None else None),
        "financials": fins,
        "next_earnings": next_earn,
        "off_52w_high": off_high, "off_52w_low": off_low,
        "gm_trend": gm_trend, "gm_latest": latest.get("gm"), "om_latest": latest.get("om"),
        "fcfm_latest": latest.get("fcfm"), "share_chg_pct": share_chg,
        "price": price_now, "ret_1m": ret_1m, "ret_3m": ret_3m, "price_spark": spk,
        "pe_low": pe_low, "pe_high": pe_high, "pe_pctile": pe_pctile,
        "beat_rate": beat_rate, "beats_n": tot,
        "nq_eps_est": nq_eps, "nq_rev_est": nq_rev,
        "cash_conv": cash_conv, "accruals": accruals,
        "cur_ratio": cur_ratio,
        "int_cov": (round(int_cov, 1) if int_cov is not None else None),
        "net_debt_ebitda": net_debt_ebitda,
        "peg": (round(peg, 2) if peg is not None else None),
        "ev_ebitda": (round(ev_ebitda, 1) if ev_ebitda is not None else None),
        "acq_driven": acq_driven, "acq_pct": acq_pct,
        "seg_conc": seg_conc, "seg_n": seg_n,
        "insider_net": insider_net, "insider_sig": insider_sig,
        "insider_buys": ibuys, "insider_sells": isells,
    }


def fetch_peer_pe():
    """Best-effort industry + sector P/E snapshots for the vs-peers comparison."""
    today = datetime.now(timezone.utc).date().isoformat()
    ind, sec = {}, {}
    isnap = fmp("industry-pe-snapshot", {"date": today})
    for row in (isnap if isinstance(isnap, list) else []):
        k = row.get("industry"); v = num(row.get("pe"))
        if k and v:
            ind[k] = round(v, 1)
    ssnap = fmp("sector-pe-snapshot", {"date": today})
    for row in (ssnap if isinstance(ssnap, list) else []):
        k = row.get("sector"); v = num(row.get("pe"))
        if k and v:
            sec[k] = round(v, 1)
    return ind, sec




def load_confirmation_feeds():
    """Pull per-ticker confirmation signals from the wider engine fleet."""
    def L(k):
        try:
            return json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            return {}
    si = (L("data/short-interest.json") or {}).get("by_ticker", {}) or {}
    f13 = (L("data/13f-positions.json") or {}).get("aggregate_by_ticker", {}) or {}
    fwd = (L("data/estimate-revisions-latest.json") or {}).get("fwd_rev_growth", {}) or {}
    chains = (L("data/rotation-chains.json") or {}).get("chains", {}) or {}
    chain_idx = {}
    for cname, c in (chains.items() if isinstance(chains, dict) else []):
        if not isinstance(c, dict):
            continue
        for nt in (c.get("next_up_tickers") or []):
            t = nt.get("ticker")
            if t and t not in chain_idx:
                chain_idx[t] = {
                    "chain": c.get("chain") or cname, "role": "next-up laggard",
                    "catchup": c.get("expected_catchup_pct"),
                    "own_30d": nt.get("own_30d_pct"), "leader_30d": c.get("leader_perf_30d_pct"),
                }
    return si, f13, fwd, chain_idx




def _hist_map(tk):
    """date -> close for a ticker (recent sessions), for point-in-time grading."""
    h = fmp("historical-price-eod/light", {"symbol": tk})
    rows = h if isinstance(h, list) else (h.get("historical") if isinstance(h, dict) else [])
    m = {}
    for row in (rows or []):
        d = str(row.get("date", ""))[:10]
        c = num(row.get("price")) if row.get("price") is not None else num(row.get("close"))
        if d and c:
            m[d] = c
    return m


def _on_or_after(series_dates, target):
    for d in series_dates:
        if d >= target:
            return d
    return None




def make_thesis(name, tk, ind, signals_block, system, max_tokens=440):
    """Generic plain-English thesis + bear case. Engine supplies signals_block + system prompt."""
    prompt = (f"Stock: {name} ({tk}); industry: {ind}.\n{signals_block}\n"
              "Write the plain-English thesis for why this stock could work, then the bear case.")
    try:
        out = complete(prompt, tier="bulk", max_tokens=max_tokens, system=system)
        txt = (out or "").strip()
        if not txt or any(x in txt for x in ("Draft", "Critique", "Analyze the Request", "**")):
            return None, None
        thesis, bear = txt, None
        if "BEAR:" in txt:
            a, b = txt.split("BEAR:", 1)
            thesis, bear = a, b.strip()
        thesis = thesis.replace("THESIS:", "").strip()
        return (thesis or None), (bear or None)
    except Exception as e:
        print(f"[enrich] thesis fail {tk}: {str(e)[:80]}")
        return None, None


def grade_track_record(signal_type, out_key, windows=(5, 21, 63)):
    """Forward test of logged calls of a given signal_type vs SPY (point-in-time, no look-ahead)."""
    try:
        from boto3.dynamodb.conditions import Attr
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
        items = []; lek = None
        for _ in range(10):
            kw = dict(FilterExpression=Attr("signal_type").eq(signal_type), Limit=300)
            if lek:
                kw["ExclusiveStartKey"] = lek
            r = tbl.scan(**kw); items += r.get("Items", []); lek = r.get("LastEvaluatedKey")
            if not lek:
                break
    except Exception as e:
        print(f"[track] scan fail {str(e)[:80]}"); return None
    if not items:
        return None
    today = datetime.now(timezone.utc).date().isoformat()
    spy = _hist_map("SPY"); spy_d = sorted(spy)
    agg = {w: {"n": 0, "wins": 0, "sx": 0.0} for w in windows}
    hist_cache = {}; n_pending = 0
    for it in items:
        sid = it.get("signal_id", ""); logged = it.get("logged_at")
        try:
            base = float(str(it.get("baseline_price")))
        except Exception:
            base = None
        tk = sid.split("#")[1] if sid.count("#") >= 1 else it.get("ticker")
        if not (logged and base and tk):
            continue
        d_log = str(logged)[:10]
        if tk not in hist_cache:
            hist_cache[tk] = _hist_map(tk)
        sm = hist_cache[tk]; sm_d = sorted(sm)
        spy_base_d = _on_or_after(spy_d, d_log); spy_base = spy.get(spy_base_d) if spy_base_d else None
        graded = False
        for w in windows:
            tgt = (datetime.fromisoformat(d_log) + timedelta(days=w)).date().isoformat()
            if tgt > today:
                continue
            sd = _on_or_after(sm_d, tgt); pd = _on_or_after(spy_d, tgt)
            if sd and pd and spy_base:
                ex = (sm[sd] / base - 1) * 100 - (spy[pd] / spy_base - 1) * 100
                agg[w]["n"] += 1; agg[w]["wins"] += 1 if ex > 0 else 0; agg[w]["sx"] += ex
                graded = True
        if not graded:
            n_pending += 1
    dates = sorted(str(it.get("logged_at"))[:10] for it in items if it.get("logged_at"))
    out = {"generated_at": datetime.now(timezone.utc).isoformat(), "n_calls": len(items),
           "n_pending": n_pending, "first_date": dates[0] if dates else None,
           "last_date": dates[-1] if dates else None, "windows": {}}
    for w in windows:
        a = agg[w]
        if a["n"]:
            out["windows"][f"d{w}"] = {"n": a["n"], "hit_rate": round(a["wins"] / a["n"] * 100),
                                       "avg_excess": round(a["sx"] / a["n"], 1)}
    try:
        S3.put_object(Bucket=BUCKET, Key=out_key, Body=json.dumps(out).encode(),
                      ContentType="application/json", CacheControl="public, max-age=1800")
    except Exception as e:
        print(f"[track] write fail {str(e)[:60]}")
    return out
