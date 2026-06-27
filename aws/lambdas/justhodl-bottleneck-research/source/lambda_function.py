"""
justhodl-bottleneck-research — per-stock research + plain-English AI thesis layer
================================================================================
Turns the bottleneck-boom page into a self-contained research terminal. For each
top bottleneck candidate it assembles:
  • What the company does (FMP profile description)
  • Valuation: P/E, P/S, P/B, and the stock's P/E vs its INDUSTRY P/E
  • 10 years of financials (revenue, net income, EPS, gross/op/net margins)
  • A short, HONEST, normie-readable thesis: the mechanism for why it could
    re-rate (bottleneck demand + the numbers) AND the single biggest risk.
Theses are generated via the tiered LLM router (tier="reason" -> GLM-5.1 with
Claude fallback; this is public-data synthesis) and CACHED so we don't pay to
regenerate identical theses every run. Output: data/bottleneck-boom-research.json
keyed by ticker, consumed by /bottleneck-boom.html as expandable research drawers.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from llm_router import complete

VERSION = "1.0.0"
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/bottleneck-boom.json"
OUT_KEY = "data/bottleneck-boom-research.json"
FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"
TOP_N = 30
THESIS_CACHE_HRS = 20
MAX_NEW_THESES = 30
THESIS_VER = "haiku-3"

SYSTEM = (
    "You are a sharp, honest equity analyst explaining a stock to a smart beginner with no "
    "finance background. Plain English, zero jargon, no hype, no price targets, never promise gains. "
    "Output EXACTLY two labeled parts and nothing else.\n"
    "THESIS: 3-4 short sentences — first the plain-English MECHANISM for why this stock could re-rate "
    "higher (tie it to the demand/supply bottleneck and the specific numbers given), then one sentence "
    "on the single biggest risk.\n"
    "BEAR: 1-2 sentences — the strongest argument AGAINST owning it, plus the one specific number or "
    "event that would prove the bull case wrong (a clear falsifier).\n"
    "Use the exact labels 'THESIS:' and 'BEAR:'. No headings, no markdown, no bullet points, no numbered "
    "steps, no 'Draft', and never restate these instructions."
)


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
    inc = fmp("income-statement", {"symbol": tk, "period": "annual", "limit": 10})
    cf = fmp("cash-flow-statement", {"symbol": tk, "period": "annual", "limit": 10})
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
    for row in (inc if isinstance(inc, list) else [])[:10]:
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
    rat_hist = fmp("ratios", {"symbol": tk, "period": "annual", "limit": 10})
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


def make_thesis(name, tk, ind, m):
    prompt = (
        f"Stock: {name} ({tk}); industry: {ind}.\n"
        f"What it does: {(m.get('desc') or '')[:320]}\n"
        f"Signals:\n"
        f"- revenue growth {m.get('rev_growth_yoy')}% year-over-year\n"
        f"- revenue ACCELERATION {m.get('rev_accel_pp')}pp (this quarter's growth minus last quarter's — positive means speeding up)\n"
        f"- revenue-to-market-cap {m.get('rev_to_mcap_pct')}% (higher = cheaper for the sales it generates)\n"
        f"- valuation: P/E {m.get('pe')} vs its industry's typical P/E {m.get('industry_pe')}\n"
        f"- its industry's supply-bottleneck pressure is {m.get('group_pressure')}/100 "
        f"(high = orders/backlog piling up faster than companies can ship)\n"
        f"- gross margin trend: {('expanding' if (m.get('gm_trend') or 0) > 0.5 else 'compressing' if (m.get('gm_trend') or 0) < -0.5 else 'roughly flat')} "
        f"({m.get('gm_trend')}pp vs its own history — rising margins mean real pricing power, the proof it's capturing the bottleneck)\n"
        f"Write the plain-English thesis for why this stock could re-rate higher, then the single biggest risk."
    )
    try:
        out = complete(prompt, tier="bulk", max_tokens=440, system=SYSTEM)
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
        print(f"[research] thesis fail {tk}: {str(e)[:80]}")
        return None, None


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


def pressure_trend(industry_pressure):
    """Snapshot today's industry pressure and return the ~30-day change per group."""
    HK = "data/bottleneck-pressure-history.json"
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HK)["Body"].read())
    except Exception:
        hist = {"by_date": {}}
    hist.setdefault("by_date", {})
    today = datetime.now(timezone.utc).date().isoformat()
    snap = {}
    for g, v in (industry_pressure or {}).items():
        if not isinstance(v, dict):
            continue
        s = v.get("pressure_0_100")
        if s is None and v.get("ip_yoy_z") is not None:
            s = round(50 + v["ip_yoy_z"] * 10, 1)
        if s is not None:
            snap[g] = s
    hist["by_date"][today] = snap
    for d0 in sorted(hist["by_date"].keys())[:-60]:
        hist["by_date"].pop(d0, None)
    try:
        S3.put_object(Bucket=BUCKET, Key=HK, Body=json.dumps(hist).encode(), ContentType="application/json")
    except Exception:
        pass
    trend = {}
    dates = sorted(hist["by_date"].keys())
    base = None
    for d0 in dates:
        if (datetime.fromisoformat(today) - datetime.fromisoformat(d0)).days >= 21:
            base = d0
    if base:
        for g, s in snap.items():
            if g in hist["by_date"][base]:
                trend[g] = round(s - hist["by_date"][base][g], 1)
    return trend


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


def grade_track_record():
    """Forward test of logged bottleneck calls vs SPY at 5/21/63 days (point-in-time, no look-ahead)."""
    try:
        from boto3.dynamodb.conditions import Attr
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
        items = []; lek = None
        for _ in range(10):
            kw = dict(FilterExpression=Attr("signal_type").eq("bottleneck_boom"), Limit=300)
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
    windows = [5, 21, 63]
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
                stock_ret = (sm[sd] / base - 1) * 100
                spy_ret = (spy[pd] / spy_base - 1) * 100
                ex = stock_ret - spy_ret
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
    # per-window maturity so the page can show 21/63-day as "maturing — X/N graded" rather than
    # hiding the horizons that matter most for a re-rate thesis
    out["maturity"] = {}
    for w in windows:
        pend = 0
        for it in items:
            lg = it.get("logged_at")
            if not lg:
                continue
            tgt = (datetime.fromisoformat(str(lg)[:10]) + timedelta(days=w)).date().isoformat()
            if tgt > today:
                pend += 1
        out["maturity"][f"d{w}"] = {"graded": agg[w]["n"], "pending": pend, "total": len(items)}
    try:
        S3.put_object(Bucket=BUCKET, Key="data/bottleneck-track-record.json",
                      Body=json.dumps(out).encode(), ContentType="application/json",
                      CacheControl="public, max-age=1800")
    except Exception as e:
        print(f"[track] write fail {str(e)[:60]}")
    return out


def log_targets(out_recs, top_calls):
    """Log today's price targets to the signals table so they can be graded over time —
    closing the loop on the valuation layer exactly like the boom calls. Idempotent per
    ticker per day (signal_id carries the date)."""
    try:
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
    except Exception as e:
        print(f"[targets] table fail {str(e)[:60]}"); return 0
    now = datetime.now(timezone.utc); today = now.date().isoformat()
    n = 0
    for tk in top_calls:
        rec = out_recs.get(tk) or {}
        fv = rec.get("fwd_val") or {}
        base = fv.get("tp_base"); price = rec.get("price")
        if not (base and price):
            continue
        try:
            tbl.put_item(Item={
                "signal_id":     f"bottleneck-target#{tk}#{today}",
                "signal_type":   "bottleneck_target",
                "ticker":        tk,
                "logged_at":     now.isoformat(),
                "logged_epoch":  int(now.timestamp()),
                "baseline_price": str(price),
                "target_base":   str(base),
                "target_bull":   (str(fv["tp_bull"]) if fv.get("tp_bull") is not None else None),
                "target_bear":   (str(fv["tp_bear"]) if fv.get("tp_bear") is not None else None),
                "horizon_days":  63,
                "growth_pct":    (str(fv["growth_1y_pct"]) if fv.get("growth_1y_pct") is not None else None),
                "status":        "pending",
                "ttl":           int(now.timestamp()) + 180 * 86400,
            })
            n += 1
        except Exception as e:
            print(f"[targets] put {tk} fail {str(e)[:50]}")
    print(f"[targets] logged {n} targets")
    return n


def grade_targets():
    """Did the price reach the base/bull target within the 63-day horizon? Hit-rate over
    matured targets (point-in-time, peak price in the window vs the dated target). No look-ahead."""
    try:
        from boto3.dynamodb.conditions import Attr
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
        items = []; lek = None
        for _ in range(8):
            kw = dict(FilterExpression=Attr("signal_type").eq("bottleneck_target"), Limit=300)
            if lek:
                kw["ExclusiveStartKey"] = lek
            r = tbl.scan(**kw); items += r.get("Items", []); lek = r.get("LastEvaluatedKey")
            if not lek:
                break
    except Exception as e:
        print(f"[targets] scan fail {str(e)[:60]}"); return None
    if not items:
        return None
    today = datetime.now(timezone.utc).date().isoformat()
    hist_cache = {}
    n_graded = base_hit = bull_hit = n_pending = 0
    sum_peak = 0.0
    for it in items:
        tk = it.get("ticker"); lg = it.get("logged_at")
        try:
            base_px = float(str(it.get("baseline_price")))
            tgt_base = float(str(it.get("target_base")))
            tgt_bull = float(str(it.get("target_bull"))) if it.get("target_bull") else None
            hz = int(it.get("horizon_days") or 63)
        except Exception:
            continue
        if not (tk and lg and base_px and tgt_base):
            continue
        d_log = str(lg)[:10]
        end = (datetime.fromisoformat(d_log) + timedelta(days=hz)).date().isoformat()
        if end > today:
            n_pending += 1; continue
        if tk not in hist_cache:
            hist_cache[tk] = _hist_map(tk)
        sm = hist_cache[tk]
        window = [v for d, v in sm.items() if d_log <= d <= end]
        if not window:
            n_pending += 1; continue
        peak = max(window)
        n_graded += 1
        if peak >= tgt_base: base_hit += 1
        if tgt_bull and peak >= tgt_bull: bull_hit += 1
        sum_peak += (peak / base_px - 1) * 100
    out = {"n_targets": len(items), "n_graded": n_graded, "n_pending": n_pending, "horizon_days": 63}
    if n_graded:
        out["base_hit_rate"] = round(base_hit / n_graded * 100)
        out["bull_hit_rate"] = round(bull_hit / n_graded * 100)
        out["avg_peak_move_pct"] = round(sum_peak / n_graded, 1)
    return out


def pressure_percentiles(industry_pressure):
    """Convert each industry's standardized pressure z-scores to a 0-100 historical percentile."""
    out = {}
    for g, v in (industry_pressure or {}).items():
        if not isinstance(v, dict):
            continue
        zs = [v.get(k) for k in ("new_orders_yoy_z", "backlog_yoy_z", "backlog_ratio_z", "ip_yoy_z")
              if isinstance(v.get(k), (int, float))]
        if zs:
            out[g] = round(_phi(max(zs)) * 100)
    return out


def compute_changes(ranked, trap_by_tk):
    """Diff today's list vs the previously stored state: new entrants + trap-status flips."""
    PK = "data/bottleneck-prev-state.json"
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET, Key=PK)["Body"].read())
    except Exception:
        prev = {}
    prev_ranked = set(prev.get("ranked") or [])
    prev_trap = prev.get("trap_by_tk") or {}
    new_entrants = [t for t in ranked if t not in prev_ranked] if prev_ranked else []
    dropped = [t for t in prev_ranked if t not in set(ranked)] if prev_ranked else []
    promoted, demoted = [], []
    order = {"trap": 0, "watch": 1, "real": 2}
    for t, now_v in trap_by_tk.items():
        pv = prev_trap.get(t)
        if pv and now_v and pv != now_v:
            if order.get(now_v, 1) > order.get(pv, 1):
                promoted.append({"t": t, "from": pv, "to": now_v})
            elif order.get(now_v, 1) < order.get(pv, 1):
                demoted.append({"t": t, "from": pv, "to": now_v})
    try:
        S3.put_object(Bucket=BUCKET, Key=PK,
                      Body=json.dumps({"date": datetime.now(timezone.utc).date().isoformat(),
                                       "ranked": ranked, "trap_by_tk": trap_by_tk}).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    if not prev_ranked:
        return {"first_run": True}
    return {"new": new_entrants[:10], "dropped": dropped[:10],
            "promoted": promoted[:10], "demoted": demoted[:10]}


def forward_valuation(rec, pressure_entry, industry_ps=None):
    """Bloomberg-style forward + projected valuation for a bottleneck candidate, all from
    real fields already on the record:
      • price prediction — revenue growth (analyst forward, backlog-supported) → target price
      • forward P/E & P/S (next 12m) vs the industry multiple
      • projected P/E & P/S once the bottleneck-driven growth sustains (~2y) — the re-rate runway
    The thesis: a cheap, accelerating name in a supply-tight industry should grow INTO and then
    re-rate toward its industry multiple. These numbers quantify both legs."""
    price = num(rec.get("price")); pe = num(rec.get("pe")); ps = num(rec.get("ps"))
    ipe = num(rec.get("industry_pe")); mc = num(rec.get("mkt_cap")); ips = num(industry_ps)
    fins = rec.get("financials") or []
    # TTM EPS/revenue: derive from price/PE and mktcap/PS (both are TTM) FIRST — robust to
    # financials ordering; fall back to the latest statement only if a multiple is missing.
    eps_ttm = (price / pe) if (price and pe and pe > 0) else None
    rev_ttm = (mc / ps) if (mc and ps and ps > 0) else None
    if eps_ttm is None and fins: eps_ttm = num(fins[0].get("eps")) or num(fins[-1].get("eps"))
    if rev_ttm is None and fins: rev_ttm = num(fins[0].get("revenue")) or num(fins[-1].get("revenue"))

    # growth driver: prefer the real analyst forward-revenue-growth feed, else trailing YoY
    used_fwd = rec.get("fwd_rev_growth") is not None
    g = rec.get("fwd_rev_growth")
    if g is None: g = rec.get("rev_growth_yoy")
    if g is None or not price: return None
    # clamp to a defensible band — trailing YoY can be distorted by M&A (+99% etc.), which would
    # produce fantasy targets; 60% is an aggressive-but-sane ceiling for a boom name.
    g = max(-0.4, min(g / 100.0, 0.6))
    g_capped = used_fwd is False and (rec.get("rev_growth_yoy") or 0) / 100.0 > 0.6
    f1, f2 = (1 + g), (1 + g) ** 2                 # 1-year and growth-sustained-2-year factors

    backlog_yoy = None
    if isinstance(pressure_entry, dict):
        backlog_yoy = pressure_entry.get("backlog_yoy_pct")
        if backlog_yoy is None: backlog_yoy = pressure_entry.get("new_orders_yoy_pct")

    out = {
        "growth_1y_pct":  round(g * 100, 1),
        "growth_source":  ("analyst forward revenue" if used_fwd
                           else ("trailing YoY (capped 60%)" if g_capped else "trailing revenue YoY")),
        "backlog_yoy_pct": backlog_yoy,
        "industry_pe":    ipe,
        "industry_ps":    ips,
        "cur_pe":         round(pe, 1) if pe else None,
        "cur_ps":         round(ps, 2) if ps else None,
    }
    # forward (next-12m) multiples — earnings/sales grow one year
    if pe and pe > 0: out["fwd_pe"] = round(pe / f1, 1)
    if ps and ps > 0: out["fwd_ps"] = round(ps / f1, 2)
    # projected (growth sustained ~2y) multiples — the re-rate runway
    if pe and pe > 0: out["proj_pe"] = round(pe / f2, 1)
    if ps and ps > 0: out["proj_ps"] = round(ps / f2, 2)
    # vs industry — P/E
    if ipe:
        if pe and pe > 0:      out["cur_pe_vs_ind_pct"]  = round((pe / ipe - 1) * 100)
        if out.get("fwd_pe"):  out["fwd_pe_vs_ind_pct"]  = round((out["fwd_pe"] / ipe - 1) * 100)
        if out.get("proj_pe"): out["proj_pe_vs_ind_pct"] = round((out["proj_pe"] / ipe - 1) * 100)
    # vs industry — P/S (industry P/S = median of bottleneck peers in the same industry)
    if ips:
        if ps and ps > 0:      out["cur_ps_vs_ind_pct"]  = round((ps / ips - 1) * 100)
        if out.get("fwd_ps"):  out["fwd_ps_vs_ind_pct"]  = round((out["fwd_ps"] / ips - 1) * 100)
        if out.get("proj_ps"): out["proj_ps_vs_ind_pct"] = round((out["proj_ps"] / ips - 1) * 100)
    # price prediction (from forward EPS)
    if eps_ttm and eps_ttm > 0:
        fwd_eps = eps_ttm * f1
        out["fwd_eps"] = round(fwd_eps, 2)
        # base: multiple held, price tracks one year of earnings growth
        out["tp_base"] = round(price * f1, 2)
        out["tp_base_upside_pct"] = round((f1 - 1) * 100)
        # bull: re-rate to the industry P/E on forward earnings
        if ipe:
            tp_bull = ipe * fwd_eps
            out["tp_bull"] = round(tp_bull, 2)
            out["tp_bull_upside_pct"] = round((tp_bull / price - 1) * 100)
        # bear: growth only ~40% delivers, no re-rate
        out["tp_bear"] = round(price * (1 + g * 0.4), 2)
        out["tp_bear_upside_pct"] = round(g * 0.4 * 100)
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        src = json.loads(S3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"no source: {e}"})}
    ranks = (src.get("ranks") or [])[:TOP_N]
    top_calls = set(src.get("top_calls") or [])
    try:
        cache = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()).get("by_ticker", {})
    except Exception:
        cache = {}
    now = datetime.now(timezone.utc)
    ind_pe, sec_pe = fetch_peer_pe()
    si_f, f13_f, fwd_f, chain_f = load_confirmation_feeds()
    pgr_trend = pressure_trend(src.get("industry_pressure") or {})
    pgr_pct = pressure_percentiles(src.get("industry_pressure") or {})

    tickers = [r["ticker"] for r in ranks]
    fin_map = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_financials, tk): tk for tk in tickers}
        for f in as_completed(futs):
            try:
                fin_map[futs[f]] = f.result()
            except Exception:
                fin_map[futs[f]] = {}

    out = {}
    need = []
    # industry P/S benchmark = median P/S of the bottleneck peers in each industry (FMP has no
    # industry-P/S snapshot, so we use the real candidate-universe median; >=2 peers required).
    from statistics import median as _median
    _ps_by_ind = {}
    for _r in ranks:
        _f = fin_map.get(_r["ticker"], {}) or {}
        _ind = _f.get("industry") or _r.get("industry")
        _psv = num(_f.get("ps")) or num(_r.get("ps_ttm"))
        if _ind and _psv and _psv > 0:
            _ps_by_ind.setdefault(_ind, []).append(_psv)
    industry_ps_median = {k: round(_median(v), 2) for k, v in _ps_by_ind.items() if len(v) >= 2}
    for r in ranks:
        tk = r["ticker"]
        fin = fin_map.get(tk, {}) or {}
        ind = fin.get("industry") or r.get("industry")
        industry_pe = ind_pe.get(ind) or sec_pe.get(fin.get("sector") or r.get("sector"))
        rec = {
            "name": r.get("name"), "industry": ind, "sector": fin.get("sector") or r.get("sector"),
            "desc": fin.get("desc"), "ceo": fin.get("ceo"), "employees": fin.get("employees"),
            "website": fin.get("website"), "exchange": fin.get("exchange"),
            "mkt_cap": fin.get("mkt_cap") or r.get("mkt_cap"), "price": fin.get("price"),
            "range_52w": fin.get("range_52w"), "beta": fin.get("beta"),
            "pe": fin.get("pe"), "ps": fin.get("ps") or r.get("ps_ttm"), "pb": fin.get("pb"),
            "div_yield": fin.get("div_yield"), "industry_pe": industry_pe,
            "financials": fin.get("financials") or [],
            "rev_growth_yoy": r.get("rev_growth_yoy"), "rev_accel_pp": r.get("rev_accel_pp"),
            "rev_to_mcap_pct": r.get("rev_to_mcap_pct"), "boom_score": r.get("boom_score"),
            "group_pressure": r.get("group_pressure"), "is_top_call": tk in top_calls,
            "next_earnings": fin.get("next_earnings"),
            "off_52w_high": fin.get("off_52w_high"), "off_52w_low": fin.get("off_52w_low"),
            "gm_trend": fin.get("gm_trend"), "gm_latest": fin.get("gm_latest"),
            "om_latest": fin.get("om_latest"), "fcfm_latest": fin.get("fcfm_latest"),
            "share_chg_pct": fin.get("share_chg_pct"),
            "ret_1m": fin.get("ret_1m"), "ret_3m": fin.get("ret_3m"), "price_spark": fin.get("price_spark"),
            "pe_low": fin.get("pe_low"), "pe_high": fin.get("pe_high"), "pe_pctile": fin.get("pe_pctile"),
            "beat_rate": fin.get("beat_rate"), "beats_n": fin.get("beats_n"),
            "nq_eps_est": fin.get("nq_eps_est"), "nq_rev_est": fin.get("nq_rev_est"),
            "cash_conv": fin.get("cash_conv"), "accruals": fin.get("accruals"),
            "cur_ratio": fin.get("cur_ratio"), "int_cov": fin.get("int_cov"),
            "net_debt_ebitda": fin.get("net_debt_ebitda"), "peg": fin.get("peg"),
            "ev_ebitda": fin.get("ev_ebitda"), "acq_driven": fin.get("acq_driven"),
            "acq_pct": fin.get("acq_pct"), "seg_conc": fin.get("seg_conc"), "seg_n": fin.get("seg_n"),
            "insider_sig": fin.get("insider_sig"), "insider_net": fin.get("insider_net"),
            "insider_buys": fin.get("insider_buys"), "insider_sells": fin.get("insider_sells"),
            "pressure_group": r.get("pressure_group"),
        }
        # trap-check: is the thesis actually working, or a value trap?
        _accel = rec["rev_accel_pp"]; _gmt = fin.get("gm_trend")
        _cheap = ((industry_pe and rec["pe"] and rec["pe"] < industry_pe)
                  or (rec.get("rev_to_mcap_pct") and rec["rev_to_mcap_pct"] >= 20))
        if _accel is not None and _accel < 0:
            rec["trap"] = "trap"          # decelerating while cheap = the value-trap signature
        elif _accel is not None and _accel > 0 and (_gmt is None or _gmt >= -1) and _cheap:
            rec["trap"] = "real"          # accelerating + margins holding/expanding + cheap
        else:
            rec["trap"] = "watch"
        # confirmation signals from the wider engine fleet (matched by ticker)
        _s = si_f.get(tk) or {}
        if _s:
            rec["short_pct"] = _s.get("latest_short_pct"); rec["short_signal"] = _s.get("signal")
        _f = f13_f.get(tk) or {}
        if _f:
            rec["sm_funds"] = _f.get("n_funds_holding")
            rec["sm_net"] = (_f.get("n_funds_adding") or 0) - (_f.get("n_funds_trimming") or 0)
            rec["sm_new"] = _f.get("n_funds_new_position"); rec["sm_value"] = _f.get("total_value")
        if tk in fwd_f:
            rec["fwd_rev_growth"] = fwd_f.get(tk)
        if tk in chain_f:
            rec["chain"] = chain_f.get(tk)
        rec["pressure_trend"] = pgr_trend.get(r.get("pressure_group"))
        rec["pressure_pctile"] = pgr_pct.get(r.get("pressure_group"))
        _pentry = (src.get("industry_pressure") or {}).get(r.get("pressure_group")) or {}
        rec["fwd_val"] = forward_valuation(rec, _pentry, industry_ps_median.get(ind))
        cached = cache.get(tk, {})
        ts = cached.get("thesis_at")
        fresh = False
        if ts:
            try:
                fresh = (now - datetime.fromisoformat(ts)).total_seconds() < THESIS_CACHE_HRS * 3600
            except Exception:
                fresh = False
        rec["thesis"], rec["thesis_at"] = cached.get("thesis"), cached.get("thesis_at")
        rec["bear"] = cached.get("bear")
        if not (fresh and cached.get("thesis") and cached.get("thesis_ver") == THESIS_VER):
            need.append(tk)
        else:
            rec["thesis_ver"] = THESIS_VER
        out[tk] = rec

    # generate stale/new theses IN PARALLEL (independent LLM calls) to fit the timeout
    new_theses = 0
    targets = need[:MAX_NEW_THESES]

    def _gen(tk):
        rec = out[tk]
        return tk, make_thesis(rec.get("name") or tk, tk, rec.get("industry"), rec)

    if targets:
        import concurrent.futures as _cf
        THESIS_BUDGET_S = 90   # never let AI thesis generation consume the whole Lambda timeout —
                               # the data layer (targets, multiples, track record, maturity) must
                               # always get written even when the LLM is slow/out (credit outage).
        _ex = _cf.ThreadPoolExecutor(max_workers=6)
        try:
            futs = {_ex.submit(_gen, tk): tk for tk in targets}
            for fut in _cf.as_completed(futs, timeout=THESIS_BUDGET_S):
                try:
                    tk, res = fut.result()
                    th, be = res
                    if th:
                        out[tk]["thesis"], out[tk]["thesis_at"] = th, now.isoformat()
                        out[tk]["bear"] = be
                        out[tk]["thesis_ver"] = THESIS_VER
                        new_theses += 1
                except Exception:
                    pass
        except _cf.TimeoutError:
            print(f"[research] thesis budget {THESIS_BUDGET_S}s hit; writing data with "
                  f"{new_theses} new theses, the rest stay cached/pending for next run")
        finally:
            _ex.shutdown(wait=False)   # critical: do NOT block on still-hung LLM threads

    # --- sector momentum overlay (is this corner of the market working?) ---
    import statistics as _st
    by_sector = {}
    for rec in out.values():
        s = rec.get("sector"); rm = rec.get("ret_1m")
        if s and rm is not None:
            by_sector.setdefault(s, []).append(rm)
    sector_mom = {s: round(_st.median(v), 1) for s, v in by_sector.items() if v}
    for rec in out.values():
        rec["sector_mom"] = sector_mom.get(rec.get("sector"))

    # --- conviction scorecard: tally bullish vs bearish signals (surface DISAGREEMENT) ---
    for rec in out.values():
        bull, bearf = [], []
        if rec.get("trap") == "real": bull.append("trap-check: real play")
        if rec.get("trap") == "trap": bearf.append("trap-check: revenue decelerating")
        cc = rec.get("cash_conv")
        if cc is not None and cc >= 80: bull.append("strong cash conversion")
        if cc is not None and cc < 50: bearf.append("weak cash conversion")
        ac = rec.get("accruals")
        if ac is not None and ac > 15: bearf.append("high accruals / low earnings quality")
        nde = rec.get("net_debt_ebitda")
        if nde is not None and nde > 4: bearf.append("high leverage")
        if nde is not None and 0 <= nde < 2: bull.append("low leverage")
        if rec.get("insider_sig") == "buying": bull.append("insiders buying")
        if rec.get("insider_sig") == "selling": bearf.append("insiders selling")
        pt = rec.get("pressure_trend")
        if pt is not None and pt > 1: bull.append("bottleneck intensifying")
        if pt is not None and pt < -1: bearf.append("bottleneck easing")
        pp = rec.get("pe_pctile")
        if pp is not None and pp < 40: bull.append("cheap vs own history")
        if pp is not None and pp > 80: bearf.append("expensive vs own history")
        if (rec.get("sm_funds") is not None) and (rec.get("sm_net") or 0) > 0: bull.append("13F funds adding")
        if rec.get("beat_rate") is not None and rec["beat_rate"] >= 70: bull.append("consistent earnings beats")
        if rec.get("acq_driven"): bearf.append("acquisition-driven growth")
        if rec.get("seg_conc") is not None and rec["seg_conc"] > 70: bearf.append("revenue concentration")
        sp = rec.get("short_pct")
        if sp is not None and sp >= 20: bull.append("high short interest (squeeze fuel)")
        rec["score_bull"] = len(bull); rec["score_bear"] = len(bearf)
        rec["flags_bull"] = bull; rec["flags_bear"] = bearf

    # --- #2 theme concentration of the top 10 ---
    from collections import Counter
    grp = Counter(r.get("pressure_group") for r in ranks[:10] if r.get("pressure_group"))
    dom_g, dom_n = (grp.most_common(1)[0] if grp else (None, 0))
    concentration = {"dominant_group": dom_g, "count": dom_n, "of": min(10, len(ranks)), "groups": dict(grp)}

    # --- #3 what changed since the last run ---
    ranked_tk = [r["ticker"] for r in ranks]
    trap_by_tk = {t: (out.get(t, {}) or {}).get("trap") for t in ranked_tk if (out.get(t, {}) or {}).get("trap")}
    changes = compute_changes(ranked_tk, trap_by_tk)

    # --- #1 track record (forward test vs SPY) ---
    track = grade_track_record()
    # log today's price targets + grade matured ones (closes the loop on the valuation layer)
    try:
        log_targets(out, top_calls)
        target_record = grade_targets()
    except Exception as e:
        print(f"[targets] loop fail {str(e)[:80]}"); target_record = None

    payload = {
        "engine": "bottleneck-research", "version": VERSION,
        "generated_at": now.isoformat(), "source_generated_at": src.get("generated_at"),
        "n": len(out), "new_theses": new_theses, "duration_s": round(time.time() - t0, 1),
        "concentration": concentration, "changes": changes, "track_record": track,
        "target_record": target_record,
        "pressure_pctiles": pgr_pct,
        "by_ticker": out,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[research] enriched {len(out)} tickers, {new_theses} new theses in {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(out), "new_theses": new_theses})}
