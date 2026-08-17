"""justhodl-spx-beaters v1.0.0 -- the weekly "who beats the S&P?" league.

Every Saturday: scan ALL caps (large/mid/small/micro via universe.json,
~5,300 names), ALL ETFs (etf-census + compass + rotation tickers) and the
asset-class board, score each candidate's odds of beating SPX, and say WHY
with real evidence lines. Fusion + one original factor:

  MOM   own full-market weekly-close ledger (Polygon grouped-daily, one
        call per Friday, self-bootstrapping to 53 weeks) -> 6m and 12-1
        cross-sectional momentum vs SPY. The most replicated systematic
        SPX-beating factor in the literature; nothing in the fleet
        computed it market-wide until now.
  FLEET stock-buying tiers + best-setups membership + master-ranker +
        invest tier-3 picks (each already encodes "why own vs SPX").
  FLOWS 13F net dollars by ticker (5,433 covered) + congress-alpha buys.
  IND   industry-boom league percentile of the name's industry.
  QUAL  S&P members only: the sp500 engine's five-pillar composite,
        recomputed from data/sp500.json members block.
  ETFs  compass market-implied ER spread vs SPY + rotation L1-L4 trend
        gate/rank/quadrant + same momentum ledger.

Macro gates SIZING, not selection (brain doctrine): risk-gate sizing +
rotation regime + SPX ERP/Rule-of-20 ship as a context header, never as a
score multiplier. Score = weighted average of AVAILABLE legs (weights
renormalize; >=2 legs required to list). Honest partials: first runs
carry a short ledger -> 12-1 flagged partial until 53 weeks accrue.
"""
import gzip
import io
import json
import os
import time
import ssl
import urllib.request
from bisect import bisect_left
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.2.0"
MARKER = "spx-beaters v1.2.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/spx-beaters.json"
LEDGER_KEY = "spx-beaters/weekly-closes.json"
POLY = os.environ.get("POLYGON_API_KEY") or ""
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY") or ""
AI_MODEL = "claude-haiku-4-5-20251001"
AI_CACHE_KEY = "spx-beaters/ai-cache.json"
AI_TOP_PER_BUCKET = 6
CTX = ssl.create_default_context()
MOM_ALIAS = {"BTC": "IBIT", "ETH": "ETHA"}  # pseudo-tickers -> listed proxy
TARGET_WEEKS = 53
MAX_FETCH = 30
PER_BUCKET = 15
COMEBACK_TOP = 20
COMEBACK_MIN = 60.0
W_CB = {"quality": .30, "cheap": .25, "accum": .20,
        "stabilize": .15, "revisions": .10}
MIN_SCORE = 55.0
W_STOCK = {"mom": .30, "fleet": .25, "flows": .15, "industry": .15,
           "quality": .15}
W_ETF = {"mom": .35, "er": .35, "rotation": .30}

s3 = boto3.client("s3")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json")


def _j(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent":
                                               "justhodl-spx-beaters"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def fnum(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def pctile(sorted_vals, x):
    if x is None or not sorted_vals:
        return None
    return round(100.0 * bisect_left(sorted_vals, x)
                 / len(sorted_vals), 1)


def rows_of(doc, *keys):
    """Tolerant container reader; ops G0 verifies the live key."""
    if not isinstance(doc, dict):
        return []
    for k in keys:
        v = doc.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def tick_of(r):
    for k in ("ticker", "symbol", "t", "sym"):
        v = r.get(k)
        if v:
            return str(v).upper()
    return None


# ------------------------------------------------------------ ledger --
def fridays_back(n):
    d = datetime.now(timezone.utc).date()
    d -= timedelta(days=(d.weekday() - 4) % 7 or 7)  # last completed Fri
    return [(d - timedelta(weeks=i)).isoformat() for i in range(n)]


def build_ledger(want, diag):
    led = _g(LEDGER_KEY) or {"dates": [], "closes": {}}
    dates = led["dates"]
    have = set(dates)
    missing = [ds for ds in fridays_back(TARGET_WEEKS)
               if ds not in have][:MAX_FETCH]
    fetched = 0
    for ds in missing:
        mp = None
        for shift in (0, 1):  # Friday, else Thursday (holiday)
            day = (datetime.fromisoformat(ds)
                   - timedelta(days=shift)).date().isoformat()
            j = _j("https://api.polygon.io/v2/aggs/grouped/locale/us/"
                   f"market/stocks/{day}?adjusted=true&apiKey={POLY}",
                   timeout=30)
            res = (j or {}).get("results") or []
            if res:
                mp = {r["T"]: round(r["c"], 4) for r in res
                      if r.get("T") in want
                      and isinstance(r.get("c"), (int, float))}
                break
        if not mp:
            continue
        dates.append(ds)
        for t in want:
            arr = led["closes"].setdefault(t, [None] * (len(dates) - 1))
            while len(arr) < len(dates) - 1:
                arr.append(None)
            arr.append(mp.get(t))
        fetched += 1
        time.sleep(0.12)
    order = sorted(range(len(dates)), key=lambda i: dates[i])
    if order != list(range(len(dates))):
        led["dates"] = [dates[i] for i in order]
        for t, arr in led["closes"].items():
            while len(arr) < len(dates):
                arr.append(None)
            led["closes"][t] = [arr[i] for i in order]
    else:
        led["dates"] = dates
    if fetched:
        _put(LEDGER_KEY, led)
    diag["ledger"] = {"weeks": len(led["dates"]),
                      "target": TARGET_WEEKS, "fetched_now": fetched,
                      "complete": len(led["dates"]) >= TARGET_WEEKS,
                      "first": led["dates"][0] if led["dates"] else None,
                      "last": led["dates"][-1] if led["dates"] else None}
    return led


def rets(led, t):
    """(ret_6m, ret_12_1) from weekly closes; None where history short
    or ticker gapped."""
    arr = led["closes"].get(t) or []
    nz = [(i, v) for i, v in enumerate(arr) if v]
    if not nz:
        return None, None
    last_i, last = nz[-1]

    def at(back):
        idx = last_i - back
        if idx < 0:
            return None
        v = arr[idx]
        if v:
            return v
        for j in (idx - 1, idx + 1):
            if 0 <= j < len(arr) and arr[j]:
                return arr[j]
        return None

    p26 = at(26)
    r6 = (last / p26 - 1) if p26 else None
    p52, p4 = at(52), at(4)
    r121 = (p4 / p52 - 1) if p52 and p4 else None
    return r6, r121


def inst_stats(led, t, spy_arr):
    """52w institutional risk block from weekly closes: annualized vol,
    max drawdown, vol-adjusted 12-1 (Sharpe-style), and 26w RS
    consistency vs SPY (% of weeks beating)."""
    arr = [v for v in (led["closes"].get(t) or []) if v]
    if len(arr) < 30:
        return None
    rets = [arr[i] / arr[i - 1] - 1 for i in range(1, len(arr))]
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / max(1, len(rets) - 1)
    vol = (var ** 0.5) * (52 ** 0.5) * 100
    peak, mdd = arr[0], 0.0
    for v in arr:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1)
    r121 = None
    if len(arr) >= 53:
        r121 = arr[-5] / arr[-53] - 1
    sharpe_mom = (round(r121 / (vol / 100), 2)
                  if r121 is not None and vol > 1 else None)
    cons = None
    if spy_arr and len(arr) >= 27 and len(spy_arr) >= 27:
        w = 0
        n = 0
        a2, s2 = arr[-27:], spy_arr[-27:]
        for i in range(1, min(len(a2), len(s2))):
            ra = a2[i] / a2[i - 1] - 1
            rs = s2[i] / s2[i - 1] - 1
            n += 1
            if ra > rs:
                w += 1
        cons = round(100 * w / n, 0) if n else None
    return {"vol_52w_pct": round(vol, 1),
            "max_dd_52w_pct": round(mdd * 100, 1),
            "sharpe_mom": sharpe_mom,
            "rs_consist_26w_pct": cons}


def base_rates(led, spy_arr, meta):
    """Empirical odds from OUR ledger: single 26w cohort. Formation =
    6m return as of 26w ago; outcome = excess return vs SPY over the
    following 26w. Quintile beat-rates + a comeback cohort (dd<=-30 at
    formation with a 4w base). Honest: one cohort, in-sample."""
    if not spy_arr or len(spy_arr) < 53:
        return None, {}
    spy_out = spy_arr[-1] / spy_arr[-27] - 1
    rows = []
    for t in meta:
        arr = [v for v in (led["closes"].get(t) or []) if v]
        if len(arr) < 53:
            continue
        form = arr[-27] / arr[-53] - 1
        out = arr[-1] / arr[-27] - 1
        dd_form = None
        peak = arr[0]
        for v in arr[:-26]:
            peak = max(peak, v)
        if peak:
            dd_form = arr[-27] / peak - 1
        base4 = arr[-27] / arr[-31] - 1 if len(arr) >= 31 else None
        rows.append((t, form, out - spy_out, dd_form, base4))
    if len(rows) < 200:
        return None, {}
    rows.sort(key=lambda r: r[1])
    n = len(rows)
    quints = []
    for q in range(5):
        seg = rows[int(n * q / 5):int(n * (q + 1) / 5)]
        ex = sorted(x[2] for x in seg)
        beat = sum(1 for x in seg if x[2] > 0)
        quints.append({
            "q": q + 1,
            "form_6m_min_pct": round(seg[0][1] * 100, 1),
            "form_6m_max_pct": round(seg[-1][1] * 100, 1),
            "n": len(seg),
            "beat_spy_26w_pct": round(100 * beat / len(seg), 1),
            "median_excess_pp": round(ex[len(ex) // 2] * 100, 1)})
    cb = [x for x in rows if x[3] is not None and x[3] <= -0.30
          and x[4] is not None and x[4] >= -0.03]
    cb_stat = None
    if len(cb) >= 25:
        ex = sorted(x[2] for x in cb)
        cb_stat = {"n": len(cb),
                   "beat_spy_26w_pct": round(
                       100 * sum(1 for x in cb if x[2] > 0)
                       / len(cb), 1),
                   "median_excess_pp": round(ex[len(ex) // 2]
                                             * 100, 1)}
    br = {"window": "26w outcome, 6m-return formation, single cohort "
                    "from our own weekly ledger (in-sample)",
          "spy_ret_26w_pct": round(spy_out * 100, 1),
          "momentum_quintiles": quints, "comeback_cohort": cb_stat,
          "caveat": "one non-overlapping cohort; real but short "
                    "history -- odds are anchors, not gospel"}
    # thresholds for mapping current 6m ret -> quintile
    th = [q["form_6m_max_pct"] / 100 for q in quints[:-1]]
    return th, br


def dd_base(led, t):
    """(drawdown from 52w high, 8w return, n_closes) from the ledger."""
    arr = [v for v in (led["closes"].get(t) or []) if v]
    if len(arr) < 40:
        return None, None, len(arr)
    hi = max(arr)
    dd = arr[-1] / hi - 1 if hi else None
    r8 = (arr[-1] / arr[-9] - 1) if len(arr) >= 9 else None
    return dd, r8, len(arr)


# ---------------------------------------------------- sp500 pillars --
SPX_PIL = {
    "valuation": (["pe_ttm", "pe_fwd", "peg_ttm", "ps_ttm", "pb",
                   "ev_ebitda_ttm", "ev_sales_ttm"], "low",
                  ["earnings_yield_pct", "fcf_yield_pct"]),
    "quality": (["sbc_to_revenue_pct"], "low",
                ["roe_pct", "roic_pct", "roa_pct", "gross_margin_pct",
                 "operating_margin_pct", "net_margin_pct",
                 "fcf_margin_pct", "income_quality", "piotroski_f"]),
    "growth": ([], "low",
               ["revenue_yoy_pct", "eps_yoy_pct",
                "revenue_cagr_3y_pct", "eps_cagr_3y_pct",
                "ntm_growth_pct"]),
    "balance": (["debt_to_equity", "netdebt_to_ebitda_ttm", "beta_2y"],
                "low", ["current_ratio", "interest_coverage_ttm",
                        "altman_z"]),
    "momentum": ([], "low", ["mom_6m_pct", "mom_12_1_pct"]),
}
SPX_PW = {"valuation": .30, "quality": .25, "growth": .25,
          "balance": .10, "momentum": .10}
SPX_MINN = {"valuation": 3, "quality": 4, "growth": 2, "balance": 3,
            "momentum": 1}


def spx_composites(sp):
    """{ticker: composite 0-100} recomputed from sp500.json members."""
    if not sp or not sp.get("members"):
        return {}
    mf = sp.get("member_fields") or []
    fi = {k: i for i, k in enumerate(mf)}
    mem = sp["members"]
    fields = set()
    for lows, _, highs in SPX_PIL.values():
        fields |= set(lows) | set(highs)
    arrs = {}
    for f in fields:
        if f not in fi:
            continue
        a = sorted(v for v in (r[fi[f]] for r in mem.values())
                   if isinstance(v, (int, float)))
        arrs[f] = a
    out = {}
    for t, r in mem.items():
        comp = wsum = 0.0
        pil = {}
        for pname, (lows, _, highs) in SPX_PIL.items():
            vals = []
            for f in lows + highs:
                if f not in arrs or f not in fi:
                    continue
                p = pctile(arrs[f], r[fi[f]]
                           if isinstance(r[fi[f]], (int, float))
                           else None)
                if p is None:
                    continue
                vals.append(100 - p if f in lows else p)
            if len(vals) >= SPX_MINN[pname]:
                sc = sum(vals) / len(vals)
                pil[pname] = round(sc, 1)
                comp += sc * SPX_PW[pname]
                wsum += SPX_PW[pname]
        if wsum:
            out[t] = {"composite": round(comp / wsum, 1),
                      "pillars": pil}
    return out


def ai_prompt(row, anchors, wing):
    return json.dumps({
        "task": "Verdict on whether this candidate beats the S&P 500.",
        "wing": wing, "candidate": row, "anchors": anchors,
        "rules": [
            "Respond ONLY with strict JSON, no prose.",
            "odds_beat_spx_26w_pct MUST stay within "
            "anchors.odds_base_26w_pct +/- 12; state the adjustment "
            "driver in one_liner.",
            "downside_risk_pct MUST lie between anchors.downside_lo "
            "and anchors.downside_hi (derived from realized 52w vol "
            "and max drawdown).",
            "horizon_weeks integer 13..52.",
            "stance: BUY only if odds >= 55 AND the evidence legs "
            "justify it; WATCH if borderline; PASS otherwise.",
            "one_liner <= 140 chars, cite the decisive evidence."],
        "format": {"stance": "BUY|WATCH|PASS",
                   "odds_beat_spx_26w_pct": 0,
                   "horizon_weeks": 26, "downside_risk_pct": 0,
                   "one_liner": ""}})


def ai_call(row, anchors, wing):
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({
        "model": AI_MODEL, "max_tokens": 300,
        "system": "You are the risk desk of a systematic fund. You "
                  "never invent numbers: every figure must derive "
                  "from the provided evidence and stay inside the "
                  "stated anchors. Strict JSON only.",
        "messages": [{"role": "user",
                      "content": ai_prompt(row, anchors, wing)}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=body,
        headers={"Content-Type": "application/json",
                 "x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01"})
    try:
        with urllib.request.urlopen(req, timeout=25,
                                    context=CTX) as r:
            txt = json.loads(r.read())["content"][0]["text"].strip()
        if txt.startswith("```"):
            txt = txt.strip("`")
            txt = txt[txt.find("{"):txt.rfind("}") + 1]
        return json.loads(txt)
    except Exception as e:  # noqa: BLE001
        print("[ai] %s %s" % (row.get("t"), e))
        return None


def ai_verdict(row, wing, cache):
    inst = row.get("inst") or {}
    vol = inst.get("vol_52w_pct") or 40.0
    mdd = abs(inst.get("max_dd_52w_pct") or vol)
    base = row.get("odds_base_26w_pct")
    if base is None:
        return None
    d_lo = round(max(8.0, 0.5 * vol), 0)
    d_hi = round(min(95.0, max(mdd, vol)), 0)
    if d_hi <= d_lo:
        d_hi = d_lo + 5
    anchors = {"odds_base_26w_pct": base, "downside_lo": d_lo,
               "downside_hi": d_hi,
               "horizon_hint_weeks": 39 if wing == "comeback" else 26}
    cached = cache.get(row["t"])
    raw = cached or ai_call(
        {k: row[k] for k in ("t", "name", "score", "legs", "why",
                             "dd_52w_pct", "ret_6m_pct",
                             "ret_12_1_pct", "inst", "sector")
         if k in row}, anchors, wing)
    mode = "llm" if raw else "rules"
    if not raw:
        sc = row.get("score", 0)
        odds = max(2.0, min(98.0, base + min(12.0, max(-12.0,
                                                       (sc - 62)
                                                       / 3.0))))
        raw = {"stance": "BUY" if sc >= 75 and odds >= 55
               else "WATCH" if sc >= 62 else "PASS",
               "odds_beat_spx_26w_pct": odds,
               "horizon_weeks": anchors["horizon_hint_weeks"],
               "downside_risk_pct": max(d_lo, min(d_hi,
                                                  round(0.8 * mdd
                                                        or vol))),
               "one_liner": (row.get("why") or ["evidence-weighted "
                                                "rule verdict"])[0]
               [:140]}
    # deterministic clamps regardless of source
    odds = fnum(raw.get("odds_beat_spx_26w_pct"))
    odds = base if odds is None else max(base - 12, min(base + 12,
                                                        odds))
    odds = max(2.0, min(98.0, odds))
    dwn = fnum(raw.get("downside_risk_pct"))
    dwn = d_hi if dwn is None else max(d_lo, min(d_hi, dwn))
    hz = fnum(raw.get("horizon_weeks")) or anchors[
        "horizon_hint_weeks"]
    hz = int(max(13, min(52, hz)))
    st = str(raw.get("stance") or "WATCH").upper()
    if st not in ("BUY", "WATCH", "PASS"):
        st = "WATCH"
    if st == "BUY" and odds < 55:
        st = "WATCH"
    return {"stance": st, "odds_beat_spx_26w_pct": round(odds, 0),
            "horizon_weeks": hz, "downside_risk_pct": round(dwn, 0),
            "one_liner": str(raw.get("one_liner") or "")[:150],
            "mode": mode, "anchors": anchors}


# ------------------------------------------------------------- main --
def scan():
    diag = {"feeds": {}}
    uni = _g("data/universe.json") or {}
    stocks = rows_of(uni, "stocks", "rows", "universe")
    diag["feeds"]["universe"] = len(stocks)

    etfc = _g("data/etf-census.json") or {}
    etf_rows = rows_of(etfc, "etfs", "rows", "matrix", "items")
    diag["feeds"]["etf_census"] = len(etf_rows)

    compass = _g("data/asset-compass.json") or {}
    c_assets = rows_of(compass, "assets")
    diag["feeds"]["asset_compass"] = len(c_assets)

    rot = _g("data/rotation-dashboard.json") or {}
    r_assets = rows_of(rot, "assets")
    diag["feeds"]["rotation"] = len(r_assets)

    sb = _g("data/stock-buying.json") or {}
    sb_rows = rows_of(sb, "top", "rows")
    sb_map = {tick_of(r): r for r in sb_rows if tick_of(r)}
    diag["feeds"]["stock_buying"] = len(sb_map)

    bs = _g("data/best-setups.json") or {}
    bs_rows = rows_of(bs, "setups", "rows", "top", "best_setups")
    bs_rank = {tick_of(r): i + 1 for i, r in enumerate(bs_rows)
               if tick_of(r)}
    diag["feeds"]["best_setups"] = len(bs_rank)

    mr = _g("data/master-ranker.json") or {}
    mr_rows = rows_of(mr, "rankings", "top", "rows", "tickers",
                      "results")
    mr_rank = {tick_of(r): i + 1 for i, r in enumerate(mr_rows)
               if isinstance(r, dict) and tick_of(r)}
    diag["feeds"]["master_ranker"] = len(mr_rank)

    inv = _g("data/invest.json") or {}
    inv_rows = rows_of(inv, "stock_picks", "picks", "rows")
    inv_set = {tick_of(r) for r in inv_rows
               if isinstance(r, dict) and tick_of(r)}
    diag["feeds"]["invest_picks"] = len(inv_set)

    f13 = (_g("data/13f-flows-by-ticker.json") or {}).get("t") or {}
    diag["feeds"]["flows_13f"] = len(f13)

    ca = _g("data/congress-alpha.json") or {}
    ca_set = set()
    for r in rows_of(ca, "signals", "rows", "buys"):
        if isinstance(r, dict) and tick_of(r):
            ca_set.add(tick_of(r))
    diag["feeds"]["congress_alpha"] = len(ca_set)

    # value-trap guards (master-ranker redflag pattern) + comeback feeds
    trap = {}
    for r in ((_g("data/beneish.json") or {}).get("red_flags") or []):
        t_ = tick_of(r)
        if t_:
            trap.setdefault(t_, []).append("Beneish manipulation flag")
    for r in ((_g("data/earnings-quality.json") or {})
              .get("top_10_low_quality_avoid") or []):
        t_ = tick_of(r)
        if t_:
            trap.setdefault(t_, []).append("low earnings quality")
    for r in ((_g("data/insider-sell-cluster.json") or {})
              .get("top_clusters") or []):
        t_ = tick_of(r)
        if t_:
            trap.setdefault(t_, []).append("insider selling cluster")
    for r in rows_of(_g("data/share-flows.json") or {}, "rows", "top"):
        t_ = tick_of(r)
        fl = r.get("flags") or []
        if t_ and any(f in ("SBC_WASH", "BUYBACK_BLUFF") for f in fl):
            trap.setdefault(t_, []).append("share-flows " +
                                           "/".join(fl[:2]))
    diag["feeds"]["trap_guards"] = len(trap)
    cbs = _g("data/comeback-screener.json") or {}
    cb_boards = cbs.get("boards") or {}
    cb_state, cb_trap = {}, set()
    for bname, rws in cb_boards.items():
        for r in (rws or []):
            t_ = tick_of(r)
            if not t_:
                continue
            if "DILUTION" in str(bname).upper():
                cb_trap.add(t_)
            else:
                cb_state[t_] = str(bname).upper()
    diag["feeds"]["comeback_screener"] = len(cb_state)
    rev = {}
    for r in rows_of(_g("data/eps-revision-velocity.json") or {},
                     "rows", "top", "stocks", "data"):
        t_ = tick_of(r)
        if not t_:
            continue
        v = None
        for k in ("velocity", "revision_velocity", "net_revisions",
                  "score"):
            if fnum(r.get(k)) is not None:
                v = fnum(r.get(k))
                break
        if v is not None:
            rev[t_] = v
    diag["feeds"]["eps_revisions"] = len(rev)

    boom = _g("data/industry-boom.json") or {}
    league = rows_of(boom, "league", "rows")
    boom_by_ind, boom_scores = {}, []
    for b in league:
        nm = str(b.get("industry") or "").lower()
        sc = fnum(b.get("score"))
        if nm and sc is not None:
            boom_by_ind[nm] = sc
            boom_scores.append(sc)
    boom_scores.sort()
    diag["feeds"]["industry_boom"] = len(boom_by_ind)

    sp = _g("data/sp500.json") or {}
    qual = spx_composites(sp)
    diag["feeds"]["sp500_pillars"] = len(qual)

    rg = _g("data/risk-gate.json") or {}

    # ---------------------------------------------------- ledger set --
    want = {"SPY"} | set(MOM_ALIAS.values())
    name_of, meta = {}, {}
    for r in stocks:
        t = tick_of(r)
        if not t:
            continue
        want.add(t)
        cb = str(r.get("cap_bucket") or "").lower()
        if cb in ("nano",):
            cb = "micro"
        if cb == "mega":
            cb = "large"
        if cb not in ("large", "mid", "small", "micro"):
            mc = fnum(r.get("market_cap")) or 0
            cb = ("large" if mc >= 10e9 else "mid" if mc >= 2e9
                  else "small" if mc >= 3e8 else "micro")
        meta[t] = {"name": (r.get("name") or "")[:44],
                   "sector": r.get("sector"),
                   "industry": str(r.get("industry") or ""),
                   "mcap": fnum(r.get("market_cap")), "bucket": cb}
    etf_class = {}
    for r in etf_rows:
        t = tick_of(r)
        if not t:
            continue
        want.add(t)
        etf_class[t] = (str(r.get("asset_class") or r.get("category")
                            or "equity").lower())
        name_of[t] = (r.get("name") or "")[:44]
    for r in c_assets + r_assets:
        t = tick_of(r)
        if t:
            want.add(t)
            name_of.setdefault(t, (r.get("label")
                                   or r.get("name") or "")[:44])

    led = build_ledger(want, diag)
    weeks = len(led["dates"])
    spy6, spy121 = rets(led, "SPY")
    spy_arr = [v for v in (led["closes"].get("SPY") or []) if v]
    q_th, br = base_rates(led, spy_arr, meta)

    def quintile_odds(t):
        if not q_th or not br:
            return None
        arr = [v for v in (led["closes"].get(t) or []) if v]
        if len(arr) < 27:
            return None
        r6c = arr[-1] / arr[-27] - 1
        qi = 0
        for th in q_th:
            if r6c > th:
                qi += 1
        return (br["momentum_quintiles"][qi]["beat_spy_26w_pct"], qi + 1)
    mom_ok_6 = weeks >= 27 and spy6 is not None
    mom_ok_121 = weeks >= TARGET_WEEKS and spy121 is not None

    # cross-sectional pools
    st_r6, st_r121, et_r6, et_r121 = [], [], [], []
    r6_map, r121_map = {}, {}
    for t in want:
        r6, r121 = rets(led, t)
        r6_map[t], r121_map[t] = r6, r121
        pool6 = st_r6 if t in meta else et_r6
        pool121 = st_r121 if t in meta else et_r121
        if r6 is not None:
            pool6.append(r6)
        if r121 is not None:
            pool121.append(r121)
    for a in (st_r6, st_r121, et_r6, et_r121):
        a.sort()

    def mom_leg(t, is_stock):
        mt = MOM_ALIAS.get(t, t)
        r6, r121 = r6_map.get(mt), r121_map.get(mt)
        p6 = pctile(st_r6 if is_stock else et_r6, r6) \
            if mom_ok_6 else None
        p121 = pctile(st_r121 if is_stock else et_r121, r121) \
            if mom_ok_121 else None
        if p6 is None and p121 is None:
            return None, None, None, None
        if p121 is None:
            leg = p6 / 100.0
        else:
            leg = (0.4 * (p6 or 50) + 0.6 * p121) / 100.0
        return leg, r6, r121, (p121 if p121 is not None else p6)

    # ------------------------------------------------------ stocks ----
    def score_stock(t):
        legs, why = {}, []
        m = meta[t]
        leg, r6, r121, mp = mom_leg(t, True)
        if leg is not None:
            legs["mom"] = leg
            rs6 = (r6 - spy6) * 100 if (r6 is not None
                                        and spy6 is not None) else None
            wtxt = "momentum: "
            if r121 is not None and mom_ok_121:
                wtxt += "12-1 %+.0f%% (top %.0f%% of all stocks)" % (
                    r121 * 100, 100 - mp)
            elif r6 is not None:
                wtxt += "6m %+.0f%% (top %.0f%%, 12-1 pending %d/%d " \
                        "wks)" % (r6 * 100, 100 - mp, weeks,
                                  TARGET_WEEKS)
            if rs6 is not None:
                wtxt += ", %+.0fpp vs SPY 6m" % rs6
            why.append(wtxt)
        sub = []
        sbr = sb_map.get(t)
        if sbr:
            sc = fnum(sbr.get("score"))
            if sc is not None:
                sub.append(min(1.0, sc / 100.0))
                why.append("stock-buying screener %s score %.0f (%s)"
                           % (sbr.get("tier") or "", sc,
                              "; ".join((sbr.get("gate_reasons")
                                         or [])[:2])))
        if t in bs_rank:
            sub.append(max(0.3, 1 - bs_rank[t] / 60.0))
            why.append("best-setups rank #%d" % bs_rank[t])
        if t in mr_rank:
            sub.append(max(0.3, 1 - mr_rank[t] / 40.0))
            why.append("master-ranker #%d" % mr_rank[t])
        if t in inv_set:
            sub.append(0.9)
            why.append("invest engine tier-3 pick (beat-SPX gates)")
        if sub:
            legs["fleet"] = sum(sub) / len(sub)
        fl = f13.get(t)
        if isinstance(fl, dict):
            net = None
            for k, v in fl.items():
                if "net" in k and fnum(v) is not None:
                    net = fnum(v)
                    break
            if net is None:
                nums = [fnum(v) for v in fl.values()
                        if fnum(v) is not None]
                net = nums[0] if nums else None
            if net is not None:
                legs["flows"] = 0.85 if net > 0 else 0.25
                why.append("13F net %s$%.0fM last quarters"
                           % ("+" if net > 0 else "-",
                              abs(net) / 1e6 if abs(net) > 1e6
                              else abs(net)))
        if t in ca_set:
            legs["flows"] = min(1.0, legs.get("flows", 0.5) + 0.15)
            why.append("congress-alpha disclosed buy (family hit "
                       "rate 57%)")
        ind = (m.get("industry") or "").lower()
        if ind and ind in boom_by_ind:
            bp = pctile(boom_scores, boom_by_ind[ind])
            if bp is not None:
                legs["industry"] = bp / 100.0
                why.append("industry '%s' boom score %.0f (top %.0f%% "
                           "of 120)" % (m["industry"][:28],
                                        boom_by_ind[ind], 100 - bp))
        if t in qual:
            legs["quality"] = qual[t]["composite"] / 100.0
            why.append("sp500 five-pillar composite %.0f/100 vs index"
                       % qual[t]["composite"])
        if len(legs) < 2 or set(legs) == {"mom", "industry"}:
            return None  # need name-specific evidence beyond momentum
        num = sum(W_STOCK[k] * v for k, v in legs.items())
        den = sum(W_STOCK[k] for k in legs)
        score = round(100 * num / den, 1)
        qo = quintile_odds(t)
        row = {"t": t, "name": m["name"], "sector": m.get("sector"),
               "industry": m.get("industry"), "mcap": m.get("mcap"),
               "score": score,
               "legs": {k: round(v, 2) for k, v in legs.items()},
               "n_legs": len(legs),
               "ret_6m_pct": round(r6 * 100, 1)
               if r6 is not None else None,
               "rs_6m_pp": round((r6 - spy6) * 100, 1)
               if r6 is not None and spy6 is not None else None,
               "ret_12_1_pct": round(r121 * 100, 1)
               if r121 is not None else None,
               "inst": inst_stats(led, t, spy_arr),
               "odds_base_26w_pct": qo[0] if qo else None,
               "mom_quintile": qo[1] if qo else None,
               "why": why[:6]}
        ins = row["inst"] or {}
        if ins.get("sharpe_mom") is not None and                 ins["sharpe_mom"] >= 1.0:
            row["why"].append("vol-adjusted 12-1 (Sharpe-mom) %.1f -- "
                              "trend not just noise"
                              % ins["sharpe_mom"])
        if ins.get("rs_consist_26w_pct") is not None and                 ins["rs_consist_26w_pct"] >= 60:
            row["why"].append("beat SPY in %.0f%% of the last 26 "
                              "weeks" % ins["rs_consist_26w_pct"])
        return row

    buckets = {b: [] for b in ("large", "mid", "small", "micro")}
    scanned = 0
    for t in meta:
        row = score_stock(t)
        scanned += 1
        if row and row["score"] >= MIN_SCORE:
            buckets[meta[t]["bucket"]].append(row)
    for b in buckets:
        buckets[b].sort(key=lambda r: -r["score"])

    # -------------------------------------------------------- ETFs ----
    c_by_t = {tick_of(r): r for r in c_assets if tick_of(r)}
    r_by_t = {tick_of(r): r for r in r_assets if tick_of(r)}
    spy_c = c_by_t.get("SPY") or {}
    spy_er5 = fnum(spy_c.get("er_5y_pct"))

    def score_etf(t):
        legs, why = {}, []
        leg, r6, r121, mp = mom_leg(t, False)
        if leg is not None:
            legs["mom"] = leg
            if r6 is not None and spy6 is not None:
                why.append("momentum%s: 6m %+.0f%% (%+.0fpp vs SPY)"
                           % (" (via %s)" % MOM_ALIAS[t]
                              if t in MOM_ALIAS else "",
                              r6 * 100, (r6 - spy6) * 100))
        ca_row = c_by_t.get(t)
        if ca_row:
            er5 = fnum(ca_row.get("er_5y_pct"))
            er1 = fnum(ca_row.get("er_1y_pct"))
            if er5 is not None and spy_er5 is not None:
                spread = er5 - spy_er5
                legs["er"] = max(0.0, min(1.0, 0.5 + spread / 8.0))
                why.append("market-implied ER 5y %.1f%% vs SPY %.1f%% "
                           "(%+.1fpp)" % (er5, spy_er5, spread))
            elif er1 is not None:
                legs["er"] = max(0.0, min(1.0, 0.5 + er1 / 20.0))
                why.append("market-implied ER 1y %.1f%%" % er1)
        rr = r_by_t.get(t)
        if rr:
            elig = ((rr.get("trend_gate") or {}).get("eligible")
                    if isinstance(rr.get("trend_gate"), dict)
                    else rr.get("eligible"))
            rk = fnum(rr.get("rank"))
            quad = str(((rr.get("rrg") or {}).get("quadrant")
                        if isinstance(rr.get("rrg"), dict)
                        else rr.get("quadrant")) or "")
            v = 0.5
            bits = []
            if elig is True:
                v += 0.25
                bits.append("trend gate PASS (px>200d & 12m>cash)")
            elif elig is False:
                v -= 0.30
                bits.append("trend gate FAIL")
            if rk:
                v += max(0.0, (20 - rk) / 80.0)
                bits.append("rotation rank #%d" % int(rk))
            if quad.lower() in ("improving", "leading"):
                v += 0.10
                bits.append("RRG " + quad)
            legs["rotation"] = max(0.0, min(1.0, v))
            if bits:
                why.append("; ".join(bits))
        if len(legs) < 2:
            return None
        num = sum(W_ETF[k] * v for k, v in legs.items())
        den = sum(W_ETF[k] for k in legs)
        score = round(100 * num / den, 1)
        cls = etf_class.get(t, "")
        if not cls:
            lbl = (name_of.get(t) or "").lower()
            cls = ("crypto" if "bit" in lbl or t in ("IBIT", "FBTC",
                                                     "GBTC", "ETHA")
                   else "bond" if any(w in lbl for w in
                                      ("bond", "treasur", "credit"))
                   else "commodity" if any(w in lbl for w in
                                           ("gold", "silver", "oil",
                                            "commod"))
                   else "equity")
        qo = quintile_odds(t)
        return {"t": t, "name": name_of.get(t) or t, "class": cls,
                "inst": inst_stats(led, t, spy_arr),
                "odds_base_26w_pct": qo[0] if qo else None,
                "mom_quintile": qo[1] if qo else None,
                "score": score,
                "legs": {k: round(v, 2) for k, v in legs.items()},
                "n_legs": len(legs),
                "ret_6m_pct": round(r6 * 100, 1)
                if r6 is not None else None,
                "rs_6m_pp": round((r6 - spy6) * 100, 1)
                if r6 is not None and spy6 is not None else None,
                "ret_12_1_pct": round(r121 * 100, 1)
                if r121 is not None else None,
                "why": why[:5]}

    etf_pool = (set(etf_class) | set(c_by_t) | set(r_by_t)) - {"SPY"}
    etfs = []
    for t in etf_pool:
        row = score_etf(t)
        if row and row["score"] >= MIN_SCORE:
            etfs.append(row)
    etf_buckets = {"etf_equity": [], "etf_bond": [],
                   "etf_commodity": [], "etf_crypto_alt": []}
    for r in etfs:
        c = r["class"]
        key = ("etf_bond" if "bond" in c or "fixed" in c
               else "etf_commodity" if "commod" in c or c in
               ("gold", "silver", "energy")
               else "etf_crypto_alt" if "crypto" in c or "digital" in c
               else "etf_equity")
        etf_buckets[key].append(r)
    for b in etf_buckets:
        etf_buckets[b].sort(key=lambda r: -r["score"])

    # -------------------------------- COMEBACK wing (quality, beaten
    # up, cheap vs the index, evidence it turns) ---------------------
    sp_fi = {k: i for i, k in enumerate(sp.get("member_fields") or [])}
    sp_mem = sp.get("members") or {}
    idx_fpe = ((sp.get("forward") or {}).get("pe_fwd")
               or {}).get("agg")

    def cb_row(t):
        if t in trap or t in cb_trap:
            return None
        m = meta[t]
        dd, r8, ncl = dd_base(led, t)
        if dd is None or dd > -0.30:
            return None
        state = cb_state.get(t)
        if (r8 is None or r8 < -0.03) and state not in ("CONFIRMED",
                                                        "EARLY_TURN"):
            return None  # still falling, no independent turn signal
        legs, why = {}, []
        why.append("%.0f%% below 52w high (weekly closes, %dw)"
                   % (dd * 100, ncl))
        q = qual.get(t)
        if q and q["pillars"].get("quality") is not None:
            qs = q["pillars"]["quality"]
            if qs >= 55:
                legs["quality"] = qs / 100.0
                why.append("quality pillar %.0f/100 inside the S&P"
                           % qs)
        else:
            sbr = sb_map.get(t)
            sc = fnum((sbr or {}).get("score"))
            if sc is not None and sc >= 55:
                legs["quality"] = min(0.9, sc / 100.0)
                why.append("stock-buying screener quality score %.0f "
                           "(%s)" % (sc, (sbr.get("tier") or "")))
        if "quality" not in legs:
            return None  # Khalid: GOOD QUALITY only
        if q and q["pillars"].get("valuation") is not None:
            vs = q["pillars"]["valuation"]
            legs["cheap"] = vs / 100.0
            fpe_i = sp_fi.get("pe_fwd")
            row = sp_mem.get(t)
            if row is not None and fpe_i is not None and idx_fpe:
                x = row[fpe_i]
                if isinstance(x, (int, float)):
                    why.append("fwd P/E %.1f vs index %.1f (%+.0f%%)"
                               % (x, idx_fpe, (x / idx_fpe - 1) * 100))
            else:
                why.append("valuation pillar %.0f/100 (cheaper than "
                           "most of the index)" % vs)
        else:
            peg = fnum((sb_map.get(t) or {}).get("peg"))
            if peg is not None and 0 < peg < 1.5:
                legs["cheap"] = 0.8 if peg < 1.0 else 0.6
                why.append("PEG %.2f" % peg)
        fl = f13.get(t)
        if isinstance(fl, dict):
            net = None
            for k, v in fl.items():
                if "net" in k and fnum(v) is not None:
                    net = fnum(v)
                    break
            if net is not None and net > 0:
                legs["accum"] = 0.85
                why.append("13F net +$%.0fM while the stock is down "
                           "-- accumulation into weakness"
                           % (net / 1e6 if abs(net) > 1e6 else net))
        if t in ca_set:
            legs["accum"] = min(1.0, legs.get("accum", 0.5) + 0.15)
            why.append("congress-alpha disclosed buy")
        st = 0.5 + min(0.5, max(0.0, (r8 or 0)) * 2.5)
        if state == "CONFIRMED":
            st = max(st, 0.85)
            why.append("comeback-screener CONFIRMED (SMA200 "
                       "reclaimed)")
        elif state == "EARLY_TURN":
            st = max(st, 0.65)
            why.append("comeback-screener EARLY_TURN")
        elif r8 is not None:
            why.append("8w base %+.0f%% -- no longer falling"
                       % (r8 * 100))
        legs["stabilize"] = st
        if fnum(rev.get(t)) is not None and rev[t] > 0:
            legs["revisions"] = 0.8
            why.append("EPS revision velocity positive")
        if len(legs) < 3:
            return None
        num = sum(W_CB[k] * v for k, v in legs.items())
        den = sum(W_CB[k] for k in legs)
        score = round(100 * num / den, 1)
        if score < COMEBACK_MIN:
            return None
        r6, r121 = r6_map.get(t), r121_map.get(t)
        cb_odds = ((br or {}).get("comeback_cohort")
                   or {}).get("beat_spy_26w_pct")
        if cb_odds is None:
            qo = quintile_odds(t)
            cb_odds = qo[0] if qo else None
        return {"t": t, "name": m["name"], "sector": m.get("sector"),
                "industry": m.get("industry"), "mcap": m.get("mcap"),
                "inst": inst_stats(led, t, spy_arr),
                "odds_base_26w_pct": cb_odds,
                "bucket": m["bucket"],
                "scope": "sp500" if t in qual else "broad",
                "score": score,
                "legs": {k: round(v, 2) for k, v in legs.items()},
                "n_legs": len(legs),
                "dd_52w_pct": round(dd * 100, 1),
                "ret_8w_pct": round(r8 * 100, 1)
                if r8 is not None else None,
                "ret_6m_pct": round(r6 * 100, 1)
                if r6 is not None else None,
                "why": why[:7]}

    comeback = []
    for t in meta:
        row = cb_row(t)
        if row:
            comeback.append(row)
    comeback.sort(key=lambda r: -r["score"])

    out_buckets = {b: v[:PER_BUCKET] for b, v in
                   {**buckets, **etf_buckets}.items()}
    out_buckets["comeback"] = comeback[:COMEBACK_TOP]
    # ---- AI verdicts (top rows only, weekly cache, anchored) --------
    wk_key = led["dates"][-1] if led["dates"] else "na"
    cache_doc = _g(AI_CACHE_KEY) or {}
    cache = cache_doc.get(wk_key) or {}
    ai_calls = 0
    for bname, rows in out_buckets.items():
        wing = "comeback" if bname == "comeback" else "momentum"
        for r in rows[:AI_TOP_PER_BUCKET]:
            v = ai_verdict(r, wing, cache)
            if v:
                r["ai"] = v
                if v["mode"] == "llm" and r["t"] not in cache:
                    cache[r["t"]] = {k: v[k] for k in
                                     ("stance",
                                      "odds_beat_spx_26w_pct",
                                      "horizon_weeks",
                                      "downside_risk_pct",
                                      "one_liner")}
                    ai_calls += 1
    if ai_calls:
        cache_doc[wk_key] = cache
        for k in list(cache_doc):
            if k != wk_key:
                cache_doc.pop(k)
        _put(AI_CACHE_KEY, cache_doc)
    diag["ai"] = {"mode": "llm" if ANTHROPIC_KEY else "rules",
                  "new_calls": ai_calls, "cached": len(cache)}
    counts = {b: len(v) for b, v in {**buckets, **etf_buckets}.items()}
    counts["comeback"] = len(comeback)

    regime = {
        "risk_gate_sizing": (rg.get("sizing") or rg.get("size")
                             or (rg.get("verdict") or {}).get("sizing")
                             if isinstance(rg.get("verdict"), dict)
                             else rg.get("sizing")),
        "risk_gate_state": rg.get("state") or rg.get("regime"),
        "rotation_regime": (rot.get("regime")
                            or (rot.get("l1") or {}).get("regime")
                            if isinstance(rot.get("l1"), dict)
                            else rot.get("regime")),
        "spx_erp_ttm_pct": ((sp.get("macro_cross") or {})
                            .get("erp_ttm_pct")),
        "spx_rule_of_20": ((sp.get("macro_cross") or {})
                           .get("rule_of_20")),
        "spy_ret_6m_pct": round(spy6 * 100, 1)
        if spy6 is not None else None,
        "spy_ret_12_1_pct": round(spy121 * 100, 1)
        if spy121 is not None else None,
        "note": "macro gates SIZING, not selection (brain); scores are "
                "evidence-weighted odds, not guarantees",
    }
    doc = {"ok": True, "engine": "justhodl-spx-beaters",
           "engine_v": VERSION, "marker": MARKER,
           "as_of": datetime.now(timezone.utc).isoformat(),
           "regime": regime, "ledger": diag["ledger"],
           "mom_status": {"m6": mom_ok_6, "m12_1": mom_ok_121,
                          "note": None if mom_ok_121 else
                          "12-1 momentum unlocks at %d weeks "
                          "(have %d) -- accretes weekly"
                          % (TARGET_WEEKS, weeks)},
           "scanned": {"stocks": scanned, "etfs": len(etf_pool)},
           "counts": counts, "buckets": out_buckets,
           "weights": {"stock": W_STOCK, "etf": W_ETF,
                       "comeback": W_CB},
           "min_score": MIN_SCORE, "comeback_min": COMEBACK_MIN,
           "base_rates": br,
           "diag": diag,
           "method": "score = 100 x SIGMA(w_leg x leg)/SIGMA(w_leg) "
                     "over AVAILABLE legs (>=2 required); momentum = "
                     "cross-sectional percentile of 12-1 (0.6) + 6m "
                     "(0.4) weekly returns vs the full listed "
                     "universe; every leg cites its evidence in why[]."}
    _put(OUT_KEY, doc)
    print("[spx-beaters] %s scanned=%d weeks=%d large=%d mid=%d "
          "small=%d micro=%d etf=%d"
          % (VERSION, scanned, weeks, counts["large"], counts["mid"],
             counts["small"], counts["micro"],
             sum(counts[k] for k in etf_buckets)))
    return {"ok": True, "counts": counts, "weeks": weeks}


def lambda_handler(event, context):
    return scan()
