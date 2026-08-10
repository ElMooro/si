"""justhodl-impact-graph v1.0 — the impact layer's single source of truth (wo4580).

Nightly. Four jobs, each honest about coverage:

  1. EXPOSURE GRAPH → data/impact/exposure-graph.json
     per-ticker {industry, sector, mcap, adv_usd, shares_out} assembled from
     fleet data first (census matrix, screener, share-flows) + Massive
     grouped daily bars for ADV (20 sessions, whole-market, 20 calls).
     Every field reports its coverage; absent stays absent.

  2. FACTOR HISTORY → data/impact/factor-history.json (append-only)
     today's value of each engine factor (port throughput, freight composite,
     grid MW, dark share median, ETF net flow) + sector-ETF closes. This is
     the empirical substrate betas need — it accrues nightly from real
     payloads; nothing is backfilled from imagination.

  3. BETAS → data/impact/betas.json
     OLS of forward sector-ETF returns on factor changes wherever the
     accrued history has n_obs >= 8 pairs. Below that: status BOOTSTRAPPING
     with per-factor counts. se + r2 carried for CI construction.

  4. CONVERGENCE → data/impact/convergence.json
     trade_impulse  : port-cargo x freight-pulse x import-canary agreement
     flow_convergence: flow-lookthrough x dark-pool x etf-true-flows
     industries/names confirmed by >= 2 independent evidence classes.

OUTPUTS: the three data/impact/* keys above. Schedule 05:10 UTC daily.
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")

GRAPH_KEY = "data/impact/exposure-graph.json"
HIST_KEY = "data/impact/factor-history.json"
BETAS_KEY = "data/impact/betas.json"
CONV_KEY = "data/impact/convergence.json"

MAX_HIST_DAYS = 400
ADV_SESSIONS = 20

# Sector return proxies — SPDR complex, the standard institutional mapping.
SECTOR_ETF = {
    "Technology": "XLK", "Information Technology": "XLK",
    "Financial Services": "XLF", "Financials": "XLF",
    "Healthcare": "XLV", "Health Care": "XLV",
    "Energy": "XLE", "Industrials": "XLI",
    "Consumer Cyclical": "XLY", "Consumer Discretionary": "XLY",
    "Consumer Defensive": "XLP", "Consumer Staples": "XLP",
    "Utilities": "XLU", "Basic Materials": "XLB", "Materials": "XLB",
    "Real Estate": "XLRE", "Communication Services": "XLC",
}
FACTOR_DEFS = {
    "port_throughput_pulse": "port-cargo global 7d-vs-28d pulse (pct)",
    "freight_composite_z": "freight-pulse composite z",
    "grid_executed_mw": "grid-queue national executed-IA MW",
    "dark_share_median": "median dark share of volume across scored names",
    "etf_net_flow_usd": "etf-true-flows net creations minus redemptions (USD)",
}
_KEY = {}


def _get_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _put_json(key, obj, cache="public, max-age=1800"):
    S3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl=cache)


def massive_key():
    if _KEY.get("k") is not None:
        return _KEY["k"]
    k = os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:
            k = SSM.get_parameter(Name="/justhodl/massive-api-key",
                                  WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            k = ""
    _KEY["k"] = k
    return k


def http_json(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-impact-graph/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


# ── 1. exposure graph ────────────────────────────────────────────────────
def build_graph(gaps):
    tickers = {}
    cov = {"industry": 0, "sector": 0, "mcap": 0, "adv_usd": 0, "shares_out": 0}

    # census matrix: the deepest per-name fundamental store (S&P sweep)
    cm = _get_json("data/fundamental-census-matrix.json")
    rows = []
    if isinstance(cm, dict):
        rows = cm.get("rows") or cm.get("matrix") or cm.get("companies") or []
    if isinstance(rows, dict):
        rows = list(rows.values())
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        tk = (r.get("ticker") or r.get("symbol") or "").upper()
        if not tk:
            continue
        d = tickers.setdefault(tk, {})
        for src, dst in (("industry", "industry"), ("sector", "sector"),
                         ("market_cap", "mcap"), ("mcap", "mcap")):
            v = r.get(src)
            if v and not d.get(dst):
                d[dst] = v
    if not rows:
        gaps.append("fundamental-census-matrix rows not found under "
                    "rows/matrix/companies — census layer skipped")

    # screener: breadth (sector at minimum; take industry/mcap/volume if present)
    scr = _get_json("screener/data.json") or {}
    srows = scr.get("data") or scr.get("stocks") or scr.get("rows") or []
    for r in srows if isinstance(srows, list) else []:
        if not isinstance(r, dict):
            continue
        tk = (r.get("ticker") or r.get("symbol") or "").upper()
        if not tk:
            continue
        d = tickers.setdefault(tk, {})
        for src, dst in (("industry", "industry"), ("sector", "sector"),
                         ("marketCap", "mcap"), ("market_cap", "mcap")):
            v = r.get(src)
            if v and not d.get(dst):
                d[dst] = v

    # share-flows: shares outstanding for its universe (fleet data reuse)
    sf = _get_json("data/share-flows.json") or {}
    for tk, r in (sf.get("tickers") or {}).items():
        if isinstance(r, dict) and r.get("shares_outstanding"):
            tickers.setdefault(tk.upper(), {})["shares_out"] = r["shares_outstanding"]
            if r.get("market_cap") and not tickers[tk.upper()].get("mcap"):
                tickers[tk.upper()]["mcap"] = r["market_cap"]

    # ADV from Massive grouped daily bars — dollar volume, 20 sessions
    k = massive_key()
    adv_days = 0
    if k:
        vol = {}
        px = {}
        d = datetime.now(timezone.utc).date()
        tried = 0
        while adv_days < ADV_SESSIONS and tried < ADV_SESSIONS * 2:
            tried += 1
            d -= timedelta(days=1)
            if d.weekday() >= 5:
                continue
            j = http_json("https://api.polygon.io/v2/aggs/grouped/locale/us/"
                          "market/stocks/%s?adjusted=true&apiKey=%s"
                          % (d.isoformat(), k), timeout=40)
            res = (j or {}).get("results") or []
            if not res:
                continue
            adv_days += 1
            for b in res:
                t = (b.get("T") or "").upper()
                v, c = b.get("v"), b.get("c")
                if t and isinstance(v, (int, float)) and isinstance(c, (int, float)):
                    vol[t] = vol.get(t, 0.0) + v * c
                    px[t] = c
        if adv_days:
            for t, dv in vol.items():
                if t in tickers or len(tickers) < 12000:
                    d2 = tickers.setdefault(t, {})
                    d2["adv_usd"] = round(dv / adv_days)
                    if not d2.get("mcap") and d2.get("shares_out") and px.get(t):
                        d2["mcap"] = round(d2["shares_out"] * px[t])
        else:
            gaps.append("Massive grouped bars returned no sessions — adv_usd absent")
    else:
        gaps.append("no massive key — adv_usd absent this run")

    # wo4585 rev-H: incremental industry reuse + bounded FMP backfill
    prev_g = _get_json(GRAPH_KEY) or {}
    reused = 0
    for tk, d0 in (prev_g.get("tickers") or {}).items():
        cur = tickers.get(tk)
        if cur is not None and not cur.get("industry") and d0.get("industry"):
            cur["industry"] = d0["industry"]
            if d0.get("sector") and not cur.get("sector"):
                cur["sector"] = d0["sector"]
            reused += 1
    missing = [t for t, d1 in tickers.items()
               if d1.get("adv_usd") and not d1.get("industry")]
    missing.sort(key=lambda t: -(tickers[t].get("adv_usd") or 0))
    fmp_key = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    filled = 0
    bf_t0 = time.time()
    for t in missing[:300]:
        if time.time() - bf_t0 > 240:   # hard time box — coverage accrues
            break                        # nightly, never blocks the graph
        j2 = http_json("https://financialmodelingprep.com/stable/profile"
                       "?symbol=%s&apikey=%s" % (t, fmp_key), timeout=6)
        row = j2[0] if isinstance(j2, list) and j2 else             (j2 if isinstance(j2, dict) else None)
        if not row:
            continue
        if row.get("industry"):
            tickers[t]["industry"] = row["industry"]
            filled += 1
        if row.get("sector") and not tickers[t].get("sector"):
            tickers[t]["sector"] = row["sector"]
        mc2 = row.get("mktCap") or row.get("marketCap")
        if mc2 and not tickers[t].get("mcap"):
            tickers[t]["mcap"] = mc2
    backfill = {"reused_from_prior_graph": reused,
                "fmp_filled_tonight": filled,
                "top_adv_still_missing": max(0, len(missing) - filled),
                "budget": "300 names / 240s per night — accrues, never blocks",
                "prior_art_note": ("wo4585 audit: no bulk map pre-existed; "
                                   "census matrix (S&P) + screener sector "
                                   "were the ceiling. equity_enrich is the "
                                   "per-ticker on-demand path; this is the "
                                   "bulk nightly complement, same FMP "
                                   "pattern.")}

    for d in tickers.values():
        for f in cov:
            if d.get(f):
                cov[f] += 1

    industries = {}
    for tk, d in tickers.items():
        ind = d.get("industry")
        if not ind:
            continue
        e = industries.setdefault(ind, {"n": 0, "mcap": 0.0, "sector": d.get("sector")})
        e["n"] += 1
        try:
            e["mcap"] += float(d.get("mcap") or 0)
        except Exception:
            pass
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0", "n_tickers": len(tickers),
            "field_coverage": cov, "adv_sessions_used": adv_days,
            "industry_backfill": backfill,
            "tickers": tickers,
            "industries": {k2: {"n": v["n"], "mcap": round(v["mcap"]),
                                "sector": v.get("sector")}
                           for k2, v in industries.items()},
            "sector_etf_proxy": SECTOR_ETF,
            "float_status": ("PENDING_WIRE — free float requires a dedicated "
                             "source; shares_out (dei) is the current best; "
                             "never approximated silently")}


# ── 2. factor history accrual ───────────────────────────────────────────
def todays_factors(gaps):
    f = {}
    pc = _get_json("data/port-cargo.json") or {}
    gp = pc.get("global_pulse")
    if isinstance(gp, dict):
        gp = gp.get("pulse_pct") or gp.get("pct")
    if isinstance(gp, (int, float)):
        f["port_throughput_pulse"] = round(float(gp), 3)
    else:
        gaps.append("port-cargo global_pulse not numeric — factor skipped today")
    fp = _get_json("data/freight-pulse.json") or {}
    comp = fp.get("composite")
    if isinstance(comp, dict):
        comp = comp.get("z") or comp.get("value")
    if isinstance(comp, (int, float)):
        f["freight_composite_z"] = round(float(comp), 3)
    gq = _get_json("data/grid-queue.json") or {}
    nat = gq.get("national") or {}
    mw = nat.get("mw_with_executed_ia")
    if isinstance(mw, (int, float)):
        f["grid_executed_mw"] = round(float(mw))
    dp = _get_json("data/dark-pool.json") or {}
    dsm = dp.get("dark_share_map") or {}
    vals = sorted(v for v in dsm.values() if isinstance(v, (int, float)))
    if vals:
        f["dark_share_median"] = round(vals[len(vals) // 2] * 100, 2)
    et = _get_json("data/etf-true-flows.json") or {}
    tot = 0.0
    seen = False
    for side, sgn in (("inflows", 1), ("outflows", -1)):
        for row in (et.get(side) or [])[:200]:
            v = row.get("flow_usd") or row.get("net_flow_usd") or row.get("flow")
            if isinstance(v, (int, float)):
                tot += sgn * abs(v)
                seen = True
    if seen:
        f["etf_net_flow_usd"] = round(tot)
    return f


def sector_closes(gaps):
    k = massive_key()
    out = {}
    if not k:
        return out
    d = datetime.now(timezone.utc).date()
    for _ in range(6):
        d -= timedelta(days=1)
        if d.weekday() >= 5:
            continue
        j = http_json("https://api.polygon.io/v2/aggs/grouped/locale/us/market/"
                      "stocks/%s?adjusted=true&apiKey=%s" % (d.isoformat(), k),
                      timeout=40)
        res = (j or {}).get("results") or []
        if res:
            want = set(SECTOR_ETF.values())
            for b in res:
                t = (b.get("T") or "").upper()
                if t in want and isinstance(b.get("c"), (int, float)):
                    out[t] = b["c"]
            out["_date"] = d.isoformat()
            return out
    gaps.append("no sector ETF closes found in last 6 sessions")
    return out


def append_history(gaps):
    h = _get_json(HIST_KEY) or {"days": []}
    days = h.get("days") or []
    today = datetime.now(timezone.utc).date().isoformat()
    entry = {"date": today, "factors": todays_factors(gaps),
             "sector_close": sector_closes(gaps)}
    days = [d for d in days if d.get("date") != today]
    days.append(entry)
    days.sort(key=lambda d: d["date"])
    days = days[-MAX_HIST_DAYS:]
    h = {"days": days, "updated": datetime.now(timezone.utc).isoformat(),
         "factor_defs": FACTOR_DEFS,
         "note": ("append-only from live payloads; betas earn their sample "
                  "here — nothing is backfilled")}
    _put_json(HIST_KEY, h, cache="no-cache")
    return h


# ── 3. betas ─────────────────────────────────────────────────────────────
def _ols(x, y):
    n = len(x)
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    if sxx <= 0:
        return None
    b = sum((a - mx) * (c - my) for a, c in zip(x, y)) / sxx
    a0 = my - b * mx
    resid = [c - (a0 + b * a) for a, c in zip(x, y)]
    sse = sum(e * e for e in resid)
    sst = sum((c - my) ** 2 for c in y) or 1e-12
    r2 = 1 - sse / sst
    se = math.sqrt((sse / max(n - 2, 1)) / sxx)
    return b, se, r2


def compute_betas(hist):
    days = hist.get("days") or []
    factors = {}
    counts = {}
    for fac in FACTOR_DEFS:
        # pair factor CHANGE at t with sector ETF forward return t → t+1obs
        xs = {}
        for i in range(1, len(days) - 1):
            d0, d1, d2 = days[i - 1], days[i], days[i + 1]
            f0 = (d0.get("factors") or {}).get(fac)
            f1 = (d1.get("factors") or {}).get(fac)
            if not (isinstance(f0, (int, float)) and isinstance(f1, (int, float))):
                continue
            base = abs(f0) if abs(f0) > 1e-9 else 1.0
            dx = (f1 - f0) / base
            for sec_etf in set(SECTOR_ETF.values()):
                c1 = (d1.get("sector_close") or {}).get(sec_etf)
                c2 = (d2.get("sector_close") or {}).get(sec_etf)
                if isinstance(c1, (int, float)) and isinstance(c2, (int, float)) and c1:
                    xs.setdefault(sec_etf, []).append((dx, (c2 / c1 - 1) * 100))
        store = {}
        for sec_etf, pairs in xs.items():
            counts[fac] = max(counts.get(fac, 0), len(pairs))
            if len(pairs) < 8:
                continue
            res = _ols([p[0] for p in pairs], [p[1] for p in pairs])
            if not res:
                continue
            b, se, r2 = res
            secs = sorted({s for s, e in SECTOR_ETF.items() if e == sec_etf})
            for sname in secs:
                store[sname] = {"beta": round(b, 4), "se": round(se, 4),
                                "r2": round(r2, 3), "n_obs": len(pairs),
                                "unit": "sector_fwd_return_1obs_pct",
                                "proxy": sec_etf,
                                "basis": "OLS fwd sector-ETF return on factor "
                                         "pct-change (accrued history)"}
        if store:
            factors[fac] = store
    status = "LIVE" if factors else "BOOTSTRAPPING"
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "version": "1.0", "status": status,
           "n_history_days": len(days),
           "pairs_by_factor": counts,
           "min_n_obs": 8,
           "betas": factors,
           "note": ("estimated pp consumers must carry ci from se and state "
                    "n_obs — impact_mapper enforces this at construction")}
    _put_json(BETAS_KEY, out)
    return out


# ── 4. convergence boards ───────────────────────────────────────────────
def convergence(graph, gaps):
    # trade impulse — three independent physical reads
    legs = []
    pc = _get_json("data/port-cargo.json") or {}
    gp = pc.get("global_pulse")
    if isinstance(gp, dict):
        gp = gp.get("pulse_pct") or gp.get("pct")
    if isinstance(gp, (int, float)):
        legs.append({"leg": "port_cargo", "value": round(float(gp), 2),
                     "read": "EXPANDING" if gp > 0.5 else
                             "CONTRACTING" if gp < -0.5 else "FLAT",
                     "as_of": pc.get("latest_data_date")})
    fp = _get_json("data/freight-pulse.json") or {}
    v = fp.get("verdict")
    if v:
        legs.append({"leg": "freight_pulse", "value": None, "read": str(v),
                     "as_of": fp.get("generated_at")})
    ic = _get_json("data/import-canary.json") or {}
    st = ic.get("state") or ic.get("verdict") or ic.get("signal")
    if st:
        legs.append({"leg": "import_canary", "value": None, "read": str(st),
                     "as_of": ic.get("generated_at")})
    if not legs:
        gaps.append("trade impulse: no physical legs readable")
    pos = sum(1 for l in legs if any(w in str(l["read"]).upper()
              for w in ("EXPAND", "BOOM", "ACCEL", "GROWTH", "STRONG", "UP")))
    neg = sum(1 for l in legs if any(w in str(l["read"]).upper()
              for w in ("CONTRACT", "SLOW", "WEAK", "RECESS", "DECEL", "DOWN")))
    trade = {"legs": legs, "n_legs": len(legs),
             "state": ("EXPANSION_CONFIRMED" if pos >= 2 and neg == 0 else
                       "CONTRACTION_CONFIRMED" if neg >= 2 and pos == 0 else
                       "MIXED" if legs else "NO_DATA"),
             "rule": ">=2 independent physical legs agreeing, none opposing"}

    # flow convergence / rev-H (wo4585 audit): justhodl-flow-confluence is
    # the fleet's CANONICAL per-name flow fusion (13F + dark pool + ETF
    # lookthrough + short + stealth, alpha-gated). This board is its
    # INDUSTRY lens — roll its per-name postures up through the exposure
    # graph. The local 3-feed self-fusion demotes to fallback when the
    # canonical feed is absent.
    votes = {}
    flow_src = None
    fc = _get_json("data/flow-confluence.json") or {}
    tm = fc.get("ticker_map") or {}
    if tm:
        flow_src = "justhodl-flow-confluence (canonical per-name fusion)"
        for tk, rec in tm.items():
            if not isinstance(rec, dict):
                continue
            post = str(rec.get("posture") or "").upper()
            n_eng = rec.get("n_engines") or 1
            val = float(n_eng)
            if any(w in post for w in ("SELL", "DIST", "OUT", "NEG", "BEAR")):
                val = -val
            elif not any(w in post for w in ("BUY", "ACC", "IN", "POS",
                                             "BULL")):
                continue
            info = (graph.get("tickers") or {}).get(str(tk).upper()) or {}
            ind = info.get("industry")
            if not ind:
                continue
            d = votes.setdefault(ind, {"sources": {}, "names": set()})
            d["sources"].setdefault("flow_confluence", 0.0)
            d["sources"]["flow_confluence"] += val
            for eng in (rec.get("engines") or [])[:6]:
                d["sources"].setdefault(str(eng), 0.0)
                d["sources"][str(eng)] += val / max(n_eng, 1)
            d["names"].add(str(tk).upper())

    def vote(tk, src, val):
        info = (graph.get("tickers") or {}).get(tk) or {}
        ind = info.get("industry")
        if not ind:
            return
        d = votes.setdefault(ind, {"sources": {}, "names": set()})
        d["sources"].setdefault(src, 0.0)
        d["sources"][src] += val
        d["names"].add(tk)

    if not tm:
        flow_src = "local 3-feed FALLBACK (flow-confluence feed absent)"
    fl = _get_json("data/flow-lookthrough.json") if not tm else {}
    fl = fl or {}
    for r in (fl.get("actual_accumulation") or [])[:20]:
        vote((r.get("ticker") or "").upper(), "flow_lookthrough", 1.0)
    for r in (fl.get("actual_distribution") or [])[:20]:
        vote((r.get("ticker") or "").upper(), "flow_lookthrough", -1.0)
    dp = (_get_json("data/dark-pool.json") if not tm else {}) or {}
    for r in (dp.get("high_conviction") or [])[:20]:
        tk = (r.get("ticker") or r.get("symbol") or "").upper() if isinstance(r, dict) else str(r).upper()
        vote(tk, "dark_pool", 1.0)
    # wo4585 audit: justhodl-flow-confluence PRE-EXISTED this board and is
    # the name-level authority (trust-gated cross-read of the same evidence
    # classes). Consume it as a vote source — converge, never duplicate.
    fc = _get_json("data/flow-confluence.json") or {}
    # actual contract (read, not assumed): multi_engine_confluence rows with
    # ticker + posture (ACCUMULATION / DISTRIBUTION / SHORT_SQUEEZE_SETUP...)
    fc_rows = fc.get("multi_engine_confluence") or []
    for r in fc_rows[:40] if isinstance(fc_rows, list) else []:
        if not isinstance(r, dict):
            continue
        tk = (r.get("ticker") or "").upper()
        po = str(r.get("posture") or "").upper()
        sgn = (1.0 if ("ACCUM" in po or "SQUEEZE" in po)
               else -1.0 if "DIST" in po else 0.0)
        if tk and sgn:
            vote(tk, "flow_confluence", sgn)
    et = _get_json("data/etf-true-flows.json") or {}
    cr = et.get("category_rotation") or {}
    rows = []
    for ind, d in votes.items():
        srcs = d["sources"]
        if len(srcs) < 2:
            continue
        score = sum(srcs.values())
        rows.append({"industry": ind, "n_sources": len(srcs),
                     "sources": {k2: round(v2, 1) for k2, v2 in srcs.items()},
                     "score": round(score, 1),
                     "direction": "ACCUMULATION" if score > 0 else "DISTRIBUTION",
                     "names": sorted(d["names"])[:8]})
    rows.sort(key=lambda r: -abs(r["score"]))
    flow = {"rows": rows[:15],
            "source": flow_src,
            "rule": "industry needs >=2 independent flow evidence classes",
            "relationship": ("name-level authority: justhodl-flow-confluence "
                             "(pre-existing, trust-gated) — consumed here as "
                             "a vote source; this board is the INDUSTRY "
                             "rollup, not a second name-level detector"),
            "category_rotation_asof": et.get("generated_at"),
            "note": ("etf-true-flows category_rotation joins at the category "
                     "level: %s" % (list(cr)[:6] if isinstance(cr, dict) else "n/a"))}
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "version": "1.1", "trade_impulse": trade,
           "flow_convergence": flow, "gaps": gaps}
    _put_json(CONV_KEY, out)
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    gaps = []
    graph = build_graph(gaps)
    _put_json(GRAPH_KEY, graph)
    hist = append_history(gaps)
    betas = compute_betas(hist)
    conv = convergence(graph, gaps)
    print("[impact-graph] DONE %.1fs tickers=%d cov=%s hist=%d betas=%s "
          "trade=%s flow_rows=%d gaps=%d"
          % (time.time() - t0, graph["n_tickers"], graph["field_coverage"],
             betas["n_history_days"], betas["status"],
             conv["trade_impulse"]["state"],
             len(conv["flow_convergence"]["rows"]), len(gaps)))
    return {"statusCode": 200, "body": json.dumps({
        "n_tickers": graph["n_tickers"], "coverage": graph["field_coverage"],
        "beta_status": betas["status"], "n_history_days": betas["n_history_days"],
        "trade_impulse": conv["trade_impulse"]["state"], "gaps": gaps[:6]})}
