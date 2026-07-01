"""
justhodl-ai-rerating-radar — RE-RATING RADAR (all sectors)
============================================================
Thesis (generalized from the original MU/AI-infra insight): a name re-rates violently
when it has HIGH growth on a LOW multiple relative to what its peers with similar
growth trade at — the market hasn't yet repriced the multiple to match the
fundamentals. That gap between priced-in growth and actual/coming growth is the alpha.

v2.0.0 — generalized beyond the AI-infra cohort to the whole market, grounded in the
institutional literature this rebuild was researched against:
  - Cross-sectional regression of a valuation multiple on growth (+ margin) to find a
    "fair" multiple and rank residuals — CFA-curriculum methodology; Damodaran's own
    published models add operating margin as a second regressor (R² ~41-51% vs growth
    alone) because profitability materially changes what multiple a given growth rate
    deserves. We do the same: EV/Sales ~ growth + operating margin.
  - The regression MUST run within a peer group, not across the whole market — different
    industries structurally trade at different growth-implied multiples (confirmed by
    Seeking Alpha's live ~5,600-stock sector-relative quant system, MSCI's Sector Neutral
    Quality Index methodology, and GuruFocus's industry-relative valuation rank). We
    regress within INDUSTRY (falling back to SECTOR when an industry has too few names
    for a stable fit) instead of one global line.
  - Estimate-revision momentum is the single most validated "fundamental momentum"
    factor in real multi-factor frameworks (Mill Street Research ranks it #1, ahead of
    price momentum and valuation; academic SUE literature — Jegadeesh, Givoly &
    Lakonishok, Stickel — finds up-revision portfolios materially outperforming down-
    revision ones) — kept as the timing trigger that separates re-rating from value trap.
  - The "layer leader / contagion" mechanic generalizes the academic "economic links"
    effect: Cohen & Frazzini (2008, Journal of Finance, "Economic Links and Predictable
    Returns") show a bellwether's returns predict laggards' returns with a lag, driven by
    limited investor attention — >150bps/month alpha, robust to momentum/industry
    controls. We proxy this with same-industry bellwether-revising-up vs laggard-flat,
    generalized from "AI layer leader" to "largest name in the peer group."
  - Growth-at-a-reasonable-price framing follows Jim Slater's Zulu Principle (PEG<0.75,
    EPS growth>15%, ROCE>12%, positive relative strength, low debt) — informs the value-
    trap guard and kicker weighting.
  - A quality/red-flag gate mirrors Seeking Alpha's disqualifying-grade mechanism (auto-
    cap regardless of other strong factors) — Beneish + earnings-quality demote here too.

ARCHITECTURE (two-stage funnel — this is also how real institutional screens work:
cheap bulk pass first, expensive precision only on the shortlist that already looks
interesting):
  STAGE 1  bulk universe — ONE company-screener call tags ~1,700 US names >= $300M
           market cap with sector/industry/marketCap. Free/cheap, real GICS coverage.
  STAGE 2  per-sector shortlist — top N by market cap per sector (every sector gets a
           fair slot count, not cap-weighted domination by Tech) UNIONED with the full
           ai-infra-stack cohort (so small/micro AI names are never dropped for missing
           a market-cap cutoff — the original page's core strength is preserved exactly).
  STAGE 3  cheap per-symbol fetch (financial-growth + key-metrics + ratios = trailing
           revenueGrowth + evToSales + operatingProfitMargin) for the whole shortlist.
  STAGE 4  per-industry (fallback: per-sector) 2-factor regression on the cheap basis;
           z-residual for everyone.
  STAGE 5  for names with an interesting residual (z <= -0.15) OR already in the
           ai-infra-stack cohort, fetch the EXPENSIVE forward-looking analyst-estimates
           growth (the original engine's precision) and refine growth/discount with it.
  STAGE 6  full enrichment reused verbatim from the original engine, now applied broadly:
           revision-velocity, contagion (industry leader), smart-money 13F, insider,
           deal-scanner, short-squeeze, sector ETF flow, small-cap bid, quality gate.

INPUTS  FMP /stable/ company-screener, financial-growth, key-metrics, ratios,
        analyst-estimates; ai-infra-stack.json; revenue-acceleration.json;
        eps-revision-velocity.json; estimate-revisions.json; analyst-consensus.json;
        finra-short.json; deal-scanner.json; smart-money-13f.json; attention-signals.json;
        etf-flows/daily.json; beneish.json; earnings-quality.json
OUTPUT  data/ai-rerating-radar.json   SCHEDULE daily 14:15 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "2.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ai-rerating-radar.json"
LAYER_ETF = {
    "silicon":"XLK","semiconductors":"XLK","memory":"XLK","optics":"XLK","optical":"XLK","networking":"XLK",
    "ai_software":"XLK","software":"XLK","compute":"XLK","neocloud":"XLK","hyperscaler":"XLK","equipment":"XLK",
    "miners_to_ai":"XLK","cooling":"XLI","infrastructure":"XLI","industrial":"XLI","datacenter_buildout":"XLI",
    "power":"XLU","power_grid":"XLU","electrons":"XLU","grid":"XLU","energy":"XLE","foundry":"XLK",
    "datacenter_reits":"XLRE",
}
SECTOR_ETF = {
    "Technology":"XLK","Financial Services":"XLF","Industrials":"XLI","Healthcare":"XLV",
    "Consumer Cyclical":"XLY","Consumer Defensive":"XLP","Utilities":"XLU","Real Estate":"XLRE",
    "Energy":"XLE","Communication Services":"XLC","Basic Materials":"XLB",
}
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name="us-east-1")

CAP_BOOST = {"nano": 18, "micro": 16, "small": 12, "mid": 8, "large": 3, "mega": 0}
SMALL_MID = {"nano", "micro", "small", "mid"}
MIN_FWD_GROWTH = 0.15
PER_SECTOR_N = 55          # shortlist size per GICS sector (Stage 2)
MIN_GROUP_N = 8            # minimum peer-group size for a stable regression (Stage 4)
INTERESTING_Z = -0.15      # cheap-basis threshold to earn the expensive forward-growth fetch


def _num(x):
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _fmp(path, tries=3):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-rerate"}), timeout=15).read()
            return json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503):
                time.sleep(1.0 * (i + 1))
                continue
            return None
        except Exception:
            time.sleep(0.3)
            continue
    return None


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def cap_bucket_of(mc):
    if not mc:
        return ""
    if mc < 300e6: return "nano"
    if mc < 2e9: return "micro"
    if mc < 10e9: return "small"
    if mc < 50e9: return "mid"
    if mc < 200e9: return "large"
    return "mega"


def build_universe():
    """STAGE 1+2: bulk sector/industry tagging, then a fair per-sector shortlist unioned
    with the full ai-infra-stack cohort (never drop a small AI name for cap rank)."""
    stack = _read("data/ai-infra-stack.json")
    ai_uni, layer_median = {}, {}
    by_layer = {}
    for layer in (stack or {}).get("stack", []):
        lk = layer.get("layer")
        for n in layer.get("names", []):
            sym = n.get("symbol")
            if not sym:
                continue
            ai_uni[sym] = {
                "symbol": sym, "name": n.get("name"), "layer": lk,
                "cap_bucket": n.get("cap_bucket") or "", "market_cap": n.get("market_cap"),
                "ret_1m": n.get("ret_1m_pct"), "ret_3m": n.get("ret_3m_pct"),
                "flow_signals": n.get("flow_signals") or [], "bottleneck": bool(n.get("bottleneck")),
            }
            by_layer.setdefault(lk, []).append(n.get("ret_3m_pct"))
    for lk, rets in by_layer.items():
        vals = sorted(r for r in rets if r is not None)
        layer_median[lk] = vals[len(vals) // 2] if vals else 0.0

    screen = _fmp("company-screener?marketCapMoreThan=300000000&limit=3000"
                   "&isActivelyTrading=true&country=US") or []
    by_sector = {}
    tagged = {}
    for r in screen:
        sym = r.get("symbol")
        if not sym or r.get("isEtf") or r.get("isFund") or not r.get("sector"):
            continue
        tagged[sym] = {"sector": r.get("sector"), "industry": r.get("industry"),
                       "market_cap": _num(r.get("marketCap")), "name": r.get("companyName")}
        by_sector.setdefault(r["sector"], []).append(sym)

    uni = {}
    # every AI-infra name is always in, regardless of cap rank
    for sym, rec in ai_uni.items():
        t = tagged.get(sym, {})
        uni[sym] = {**rec, "sector": t.get("sector"), "industry": t.get("industry"),
                    "market_cap": rec.get("market_cap") or t.get("market_cap"), "in_ai_cohort": True}
    # backfill sector/industry for AI names the bulk screener happened to miss (a handful,
    # usually a FMP screener quirk rather than actually being below the cap floor) — cheap,
    # only runs for the exceptions, not the whole cohort
    missing = [s for s, u in uni.items() if not u.get("sector")]
    if missing:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_fmp, f"profile?symbol={urllib.parse.quote(s)}"): s for s in missing}
            for f in as_completed(futs):
                s = futs[f]
                p = f.result()
                if isinstance(p, list) and p:
                    uni[s]["sector"] = p[0].get("sector")
                    uni[s]["industry"] = p[0].get("industry")
                    uni[s]["market_cap"] = uni[s]["market_cap"] or _num(p[0].get("marketCap"))
    # fair per-sector shortlist for everything else
    for sector, syms in by_sector.items():
        ranked = sorted(syms, key=lambda s: tagged[s]["market_cap"] or 0, reverse=True)
        for sym in ranked[:PER_SECTOR_N]:
            if sym in uni:
                continue
            t = tagged[sym]
            uni[sym] = {"symbol": sym, "name": t["name"], "layer": None, "sector": t["sector"],
                        "industry": t["industry"], "market_cap": t["market_cap"],
                        "cap_bucket": cap_bucket_of(t["market_cap"]), "ret_1m": None, "ret_3m": None,
                        "flow_signals": [], "bottleneck": False, "in_ai_cohort": False}
    return uni, layer_median, tagged


def cheap_fundamentals(symbol):
    """STAGE 3: trailing growth + EV/Sales + operating margin, 3 cheap calls."""
    fg = _fmp(f"financial-growth?symbol={urllib.parse.quote(symbol)}&limit=1")
    km = _fmp(f"key-metrics?symbol={urllib.parse.quote(symbol)}&limit=1")
    ra = _fmp(f"ratios?symbol={urllib.parse.quote(symbol)}&limit=1")
    growth = _num((fg or [{}])[0].get("revenueGrowth")) if isinstance(fg, list) and fg else None
    ev_sales = _num((km or [{}])[0].get("evToSales")) if isinstance(km, list) and km else None
    margin = _num((ra or [{}])[0].get("operatingProfitMargin")) if isinstance(ra, list) and ra else None
    if growth is None or ev_sales is None or ev_sales <= 0 or ev_sales > 80:
        return None
    return {"growth": growth, "ev_sales": ev_sales, "margin": margin}


def forward_growth(symbol, latest_rev_hint=None):
    """STAGE 5: expensive forward-looking growth, only fetched for shortlisted candidates —
    the original engine's precision, now reserved for names that already earned it."""
    inc = _fmp(f"income-statement?symbol={urllib.parse.quote(symbol)}&limit=1")
    latest_rev = _num((inc or [{}])[0].get("revenue")) if isinstance(inc, list) and inc else latest_rev_hint
    if not latest_rev or latest_rev <= 0:
        return None
    est = _fmp(f"analyst-estimates?symbol={urllib.parse.quote(symbol)}&limit=10")
    if not (isinstance(est, list) and est):
        return None
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fut = sorted([e for e in est if (e.get("date") or "") > today and _num(e.get("revenueAvg"))],
                key=lambda e: e["date"])
    if not fut:
        return None
    if len(fut) >= 2 and _num(fut[1].get("revenueAvg")):
        return (_num(fut[1]["revenueAvg"]) / latest_rev) ** 0.5 - 1
    return _num(fut[0].get("revenueAvg")) / latest_rev - 1 if _num(fut[0].get("revenueAvg")) else None


def regress2(pts):
    """STAGE 4: 2-factor OLS EV/Sales ~ growth + operating margin (Damodaran-style —
    margin materially changes what multiple a given growth rate deserves)."""
    n = len(pts)
    if n < MIN_GROUP_N:
        return None
    xs1 = [p[1] for p in pts]                      # growth
    xs2 = [p[2] if p[2] is not None else 0.0 for p in pts]   # margin (0 if missing)
    ys = [p[3] for p in pts]                        # ev_sales
    m1, m2, my = sum(xs1)/n, sum(xs2)/n, sum(ys)/n
    c11 = sum((x-m1)**2 for x in xs1) or 1e-9
    c22 = sum((x-m2)**2 for x in xs2) or 1e-9
    c12 = sum((xs1[i]-m1)*(xs2[i]-m2) for i in range(n))
    c1y = sum((xs1[i]-m1)*(ys[i]-my) for i in range(n))
    c2y = sum((xs2[i]-m2)*(ys[i]-my) for i in range(n))
    det = c11*c22 - c12*c12
    if abs(det) < 1e-9:                              # collinear fallback: growth-only
        b1 = c1y / c11
        b2 = 0.0
    else:
        b1 = (c1y*c22 - c2y*c12) / det
        b2 = (c2y*c11 - c1y*c12) / det
    a = my - b1*m1 - b2*m2
    resid = {p[0]: p[3] - (a + b1*p[1] + b2*(p[2] or 0.0)) for p in pts}
    rv = list(resid.values())
    mr = sum(rv)/len(rv)
    sd = (sum((r-mr)**2 for r in rv)/len(rv))**0.5 or 1e-9
    z = {k: (v-mr)/sd for k, v in resid.items()}
    return a, b1, b2, resid, z


def lambda_handler(event, context):
    t0 = time.time()
    uni, layer_median, tagged = build_universe()
    if not uni:
        return {"statusCode": 500, "body": "no universe"}

    ra = _read("data/revenue-acceleration.json") or {}
    accel = {}
    for q in ra.get("all_qualifying", []) or []:
        if isinstance(q, dict) and q.get("symbol"):
            accel[q["symbol"]] = q.get("tier") or "ACCEL"

    erv = {}
    for r in (_read("data/eps-revision-velocity.json") or {}).get("all_qualifying", []) or []:
        est = r.get("estimates", {}) or {}; rb = r.get("ratings_breadth", {}) or {}
        if r.get("symbol"):
            erv[r["symbol"]] = {"vel": _num(r.get("score")), "fy2_lift": _num(est.get("fy2_lift_pct")),
                                "ups": rb.get("n_upgrades") or 0, "downs": rb.get("n_downgrades") or 0}
    revising_up = set(erv.keys())
    for m in (_read("data/estimate-revisions.json") or {}).get("movers_up", []) or []:
        sym = (m.get("symbol") or m.get("ticker")) if isinstance(m, dict) else m
        if sym:
            revising_up.add(sym)
    for r in (_read("data/analyst-consensus.json") or {}).get("strongest_upgrades_30d", []) or []:
        if isinstance(r, dict) and r.get("ticker"):
            revising_up.add(r["ticker"])
    shrt = {}
    for r in (_read("data/finra-short.json") or {}).get("squeeze_candidates", []) or []:
        if isinstance(r, dict) and r.get("symbol"):
            shrt[r["symbol"]] = _num(r.get("squeeze_score"))
    ai_deal_syms = {x.get("symbol") for x in
                    ((_read("data/deal-scanner.json") or {}).get("summary", {}) or {}).get("ai_deals", []) or []
                    if isinstance(x, dict)}
    sm_long = set()
    for _f in (_read("data/smart-money-13f.json") or {}).get("funds", []) or []:
        for _h in _f.get("top_longs", []) or []:
            if _h.get("ticker"):
                sm_long.add(_h["ticker"])
    attn = {}
    for _r in (_read("data/attention-signals.json") or {}).get("tickers", []) or []:
        if _r.get("symbol"):
            attn[_r["symbol"]] = _r
    etf_flow = {}
    for _m in (_read("etf-flows/daily.json") or {}).get("metrics", []) or []:
        if _m.get("ticker") and not _m.get("error"):
            etf_flow[_m["ticker"]] = _m
    iwm_z = (etf_flow.get("IWM") or {}).get("flow_zscore_90d")
    # quality/red-flag gate (mirrors Seeking Alpha's disqualifying-grade mechanism)
    redflags = {}
    for r in (_read("data/beneish.json") or {}).get("red_flags", []) or []:
        if isinstance(r, dict) and r.get("ticker"):
            redflags.setdefault(r["ticker"], []).append("Beneish: possible earnings manipulation")
    for r in (_read("data/earnings-quality.json") or {}).get("top_10_low_quality_avoid", []) or []:
        if isinstance(r, dict) and r.get("ticker"):
            redflags.setdefault(r["ticker"], []).append("low earnings quality")

    # AI-infra layer leaders (preserved exactly)
    leaders, _lb = {}, {}
    for _s, _u in uni.items():
        if not _u.get("layer"):
            continue
        _lk, _mc = _u["layer"], _u.get("market_cap") or 0
        if _lk not in _lb or _mc > _lb[_lk][1]:
            _lb[_lk] = (_s, _mc)
    for _lk, (_ls, _m) in _lb.items():
        leaders[_lk] = _ls
    layer_hot = {lk: (leaders[lk] in revising_up) for lk in leaders}
    # generic industry leaders (the generalized contagion mechanic — every industry, not just AI)
    ind_leaders, _ib = {}, {}
    for _s, _u in uni.items():
        _ik, _mc = _u.get("industry"), _u.get("market_cap") or 0
        if not _ik:
            continue
        if _ik not in _ib or _mc > _ib[_ik][1]:
            _ib[_ik] = (_s, _mc)
    for _ik, (_ls, _m) in _ib.items():
        ind_leaders[_ik] = _ls
    ind_hot = {ik: (ind_leaders[ik] in revising_up) for ik in ind_leaders}

    syms = list(uni.keys())
    print(f"[rerating] universe: {len(syms)} names ({sum(1 for u in uni.values() if u['in_ai_cohort'])} AI-cohort)")

    # STAGE 3: cheap fundamentals for the whole shortlist
    cheap = {}
    with ThreadPoolExecutor(max_workers=14) as ex:
        fut = {ex.submit(cheap_fundamentals, s): s for s in syms}
        for f in as_completed(fut):
            r = f.result()
            if r:
                cheap[fut[f]] = r
    print(f"[rerating] cheap fundamentals: {len(cheap)}/{len(syms)} priced ({time.time()-t0:.0f}s)")

    # STAGE 4: per-industry (fallback per-sector) 2-factor regression on the cheap basis
    groups = {}
    for s in syms:
        u, c = uni[s], cheap.get(s)
        if not c:
            continue
        key = ("industry", u.get("industry")) if u.get("industry") else ("sector", u.get("sector") or "?")
        groups.setdefault(key, []).append((s, c["growth"], c["margin"], c["ev_sales"]))
    # merge thin industries into their sector
    sector_groups = {}
    final_groups = {}
    for (kind, gname), pts in groups.items():
        if len(pts) >= MIN_GROUP_N:
            final_groups[(kind, gname)] = pts
        else:
            sec = None
            for s, *_ in pts:
                sec = uni[s].get("sector")
                if sec:
                    break
            sector_groups.setdefault(sec, []).extend(pts)
    for sec, pts in sector_groups.items():
        final_groups.setdefault(("sector", sec), []).extend(pts)

    z_all, reg_info = {}, {}
    for (kind, gname), pts in final_groups.items():
        reg = regress2(pts)
        if not reg:
            continue
        a, b1, b2, _resid, z = reg
        reg_info[gname] = {"kind": kind, "n": len(pts), "intercept": round(a, 3),
                           "slope_growth": round(b1, 3), "slope_margin": round(b2, 3)}
        z_all.update(z)

    # STAGE 5: expensive forward growth only for names that already look interesting
    need_forward = [s for s in syms if uni[s]["in_ai_cohort"] or (z_all.get(s) is not None and z_all[s] <= INTERESTING_Z)]
    fwd = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        fut = {ex.submit(forward_growth, s): s for s in need_forward}
        for f in as_completed(fut):
            r = f.result()
            if r is not None:
                fwd[fut[f]] = r
    print(f"[rerating] forward growth fetched for {len(fwd)}/{len(need_forward)} shortlisted names ({time.time()-t0:.0f}s)")

    rows = []
    for s in syms:
        u, c = uni[s], cheap.get(s)
        if not c:
            continue
        zz = z_all.get(s)
        if zz is None:
            continue
        tg = c["growth"]
        fg = fwd.get(s, tg)                 # forward if we fetched it, else trailing
        evs = c["ev_sales"]
        group_kind = "industry" if u.get("industry") in reg_info else "sector"
        group_name = u.get("industry") if group_kind == "industry" and u.get("industry") in reg_info else u.get("sector")
        gi = reg_info.get(group_name, {})
        implied = None
        if gi:
            implied = gi["intercept"] + gi["slope_growth"]*fg + gi["slope_margin"]*(c["margin"] or 0.0)
        discount_pct = round((1 - evs/implied)*100, 1) if (implied and implied > 0) else None
        accel_flag = s in accel
        bkt = u["cap_bucket"] or cap_bucket_of(u.get("market_cap"))
        layer_med = layer_median.get(u["layer"]) if u.get("layer") else None
        lag_gap = round((layer_med or 0) - u["ret_3m"], 1) if (layer_med is not None and u["ret_3m"] is not None) else None
        not_decel = accel_flag or (tg is None) or (fg >= 0.7*tg)
        underpriced = zz < -0.2
        ev = erv.get(s, {})
        rising = s in revising_up
        falling = ((ev.get("downs") or 0) > (ev.get("ups") or 0)) and (ev.get("ups") or 0) == 0
        peer_leader = leaders.get(u.get("layer")) if u.get("layer") else ind_leaders.get(u.get("industry"))
        peer_hot = layer_hot.get(u.get("layer")) if u.get("layer") else ind_hot.get(u.get("industry"))
        contagion = bool(peer_hot and (not rising) and fg >= MIN_FWD_GROWTH and underpriced)
        rflags = redflags.get(s, [])
        is_candidate = (fg >= MIN_FWD_GROWTH) and underpriced and not_decel and not falling and not rflags

        unpriced_pts = max(0.0, -zz) * 20
        laggard_pts = max(0.0, min(lag_gap or 0, 60)) * 0.6
        infl_pts = (15 if accel_flag else 0) + max(0.0, min(((fg - (tg or 0))*100), 30))
        accum_pts = min(len(u["flow_signals"])*8, 32)
        bn_pts = 10 if u["bottleneck"] else 0
        cap_pts = CAP_BOOST.get(bkt, 5)
        rev_pts = (min(ev["vel"], 100)*0.30) if ev.get("vel") else (16 if rising else 0)
        if falling:
            rev_pts = -12
        contagion_pts = 24 if contagion else 0
        redflag_pts = -30 if rflags else 0
        sq = (shrt.get(s) or 0) >= 70
        deal = s in ai_deal_syms
        smbk = s in sm_long
        _att = attn.get(s, {})
        ins_buy = (_att.get("insider_mspr") or 0) >= 30
        anl_up = (_att.get("analyst_upgrade_mom") or 0) > 0.03
        _sec_etf = LAYER_ETF.get((u.get("layer") or "").lower()) or SECTOR_ETF.get(u.get("sector"), "SPY")
        _efz = (etf_flow.get(_sec_etf) or {}).get("flow_zscore_90d")
        etf_in = _efz is not None and _efz >= 1.0
        etf_out = _efz is not None and _efz <= -1.5
        smallcap_bid = (iwm_z is not None and iwm_z >= 1.0 and bkt in SMALL_MID)
        kick_pts = (10 if sq else 0) + (12 if deal else 0) + (14 if smbk else 0) + (12 if ins_buy else 0) + (10 if anl_up else 0) + (8 if etf_in else 0) + (-6 if etf_out else 0) + (6 if smallcap_bid else 0)
        composite = round(unpriced_pts + laggard_pts + infl_pts + rev_pts + accum_pts
                          + bn_pts + cap_pts + contagion_pts + kick_pts + redflag_pts, 1)

        why = []
        why.append(f"{fg*100:.0f}% rev growth on {evs:.1f}x EV/Sales"
                   + (f" vs ~{implied:.1f}x growth-implied ({discount_pct:.0f}% below, {group_kind}-relative)" if discount_pct is not None else ""))
        if lag_gap and lag_gap > 0:
            why.append(f"lags {u['layer']} peers by {lag_gap:.0f}pp")
        why.append("revenue accelerating" if accel_flag else ("forward > trailing growth" if (tg is not None and fwd.get(s) and fg > tg) else "growth intact"))
        if u["flow_signals"]:
            why.append("accumulation: " + ", ".join(u["flow_signals"][:2]))
        if rising:
            why.append("estimates revising UP" + (f" (velocity {ev['vel']:.0f})" if ev.get("vel") else ""))
        elif falling:
            why.append("⚠ estimates being cut")
        else:
            why.append("estimates still flat — not yet re-rated")
        if contagion:
            why.append(f"★ contagion: {peer_leader} (peer leader) revising up, this hasn't")
        if rflags:
            why.append("🚩 " + "; ".join(rflags))
        if deal:
            why.append("fresh AI deal")
        if smbk:
            why.append("★ smart money long (13F)")
        if ins_buy:
            why.append("insider buying")
        if anl_up:
            why.append("analyst upgrades accelerating")
        if etf_in:
            why.append(f"sector ETF inflow ({_sec_etf} {_efz:+.1f}z, real $)")
        elif etf_out:
            why.append(f"! sector ETF outflow ({_sec_etf})")
        if smallcap_bid:
            why.append("small-caps catching bids (IWM inflow)")

        rows.append({
            "symbol": s, "name": u["name"], "layer": u.get("layer"), "sector": u.get("sector"),
            "industry": u.get("industry"), "peer_group": group_name, "peer_group_kind": group_kind,
            "in_ai_cohort": u["in_ai_cohort"], "cap_bucket": bkt,
            "market_cap": u["market_cap"], "is_small_mid": bkt in SMALL_MID,
            "growth_pct": round(fg*100, 1), "growth_basis": "forward" if s in fwd else "trailing",
            "trailing_growth_pct": round(tg*100, 1) if tg is not None else None,
            "margin_pct": round(c["margin"]*100, 1) if c["margin"] is not None else None,
            "ev_sales": round(evs, 2), "ev_sales_implied": round(implied, 2) if implied else None,
            "discount_to_implied_pct": discount_pct, "unpriced_z": round(zz, 2),
            "laggard_gap_pp": lag_gap, "ret_3m_pct": u["ret_3m"],
            "etf_sector": _sec_etf, "etf_sector_flow_z": _efz, "smallcap_bid": smallcap_bid,
            "accelerating": accel_flag, "bottleneck": u["bottleneck"],
            "flow_signals": u["flow_signals"], "is_candidate": is_candidate,
            "revision_velocity": round(ev["vel"], 1) if ev.get("vel") else None,
            "estimates_rising": rising, "estimates_falling": falling,
            "red_flags": rflags, "contagion": contagion,
            "peer_leader": peer_leader, "peer_leader_rising": peer_hot,
            "short_squeeze": sq, "ai_deal": deal, "smart_money_backed": smbk,
            "insider_buying": ins_buy, "analyst_upgrading": anl_up,
            "composite": composite, "why": "; ".join(why),
        })

    rows.sort(key=lambda x: x["composite"], reverse=True)
    candidates = [r for r in rows if r["is_candidate"]]
    sector_counts = {}
    for r in rows:
        sector_counts[r["sector"] or "?"] = sector_counts.get(r["sector"] or "?", 0) + 1
    sector_candidate_counts = {}
    for r in candidates:
        sector_candidate_counts[r["sector"] or "?"] = sector_candidate_counts.get(r["sector"] or "?", 0) + 1

    out = {
        "engine": "ai-rerating-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Find names across every sector with growth priced below their peer-group-implied "
                  "multiple — before the market re-rates them. Generalized from the original AI-infra "
                  "insight (the MU setup) to the whole market via industry-relative regression.",
        "methodology": {
            "core": "2-factor OLS of EV/Sales on growth + operating margin, run WITHIN each industry "
                    "(falling back to sector when an industry has <8 priced names) — different industries "
                    "structurally trade at different growth-implied multiples, so the fair line must be "
                    "peer-group-relative, not market-wide (CFA cross-sectional-regression methodology; "
                    "Damodaran's published EV/Sales models add margin as a second regressor for this reason).",
            "two_stage_funnel": "Stage 1-2: bulk-tag ~1,700 US names >=$300M by sector/industry, take a "
                                "fair per-sector shortlist unioned with the full AI-infra cohort. Stage 3-4: "
                                "cheap trailing growth/EV-Sales/margin for the whole shortlist, run the "
                                "peer-group regression. Stage 5: expensive forward-looking analyst-estimate "
                                "growth is fetched only for names that already look interesting on the cheap "
                                "basis (or are in the AI cohort) — same precision as the original engine, "
                                "applied efficiently instead of market-wide.",
            "value_trap_guard": f"candidate requires growth >= {int(MIN_FWD_GROWTH*100)}% AND not decelerating "
                                "AND no red flag — cheap+shrinking or cheap+manipulation-flagged is excluded "
                                "(Jim Slater's Zulu Principle framing: growth at a reasonable price, not just cheap)",
            "quality_gate": "Beneish M-Score manipulation flag or bottom-decile earnings quality caps a name "
                            "out of candidacy regardless of how cheap it looks — mirrors the disqualifying-grade "
                            "mechanism real quant systems (e.g. Seeking Alpha's) use to stop a strong factor "
                            "score elsewhere from masking a serious red flag.",
            "inflection_trigger": "eps-revision-velocity + estimate-revisions + analyst-consensus: rising "
                                  "estimates boost, cuts disqualify. Estimate-revision momentum is the most "
                                  "validated 'fundamental momentum' factor in real multi-factor frameworks.",
            "contagion": "peer-group leader (largest name in the same industry/AI-layer) revising up while "
                        "a peer's estimates are still flat — the economic-links / lead-lag effect (Cohen & "
                        "Frazzini 2008, Journal of Finance): bellwether news predictably diffuses to peers "
                        "with a lag due to limited investor attention.",
        },
        "coverage": {"n_universe": len(syms), "n_ai_cohort": sum(1 for u in uni.values() if u["in_ai_cohort"]),
                     "n_sectors": len(sector_counts), "by_sector": sector_counts,
                     "candidates_by_sector": sector_candidate_counts,
                     "peer_groups": {k: v for k, v in sorted(reg_info.items(), key=lambda x: -x[1]["n"])}},
        "summary": {
            "n_priced": len(rows), "n_candidates": len(candidates),
            "n_small_mid_candidates": sum(1 for r in candidates if r["is_small_mid"]),
            "top_setups": candidates[:25],
            "top_small_mid_setups": [r for r in candidates if r["is_small_mid"]][:15],
            "deepest_discounts": sorted([r for r in candidates if r["discount_to_implied_pct"] is not None],
                                        key=lambda x: x["discount_to_implied_pct"], reverse=True)[:20],
            "contagion_candidates": sorted([r for r in rows if r["contagion"]],
                                           key=lambda x: x["composite"], reverse=True)[:20],
            "rising_and_cheap": [r for r in candidates if r["estimates_rising"]][:20],
            "red_flagged": [r for r in rows if r["red_flags"]][:15],
            "n_contagion": sum(1 for r in rows if r["contagion"]),
            "n_rising": sum(1 for r in rows if r["estimates_rising"]),
        },
        "all_ranked": rows[:400],
        "sources": ["FMP company-screener", "FMP financial-growth", "FMP key-metrics", "FMP ratios",
                   "FMP analyst-estimates", "ai-infra-stack", "revenue-acceleration", "eps-revision-velocity",
                   "estimate-revisions", "analyst-consensus", "finra-short", "deal-scanner", "smart-money-13f",
                   "attention-signals", "etf-flows", "beneish", "earnings-quality"],
        "disclaimer": "Pre-re-rating screen across the whole covered market. Real data, research only — not "
                      "investment advice. Estimates can be wrong and cheap can stay cheap; the guards reduce "
                      "but do not remove value-trap risk. Coverage is ~1,700 US names >= $300M market cap plus "
                      "the full AI-infra cohort — not literally every listed company.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[rerating] priced={len(rows)} candidates={len(candidates)} sectors={len(sector_counts)} "
          f"small_mid={out['summary']['n_small_mid_candidates']} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_priced": len(rows),
            "n_candidates": len(candidates), "n_sectors": len(sector_counts)})}
