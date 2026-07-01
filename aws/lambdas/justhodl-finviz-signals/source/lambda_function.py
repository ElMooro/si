"""justhodl-finviz-signals v2.0 — the FinViz TECHNICAL PILLAR (ops 2695).

v1 pulled 33 prebuilt screens into a flat feed consumed by almost nothing.
v2 turns the same Elite exports into a first-class signal family, fused in:

  1. +13 screens closing the called-out gaps: ALL-TIME new high/low, 50-day
     new high/low, price crossed BELOW 50-DMA (v1 only had above), price
     crossed ABOVE/BELOW 20-DMA, horizontal S/R (+strong), trendline
     support/resistance, gap up/down on >=2x rel-vol.
  2. QUARANTINE GUARD — the ops-2693 failure mode (bad filter code silently
     returns the whole ~11.4k universe) can never poison the feed: any screen
     with RAW count >= SUSPICIOUS_N is excluded and reported in `quarantined`.
  3. BOARDS — server-side joins vs the cached 72-field FinViz universe so the
     four dedicated pages (ma-crosses / highs-lows / patterns / consolidation)
     fetch ONE slim feed. Includes the exact requested workflow productized:
     momentum_breakouts (new high ∩ >=1.5x rel-vol) and sweep_opportunities
     (new low ∩ oversold / insider-buying / high-short fuel). Consolidation
     coils are cross-tagged with Wyckoff ACCUMULATION (accumulation-radar) and
     double tops/bottoms are cross-confirmed vs the in-house strict detector
     (chart-patterns) — two independent methodologies agreeing.
  4. BREADTH — whole-market %>20/50/200-DMA (all + S&P500 + per-sector),
     advancer share, NH-NL & ATH-ATL counts, self-appending daily history
     (<=500 sessions). Complements index-level justhodl-breadth-thrust.
  5. top_picks — tiered long/short picks in the harvester's native shape ->
     auto-logged as eng:finviz-signals -> EDGE-ACCURACY grades forward
     excess-vs-SPY from day one. Measure-before-trust.

OUTPUT data/finviz-signals.json (schema 2.0 — strict superset of v1: `signals`,
`counts`, `confluence` keep v1 shapes so insider-radar + finviz-signals.html
keep working untouched).
CONSUMERS best-setups (technical family) · master-ranker (finviz_tech system)
· signal-harvester (top_picks) · 4 dedicated pages · insider-radar.
SCHEDULE 3x/day 14/18/21 UTC — the 21:00 run lands post-close (EDT) so the
breadth-history upsert settles on end-of-day values. Real data only.
"""
import json, time, boto3
from datetime import datetime, timezone
import finviz as FV

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/finviz-signals.json"
s3 = boto3.client("s3", region_name="us-east-1")

SUSPICIOUS_N = 4500   # a real event-screen never returns half the universe
LOOSE_SCREENS = {"horizontal_sr"}  # weak patterns tag thousands legitimately (TLs now use strong variants)
SUSPICIOUS_N_LOOSE = 9500  # only near-whole-universe counts are suspicious for loose screens
SLEEP_S = 3

# (name, query-fragment, cap) — s= prebuilt screens / f= filters. Runtime
# quarantine guard backstops every code against silent-unfiltered returns.
SCREENS = [
    # ── moving-average crosses (MA x MA and price x MA, BOTH directions) ──
    ("golden_cross",   "f=ta_sma50_cross200a", 200),
    ("death_cross",    "f=ta_sma50_cross200b", 200),
    ("sma20_cross50a", "f=ta_sma20_cross50a",  150),
    ("sma20_cross50b", "f=ta_sma20_cross50b",  150),
    ("price_cross200a","f=ta_sma200_pca",      150),
    ("price_cross200b","f=ta_sma200_pcb",      150),
    ("price_cross50a", "f=ta_sma50_pca",       150),
    ("price_cross50b", "f=ta_sma50_pcb",       150),  # v2
    ("price_cross20a", "f=ta_sma20_pca",       150),  # v2
    ("price_cross20b", "f=ta_sma20_pcb",       150),  # v2
    # ── highs / lows across every horizon ──
    ("new_high_alltime","f=ta_alltime_nh",     150),  # v2 — NEW ALL-TIME HIGH
    ("new_low_alltime", "f=ta_alltime_nl",     150),  # v2 — NEW ALL-TIME LOW
    ("new_high_52w",   "s=ta_newhigh",         150),
    ("new_low_52w",    "s=ta_newlow",          150),
    ("new_high_50d",   "f=ta_highlow50d_nh",   150),  # v2 — FILTER not signal (ops 2696)
    ("new_low_50d",    "f=ta_highlow50d_nl",   150),  # v2
    ("new_high_20d",   "f=ta_highlow20d_nh",   150),  # latent v1 bug: was s= (ops 2696)
    ("new_low_20d",    "f=ta_highlow20d_nl",   150),
    # ── volume / momentum / mean-reversion events ──
    ("unusual_volume", "s=ta_unusualvolume",   150),
    ("most_active",    "s=ta_mostactive",      100),
    ("overbought",     "s=ta_overbought",       80),
    ("oversold",       "s=ta_oversold",         80),
    ("top_gainers",    "s=ta_topgainers",      100),
    ("top_losers",     "s=ta_toplosers",       100),
    ("momentum_month", "f=ta_perf_4w20o",      200),
    ("rel_vol_2x",     "f=sh_relvol_o2",       200),
    ("short_high",     "f=sh_short_high",      200),
    ("gap_up_vol",     "f=ta_gap_u,sh_relvol_o2",  120),  # v2
    ("gap_down_vol",   "f=ta_gap_d,sh_relvol_o2",  120),  # v2
    # ── smart money / news ──
    ("insider_buys",   "s=it_latestbuys",      100),
    ("insider_sales",  "s=it_latestsales",     100),
    ("major_news",     "s=n_majornews",         60),
    # ── chart patterns: reversal tops/bottoms ──
    ("double_bottom",  "f=ta_pattern_doublebottom",        120),
    ("double_top",     "f=ta_pattern_doubletop",           120),
    ("inverse_hs",     "f=ta_pattern_headandshouldersinv",  80),
    ("head_shoulders", "f=ta_pattern_headandshoulders",     80),
    ("multiple_bottom","f=ta_pattern_multiplebottom",      120),
    ("multiple_top",   "f=ta_pattern_multipletop",         120),
    # ── chart patterns: continuation / structure ──
    ("channel_up",     "f=ta_pattern_channelup",           150),
    ("channel_down",   "f=ta_pattern_channeldown",         150),
    ("triangle_asc",   "f=ta_pattern_wedgeresistance",     100),
    ("triangle_desc",  "f=ta_pattern_wedgesupport",        100),
    ("wedge_up",       "f=ta_pattern_wedgeup",              80),
    ("wedge_down",     "f=ta_pattern_wedgedown",            80),
    ("horizontal_sr",        "f=ta_pattern_horizontal",       120),  # v2
    ("horizontal_sr_strong", "f=ta_pattern_horizontal2", 120),  # v2 — strong = "2" suffix (ops 2696)
    ("tl_support",     "f=ta_pattern_tlsupport2",          120),  # v2 — STRONG variant; loose base tags ~88% of mkt (ops 2697)
    ("tl_resistance",  "f=ta_pattern_tlresistance2",       120),  # v2 — STRONG variant (ops 2697)
]

_F_MA  = ("price","change_pct","sma20_pct","sma50_pct","sma200_pct","rsi",
          "off_52w_high_pct","rel_volume","perf_m","sector","company","market_cap")
_F_HL  = ("price","change_pct","off_ath_pct","off_52w_high_pct","off_52w_low_pct",
          "rel_volume","perf_m","perf_ytd","short_float_pct","rsi","sector","company","market_cap")
_F_PAT = ("price","change_pct","rsi","rel_volume","perf_m","off_52w_high_pct",
          "sma200_pct","sector","company","market_cap")


def _get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _enrich(rows, uni, fields):
    out = []
    for r in rows:
        tk = r.get("ticker")
        u = uni.get(tk) or {}
        rec = {"ticker": tk}
        for f in fields:
            v = r.get(f)
            if v is None:
                v = u.get(f)
            if v is not None:
                rec[f] = v
        out.append(rec)
    return out


def _walk_pattern_sets(doc):
    """Defensively extract {kind: set(tickers)} from data/chart-patterns.json
    regardless of exact layout — matches on pattern/type text."""
    got = {"double_top": set(), "double_bottom": set()}
    def classify(txt):
        t = (txt or "").upper().replace("-", "_").replace(" ", "_")
        if "DOUBLE_TOP" in t: return "double_top"
        if "DOUBLE_BOTTOM" in t: return "double_bottom"
        return None
    def visit(node, hint=None):
        if isinstance(node, dict):
            tk = node.get("ticker") or node.get("symbol")
            kind = classify(str(node.get("pattern") or node.get("type") or node.get("setup") or hint or ""))
            if tk and kind:
                got[kind].add(str(tk).upper())
            for k, v in node.items():
                visit(v, hint=k)
        elif isinstance(node, list):
            for it in node:
                visit(it, hint=hint)
    visit(doc)
    return got


def _wyckoff_phases(doc):
    """Defensively extract {ticker: PHASE} from data/accumulation-radar.json."""
    phases = {}
    def visit(node):
        if isinstance(node, dict):
            tk = node.get("ticker") or node.get("symbol")
            ph = node.get("phase")
            if tk and isinstance(ph, str):
                phases[str(tk).upper()] = ph.upper()
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for it in node:
                visit(it)
    visit(doc)
    return phases


def _is_stock(u):
    if not u:
        return False
    at = (u.get("asset_type") or "").lower()
    if at and "stock" not in at:
        return False
    if u.get("etf_type") or u.get("expense_ratio") is not None:
        return False
    return True


def lambda_handler(event, context):
    signals, counts, counts_raw, quarantined, tset = {}, {}, {}, {}, {}
    for name, qs, cap in SCREENS:
        thresh = SUSPICIOUS_N_LOOSE if name in LOOSE_SCREENS else SUSPICIOUS_N
        try:
            full = FV.fetch_screen(qs)
            counts_raw[name] = len(full)
            tset[name] = {r.get("ticker") for r in full if r.get("ticker")}
            if len(full) >= thresh:
                quarantined[name] = len(full)
                signals[name], counts[name], tset[name] = [], 0, set()
                print("  %-20s QUARANTINED raw=%d" % (name, len(full)))
            else:
                signals[name] = full[:cap]
                counts[name] = len(signals[name])
                print("  %-20s %d (raw %d)" % (name, counts[name], len(full)))
        except Exception as e:
            print("  %-20s FAIL %s" % (name, str(e)[:70]))
            signals[name], counts[name], counts_raw[name], tset[name] = [], 0, 0, set()
        time.sleep(SLEEP_S)

    uni = FV.load_universe()
    chart_pat = _walk_pattern_sets(_get_json("data/chart-patterns.json"))
    wyckoff = _wyckoff_phases(_get_json("data/accumulation-radar.json"))

    def _tks(name):
        return tset.get(name) or set()   # FULL membership (pre-cap), not the display slice

    # ── confluence: multi-confirmed events (v1 keys preserved, v2 added) ──
    bottoms = _tks("double_bottom") | _tks("inverse_hs") | _tks("multiple_bottom")
    tops    = _tks("double_top") | _tks("head_shoulders") | _tks("multiple_top")
    nl_any  = _tks("new_low_52w") | _tks("new_low_alltime")
    hi_vol  = _tks("rel_vol_2x") | _tks("unusual_volume")
    ath_set = _tks("new_high_alltime")
    atl_set = _tks("new_low_alltime")
    confluence = {
        "bottom_squeeze_insider": sorted(bottoms & _tks("short_high") & _tks("insider_buys")),
        "bottom_insider":  sorted(bottoms & _tks("insider_buys")),
        "bottom_oversold": sorted(bottoms & _tks("oversold")),
        "top_insider_sell": sorted(tops & _tks("insider_sales")),
        "top_overbought":  sorted(tops & _tks("overbought")),
        # v2
        "ath_momentum":     sorted(ath_set & hi_vol),
        "breakout_52w_vol": sorted(_tks("new_high_52w") & hi_vol),
        "ma200_reclaim_vol": sorted(_tks("price_cross200a") & hi_vol),
        "trend_flip_up":    sorted(_tks("golden_cross") | (_tks("price_cross200a") & _tks("price_cross50a"))),
        "base_breakout":    sorted(_tks("horizontal_sr_strong") & (hi_vol | _tks("top_gainers"))),
        "low_sweep":        sorted(nl_any & (_tks("oversold") | _tks("insider_buys") | _tks("short_high"))),
        "bear_break":       sorted(_tks("price_cross200b") & _tks("top_losers")),
    }
    print("  confluence:", {k: len(v) for k, v in confluence.items()})

    # ── boards ──────────────────────────────────────────────────────────
    ma_crosses = {n: _enrich(signals.get(n, []), uni, _F_MA) for n in (
        "golden_cross","death_cross","sma20_cross50a","sma20_cross50b",
        "price_cross200a","price_cross200b","price_cross50a","price_cross50b",
        "price_cross20a","price_cross20b")}

    hl = {n: _enrich(signals.get(n, []), uni, _F_HL) for n in (
        "new_high_alltime","new_low_alltime","new_high_52w","new_low_52w",
        "new_high_50d","new_low_50d","new_high_20d","new_low_20d")}
    mom, seen = [], set()
    for r in _enrich(signals.get("new_high_alltime", []) + signals.get("new_high_52w", []), uni, _F_HL):
        tk = r["ticker"]
        if tk in seen or (r.get("rel_volume") or 0) < 1.5:
            continue
        seen.add(tk)
        r["breakout_grade"] = "ATH" if tk in ath_set else "52W"
        mom.append(r)
    mom = sorted(mom, key=lambda x: -(x.get("rel_volume") or 0))[:60]

    sweeps, seen = [], set()
    ins_b, sh_hi, osold = _tks("insider_buys"), _tks("short_high"), _tks("oversold")
    for r in _enrich(signals.get("new_low_52w", []) + signals.get("new_low_alltime", []), uni, _F_HL):
        tk = r["ticker"]
        if tk in seen:
            continue
        why = []
        if tk in osold or (r.get("rsi") or 99) <= 32: why.append("oversold")
        if tk in ins_b: why.append("insider_buying")
        if tk in sh_hi or (r.get("short_float_pct") or 0) >= 15: why.append("high_short_fuel")
        if why:
            seen.add(tk)
            r["sweep_reasons"] = why
            r["low_grade"] = "ATL" if tk in atl_set else "52W"
            sweeps.append(r)
    sweeps = sorted(sweeps, key=lambda x: -len(x.get("sweep_reasons", [])))[:60]
    highs_lows = dict(hl, momentum_breakouts=mom, sweep_opportunities=sweeps)

    patterns_b = {n: _enrich(signals.get(n, []), uni, _F_PAT) for n in (
        "double_bottom","double_top","inverse_hs","head_shoulders","multiple_bottom",
        "multiple_top","channel_up","channel_down","triangle_asc","triangle_desc",
        "wedge_up","wedge_down","horizontal_sr","horizontal_sr_strong",
        "tl_support","tl_resistance")}
    patterns_b["double_confirmed"] = {
        "double_top": sorted(_tks("double_top") & chart_pat["double_top"]),
        "double_bottom": sorted(_tks("double_bottom") & chart_pat["double_bottom"]),
        "note": "FinViz pattern tag AND in-house strict detector (chart-patterns) agree — two independent methodologies",
    }

    # ── consolidation: volatility-contraction coils near highs ──────────
    hsr, hsr_s, tls = _tks("horizontal_sr"), _tks("horizontal_sr_strong"), _tks("tl_support")
    coils = []
    dg = {"universe": len(uni), "stock": 0, "px_vol": 0, "have_vol": 0, "contract": 0}
    for tk, u in uni.items():
        if not _is_stock(u):
            continue
        dg["stock"] += 1
        price, avol = u.get("price") or u.get("prev_close") or 0, u.get("avg_volume") or 0
        vw, vm = u.get("volatility_w"), u.get("volatility_m")
        offh = u.get("off_52w_high_pct")
        atrp = (u.get("atr") or 0) / price if price else 99
        if price < 3 or avol < 300_000:
            continue
        dg["px_vol"] += 1
        if vw is None or vm is None or offh is None:
            continue
        dg["have_vol"] += 1
        if vm <= 0 or vw > vm * 0.85 or atrp > 0.035 or offh < -15:
            continue
        dg["contract"] += 1
        score = min(40.0, 40 * (1 - vw / vm) / 0.5)     # contraction depth
        score += 30 * max(0.0, 1 - atrp / 0.035)        # absolute tightness
        score += 15 * max(0.0, 1 + offh / 15)           # near 52w high = base, not downtrend
        tags = []
        if tk in hsr_s: score += 12; tags.append("HORIZONTAL_SR_STRONG")
        elif tk in hsr: score += 8; tags.append("HORIZONTAL_SR")
        if tk in tls: score += 6; tags.append("TL_SUPPORT")
        if wyckoff.get(tk) == "ACCUMULATION": score += 15; tags.append("WYCKOFF_ACCUMULATION")
        coils.append({"ticker": tk, "company": u.get("company"), "sector": u.get("sector"),
                      "price": price, "coil_score": round(score, 1),
                      "volatility_w": vw, "volatility_m": vm, "atr_pct": round(atrp * 100, 2),
                      "off_52w_high_pct": offh, "rel_volume": u.get("rel_volume"),
                      "market_cap": u.get("market_cap"), "tags": tags})
    coils = sorted(coils, key=lambda x: -x["coil_score"])[:120]

    dist_watch = []
    top_break = tops | _tks("tl_resistance") | _tks("price_cross200b")
    for tk in sorted(top_break):
        if wyckoff.get(tk) == "DISTRIBUTION":
            u = uni.get(tk) or {}
            why = sorted({n for n in ("double_top","head_shoulders","multiple_top",
                          "tl_resistance","price_cross200b") if tk in _tks(n)})
            dist_watch.append({"ticker": tk, "company": u.get("company"), "sector": u.get("sector"),
                               "price": u.get("price"), "off_52w_high_pct": u.get("off_52w_high_pct"),
                               "rsi": u.get("rsi"), "perf_m": u.get("perf_m"), "why": why})
    print("  coil gates:", dg, "-> coiled", len(coils))
    consolidation = {"coiled": coils, "distribution_watch": dist_watch[:60], "diag": dg,
                     "criteria": "wk-vol <= 0.85x mo-vol · ATR <= 3.5% of price · within 15% of 52w high · >=$3 · >=300k avg vol"}

    # ── whole-market breadth (FinViz cut: per-sector) + daily history ────
    tot = a200 = a50 = a20 = adv = 0
    sp_tot = sp_a200 = 0
    sect = {}
    for tk, u in uni.items():
        if not _is_stock(u) or (u.get("price") or u.get("prev_close") or 0) < 1 or (u.get("avg_volume") or 0) < 50_000:
            continue
        s200 = u.get("sma200_pct")
        if s200 is None:
            continue
        tot += 1
        if s200 > 0: a200 += 1
        if (u.get("sma50_pct") or 0) > 0: a50 += 1
        if (u.get("sma20_pct") or 0) > 0: a20 += 1
        if (u.get("change_pct") or 0) > 0: adv += 1
        if "S&P 500" in (u.get("index_membership") or ""):
            sp_tot += 1
            if s200 > 0: sp_a200 += 1
        sec = u.get("sector")
        if sec:
            st = sect.setdefault(sec, [0, 0])
            st[0] += 1
            if s200 > 0: st[1] += 1
    pct = lambda n, d: round(100.0 * n / d, 1) if d else None
    raw_ok = lambda n: None if n in quarantined else counts_raw.get(n, 0)
    pa200 = pct(a200, tot)
    regime = ("STRONG_BULL" if (pa200 or 0) >= 70 else "BULL" if (pa200 or 0) >= 55
              else "NEUTRAL" if (pa200 or 0) >= 45 else "BEAR" if (pa200 or 0) >= 30 else "WASHOUT")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    nh52, nl52 = raw_ok("new_high_52w"), raw_ok("new_low_52w")
    entry = {"date": today, "pa200": pa200, "pa50": pct(a50, tot), "pa20": pct(a20, tot),
             "adv_pct": pct(adv, tot), "sp500_pa200": pct(sp_a200, sp_tot),
             "nh52": nh52, "nl52": nl52,
             "net_hl_52w": (nh52 - nl52) if (nh52 is not None and nl52 is not None) else None,
             "nh_ath": raw_ok("new_high_alltime"), "nl_atl": raw_ok("new_low_alltime")}
    prior = _get_json(OUT_KEY)
    hist = [h for h in ((prior.get("breadth") or {}).get("history") or []) if h.get("date") != today]
    hist.append(entry)
    breadth = {"universe_n": tot, "pct_above_sma200": pa200, "pct_above_sma50": pct(a50, tot),
               "pct_above_sma20": pct(a20, tot), "sp500_pct_above_sma200": pct(sp_a200, sp_tot),
               "advancers_pct": pct(adv, tot), "regime": regime,
               "counts": {k: entry[k] for k in ("nh52","nl52","net_hl_52w","nh_ath","nl_atl")},
               "sectors": {s: pct(v[1], v[0]) for s, v in sorted(sect.items()) if v[0] >= 15},
               "history": hist[-500:]}

    # ── top_picks -> signal-harvester (eng:finviz-signals), EDGE-graded ──
    picks = {}
    def pick(tk, score, direction, reason):
        cur = picks.get(tk)
        if not tk or (cur and cur["score"] >= score):
            return
        u = uni.get(tk) or {}
        picks[tk] = {"ticker": tk, "score": score, "direction": direction, "reason": reason,
                     "price": u.get("price") or u.get("prev_close"), "sector": u.get("sector")}
    for tk in confluence["bottom_squeeze_insider"]: pick(tk, 92, "long", "double/multiple bottom + high short float + insider buying")
    for tk in confluence["ath_momentum"]:          pick(tk, 88, "long", "new ALL-TIME high on >=2x volume")
    for tk in confluence["base_breakout"]:         pick(tk, 84, "long", "strong horizontal base breaking out on volume")
    for tk in confluence["ma200_reclaim_vol"]:     pick(tk, 78, "long", "reclaimed 200-DMA on elevated volume")
    for tk in sorted(_tks("golden_cross"))[:25]:   pick(tk, 74, "long", "golden cross — 50-DMA over 200-DMA")
    for tk in confluence["breakout_52w_vol"]:      pick(tk, 72, "long", "new 52-week high on elevated volume")
    for tk in confluence["low_sweep"]:             pick(tk, 66, "long", "capitulation low with reversal fuel")
    for tk in confluence["bear_break"]:            pick(tk, 74, "short", "lost 200-DMA + top-loser tape")
    for tk in confluence["top_overbought"]:        pick(tk, 72, "short", "top pattern + overbought RSI")
    for tk in sorted(_tks("death_cross"))[:25]:    pick(tk, 70, "short", "death cross — 50-DMA under 200-DMA")
    top_picks = sorted(picks.values(), key=lambda x: -x["score"])[:45]

    doc = {
        "schema": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "finviz-elite",
        "counts": counts, "counts_raw": counts_raw, "quarantined": quarantined,
        "confluence": confluence,
        "signals": signals,
        "boards": {"ma_crosses": ma_crosses, "highs_lows": highs_lows,
                   "patterns": patterns_b, "consolidation": consolidation},
        "breadth": breadth,
        "top_picks": top_picks,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print("wrote %s | tagged=%d picks=%d quarantined=%d breadth pa200=%s regime=%s"
          % (OUT_KEY, sum(counts.values()), len(top_picks), len(quarantined), pa200, regime))
    return {"ok": True, "counts": counts, "quarantined": quarantined,
            "picks": len(top_picks), "breadth_regime": regime}
