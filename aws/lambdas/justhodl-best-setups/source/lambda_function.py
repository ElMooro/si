"""justhodl-best-setups — Unified Conviction Engine (per-ticker stock setups)

THE synthesis layer. Fuses every stock signal the platform produces into ONE
conviction score + verdict per ticker, then ranks the highest-conviction names
as "Today's Best Setups."

THE INSTITUTIONAL INSIGHT — CONFLUENCE:
  A name with insider buying + a committee-aligned politician buy + extreme call
  flow + a cascade alert ALL firing is far stronger than any single signal.
  Independent signals agreeing = real conviction. We reward confluence and
  weight each signal by the hit rate the self-improvement loop has LEARNED.

conviction = Σ(signal_strength × learned_weight) × confluence_multiplier
  signal_strength : 0-1 normalized intensity of each signal
  learned_weight  : per-tier hit-rate from cascade-calibration (blended with a
                    prior until the loop matures)
  confluence_mult : 1 + 0.22 × (n_independent_signals − 1), capped

verdict: STRONG BUY / BUY / WATCH (we don't emit AVOID on a buy-signal board;
         net-sell pressure simply suppresses a name).

OUTPUT data/best-setups.json — ranked setups with entry/stop/target + thesis.
Consumed by chart-pro "⚡ Today's Setups" board + Telegram morning push.
SCHEDULE: hourly (after trade-tickets + signals refresh).
"""
import json
import time
from datetime import datetime, timezone
from collections import defaultdict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/best-setups.json"
s3 = boto3.client("s3", region_name="us-east-1")


def read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


# ── Signal priors (institutional judgment; blended with learned hit rates) ──
# Each maps to a self-improvement tier so we can swap in the learned hit rate.
SIGNAL_PRIORS = {
    "POLITICIAN_COMMITTEE": 0.85,   # committee jurisdiction edge — strongest
    "DEEP_VALUE_OVERLAP":   0.84,   # cheap on multiple lenses + catalysts + inflection
    "CAPITAL_FLOW":         0.82,   # institutions + capital accumulating (13F+inst+ETF)
    "SECTOR_CAPITAL_FLOW":  0.72,   # sector ETF-complex capital ACCELERATING in (radar pump-setup)
    "COMPOUNDER":           0.80,   # durable quality growth (ROIC+margin+growth)
    "REVISION_UP":          0.78,   # analyst estimate-revision momentum
    "DISLOCATION":          0.78,   # relative-value buy-the-laggard
    "BUYBACK":              0.74,   # aggressive share repurchase (price support, ↑EPS)
    "CAPEX_ACCEL":          0.70,   # surging capex in a buildout sector (AI/power demand)
    "BOTTLENECK_BOOM":      0.70,   # demand outrunning supply (Census M3 backlog + revenue acceleration)
    "CAPITAL_CYCLE_EARLY":  0.55,   # Druckenmiller: money-losing cyclical cutting capacity (18-24mo)
    "INSIDER_CLUSTER":      0.80,   # multi-insider buying
    "SHORT_SQUEEZE":        0.66,   # FINRA short-volume z-score + squeeze setup
    "FDA_CATALYST":         0.62,   # upcoming PDUFA/AdCom binary event
    "GOV_CONTRACT":         0.58,   # material federal contract award
    "EXECUTIVE_BUY":        0.72,   # executive-branch proximity
    "OPTIONS_EXTREME":      0.70,   # extreme smart-money call flow
    "GAMMA_SQUEEZE":        0.68,   # dealer short-gamma + call-heavy (Massive GEX) — pre-pump fuel
    "CASCADE_ALERT":        0.65,   # theme cascade alert tier
    "CONVERGENCE":          0.60,   # multi-engine convergence
    "POLITICIAN_BUY":       0.55,   # congress buy w/o committee edge
    "OPTIONS_BULLISH":      0.55,
    "EARLY_MOVER":          0.55,
    "EARNINGS_FRESH":       0.52,   # post-earnings drift
    "CASCADE_LAGGARD":      0.50,   # catch-up play
    "RETAIL_HOT":           0.45,   # can be pump/noise
    "RETAIL_VELOCITY":      0.40,
    # ── FinViz technical events (ops 2695) — priors stay modest until the
    #    harvester ledger grades eng:finviz-signals forward excess-vs-SPY ──
    "ATH_BREAKOUT":         0.66,   # new all-time high on >=2x volume
    "BASE_BREAKOUT":        0.64,   # strong horizontal base breaking out on volume
    "GOLDEN_CROSS":         0.60,   # 50-DMA crossing above 200-DMA
    "MA200_RECLAIM":        0.60,   # price reclaiming the 200-DMA on volume
    "DOUBLE_BOTTOM_FV":     0.56,   # FinViz double/multiple bottom + oversold/insider
}


def learned_weights(calibration):
    """Blend priors with per-tier hit rates the self-improvement loop has learned.
    Until a tier has enough scored data, we lean on the prior."""
    weights = dict(SIGNAL_PRIORS)
    if not calibration:
        return weights, "prior-only"
    attr = (calibration.get("feature_attribution_by_tier") or {})
    by_tier = attr.get("by_tier") or {}
    tier_dist = attr.get("tier_distribution") or {}
    blended = []
    # map signal key → calibration tier name
    tier_map = {
        "POLITICIAN_COMMITTEE": "POLITICIAN_COMMITTEE", "POLITICIAN_BUY": "POLITICIAN_BUY",
        "EXECUTIVE_BUY": "EXECUTIVE_BUY", "INSIDER_CLUSTER": "INSIDER_CLUSTER",
        "OPTIONS_EXTREME": "OPTIONS_EXTREME", "OPTIONS_BULLISH": "OPTIONS_BULLISH",
        "CASCADE_ALERT": "ALERT", "CASCADE_LAGGARD": "LAGGARD", "CONVERGENCE": "CONVERGENCE",
        "EARLY_MOVER": "EARLY_MOVER", "RETAIL_HOT": "RETAIL_HOT", "RETAIL_VELOCITY": "RETAIL_VELOCITY",
        # Newer signals — now also self-calibrate from proven forward-return data
        # once their tier accrues enough scored observations.
        "DEEP_VALUE_OVERLAP": "DEEP_VALUE_OVERLAP", "DISLOCATION": "DISLOCATION",
        "COMPOUNDER": "COMPOUNDER", "CAPITAL_FLOW": "CAPITAL_FLOW",
        "REVISION_UP": "REVISION_UP", "SHORT_SQUEEZE": "SHORT_SQUEEZE",
        "FDA_CATALYST": "FDA_CATALYST", "GOV_CONTRACT": "GOV_CONTRACT",
        "BUYBACK": "BUYBACK", "CAPEX_ACCEL": "CAPEX_ACCEL",
    }
    for sig, prior in SIGNAL_PRIORS.items():
        cal_tier = tier_map.get(sig)
        t = by_tier.get(cal_tier) if cal_tier else None
        n = tier_dist.get(cal_tier, 0) if cal_tier else 0
        if t and not t.get("insufficient_data") and n >= 10:
            ranked = t.get("ranked_by_hit_rate_lift") or []
            if ranked:
                hr = (ranked[0].get("top_q_hit_rate") or 0) / 100.0
                # confidence blend by sample size: more data → trust learned more
                conf = min(1.0, n / 100.0)
                w = prior * (1 - conf) + hr * conf
                weights[sig] = round(w, 3)
                blended.append(sig)
    return weights, f"blended:{len(blended)}" if blended else "prior-only"


def normalize(value, lo, hi):
    if value is None:
        return 0.0
    try:
        v = float(value)
    except Exception:
        return 0.0
    if hi == lo:
        return 0.0
    return max(0.0, min(1.0, (v - lo) / (hi - lo)))


# ── Factor-family map for correlation-adjusted confluence ──
# Signals in the SAME family echo one underlying factor, so they must NOT each
# count as an independent confirmation. Cross-family agreement is the real edge.
SIGNAL_FAMILY = {
    "COMPOUNDER": "value_quality", "DEEP_VALUE_OVERLAP": "value_quality", "BUYBACK": "value_quality",
    "CAPEX_ACCEL": "growth",
    "BOTTLENECK_BOOM": "supply_demand", "CAPITAL_CYCLE_EARLY": "supply_demand",
    "REVISION_UP": "revision", "EARNINGS_FRESH": "revision",
    "CAPITAL_FLOW": "flow", "SECTOR_CAPITAL_FLOW": "flow", "CASCADE_ALERT": "flow",
    "CASCADE_LAGGARD": "flow", "CONVERGENCE": "flow", "EARLY_MOVER": "flow",
    "DISLOCATION": "mean_reversion",
    "OPTIONS_BULLISH": "positioning", "OPTIONS_EXTREME": "positioning",
    "GAMMA_SQUEEZE": "positioning", "SHORT_SQUEEZE": "positioning",
    "INSIDER_CLUSTER": "smart_money", "EXECUTIVE_BUY": "smart_money",
    "POLITICIAN_BUY": "political", "POLITICIAN_COMMITTEE": "political",
    "FDA_CATALYST": "catalyst", "GOV_CONTRACT": "catalyst",
    "ATH_BREAKOUT": "technical", "BASE_BREAKOUT": "technical", "GOLDEN_CROSS": "technical",
    "MA200_RECLAIM": "technical", "DOUBLE_BOTTOM_FV": "technical",
}
WITHIN_FAMILY_WEIGHT = 0.25   # an extra same-family signal adds only a quarter-bet

# ── #3: map best-setups signal keys -> rigorously alpha-graded engine signal_types ──
# Only correspondences we're confident in. squeeze_risk is the single ALPHA_PROVEN
# equity-relevant engine, so squeeze signals here inherit its proven lift. Most equity
# tiers self-calibrate via cascade-calibration and aren't independently alpha-graded;
# the 28 chronic ALPHA_NEGATIVE engines are macro signals this equity board does not
# ingest, so the hard-prune rarely fires here by design (it lives in the macro boards).
SIGNAL_TRUST_MAP = {
    "SHORT_SQUEEZE": "squeeze_risk", "GAMMA_SQUEEZE": "squeeze_risk",
}


def effective_bets(signal_keys):
    """Effective number of INDEPENDENT bets: distinct factor families count
    fully; extra signals within an already-represented family are discounted."""
    fams = [SIGNAL_FAMILY.get(k, k) for k in signal_keys]
    n, n_fam = len(fams), len(set(fams))
    n_eff = n_fam + WITHIN_FAMILY_WEIGHT * (n - n_fam)
    return n_eff, n_fam, sorted(set(fams))


def lambda_handler(event, context):
    t0 = time.time()
    cascade = read_json("data/theme-cascade.json") or {}
    options = read_json("data/polygon-options-flow.json") or {}
    insider = read_json("data/insider-clusters.json") or {}
    finra_short = read_json("data/finra-short.json") or {}
    catalysts = read_json("data/catalyst-calendar.json") or {}
    # ops 3145 fusion (additive): earnings dates + squeeze fuel
    _ecal = read_json("data/benzinga-earnings-calendar.json") or {}
    _edates = {}
    for _r in (_ecal.get("upcoming") or _ecal.get("calendar") or
               (_ecal if isinstance(_ecal, list) else [])):
        if isinstance(_r, dict) and _r.get("ticker") and _r.get("date"):
            _edates.setdefault(_r["ticker"].upper(), _r["date"])
    # ops 3171: KHALID'S OWN NOTES — his research is the highest-signal
    # proprietary input on the platform; a setup that contradicts his own
    # written thesis should say so out loud.
    _notes_idx = (read_json("data/notes-index.json") or {}).get("index") or {}
    # ops 3178 FUSION: his own watchlist engines, evidence-weighted.
    # Unproven panels = context only. Proven ones tilt conviction within
    # [0.90, 1.10]. If the feed is missing, everything below is a no-op.
    try:
        import wl_fusion
        _wlf = wl_fusion.load()
        _wl_ctx = wl_fusion.context(_wlf, ("CREDIT", "STRESS", "LIQUIDITY"))
        _wl_mult, _wl_why = wl_fusion.multiplier(_wlf, "CREDIT")
        _wl_m2, _wl_why2 = wl_fusion.multiplier(_wlf, "STRESS")
        _wl_mult = round(_wl_mult * _wl_m2, 3)
        _wl_audit = [x for x in (_wl_why, _wl_why2) if x]
    except Exception as _e:
        _wlf, _wl_ctx, _wl_mult, _wl_audit = {}, None, 1.0, []
        print(f"[best-setups] fusion skipped: {str(_e)[:80]}")
    _sqf = read_json("data/squeeze-fuel.json") or {}
    _sq_idx = {}
    _sq_rows = (_sqf.get("board") or _sqf.get("rows")
                or _sqf.get("items") or [])
    if isinstance(_sq_rows, dict):
        _sq_rows = _sq_rows.get("items") or _sq_rows.get("rows") or []
    for _r in _sq_rows:
        _t = (_r.get("ticker") or "").upper()
        if _t:
            _sq_idx[_t] = {"score": _r.get("score"), "state": _r.get("state")}
    overlap = read_json("data/deep-value-overlap.json") or {}
    political = read_json("data/political-intel.json") or {}
    executive = read_json("data/executive-intel.json") or {}
    retail = read_json("data/retail-sentiment.json") or {}
    preds_doc = read_json("data/predictions-snapshots/latest.json") or {}
    tickets_doc = read_json("data/trade-tickets.json") or {}
    calibration = read_json("data/cascade-calibration.json") or {}
    ai_rationale = read_json("data/trade-tickets-ai-rationale.json") or {}
    pol_ai = read_json("data/political-ai-investigation.json") or {}
    dislocations = read_json("data/dislocations.json") or {}
    opportunities = read_json("data/opportunities.json") or {}
    capital_flow = read_json("data/capital-flow.json") or {}
    capital_flow_radar = read_json("data/capital-flow-radar.json") or {}
    # ── cycle overlay: accumulation-radar phase per ticker (distribution-at-top caution) ──
    accum = read_json("data/accumulation-radar.json") or {}
    cycle_map = {}
    for _book in ("tops", "distributing", "bottoms", "accumulating"):
        b = accum.get(_book) or {}
        for r in (b.get("stocks") or []) + (b.get("etfs") or []):
            cycle_map.setdefault(r.get("ticker"), r)
    # ── red-flag gate: names insiders are dumping / failing Beneish M-score / low earnings quality ──
    _beneish = read_json("data/beneish.json") or {}
    _insider_sell = read_json("data/insider-sell-cluster.json") or {}
    _eq = read_json("data/earnings-quality.json") or {}
    red_flag_map = {}
    for r in (_beneish.get("red_flags") or []):
        red_flag_map.setdefault(r.get("ticker"), []).append("fails Beneish manipulation test")
    for r in (_insider_sell.get("top_clusters") or []):
        red_flag_map.setdefault(r.get("ticker"), []).append("cluster of insider selling")
    for r in (_eq.get("top_10_low_quality_avoid") or []):
        red_flag_map.setdefault(r.get("ticker"), []).append("low earnings quality (accruals)")
    red_flag_map.pop(None, None)
    risk_regime = read_json("data/risk-regime.json") or {}
    finviz_sig = read_json("data/finviz-signals.json") or {}
    chokepoint = read_json("data/chokepoint.json") or {}
    et_doc = read_json("data/engine-trust.json") or {}
    equity_conf = read_json("data/equity-confluence.json") or {}
    resilience_doc = read_json("data/resilience.json") or {}
    strategist_doc = read_json("data/strategist.json") or {}
    options_confl = read_json("data/options-confluence.json") or {}
    flow_confl = read_json("data/flow-confluence.json") or {}
    opt_map = options_confl.get("ticker_map") or {}
    flow_map = flow_confl.get("ticker_map") or {}
    earn_confl = read_json("data/earnings-confluence.json") or {}
    earn_map = {r.get("ticker"): r for r in (earn_confl.get("confluence_book") or []) if r.get("ticker")}
    trust_by = {}
    for e in (et_doc.get("engines") or []):
        st = e.get("signal_type")
        if st:
            trust_by[st] = {"effective_trust": e.get("effective_trust"),
                            "alpha_status": e.get("alpha_status"), "status": e.get("status")}
    kill_theses = read_json("data/kill-theses.json") or {}
    engine_conflicts = read_json("data/engine-conflicts.json") or {}
    lead_lag = read_json("data/lead-lag-graph.json") or {}
    orthogonality = read_json("data/signal-orthogonality.json") or {}
    _screener_doc = read_json("screener/data.json") or {}
    _rr_score = risk_regime.get("risk_regime_score")
    _rr_regime = risk_regime.get("risk_regime") or "NEUTRAL"
    RR_HIGH_BETA = {"Technology", "Consumer Cyclical", "Energy", "Financial Services",
                    "Basic Materials", "Communication Services", "Industrials"}
    RR_DEFENSIVE = {"Utilities", "Consumer Defensive", "Healthcare", "Real Estate"}

    # Build a robust ticker->sector lookup. The per-signal records do NOT carry a
    # sector (add()'s 2nd arg is `name`), so without this the regime gate silently
    # no-ops. Harvest every {ticker/symbol -> sector} pair across the screener output
    # and the already-loaded feeds (defensive recursive walk).
    def _harvest_sectors(*docs):
        m = {}
        def walk(o):
            if isinstance(o, dict):
                t = o.get("ticker") or o.get("symbol") or o.get("t")
                s = o.get("sector") or o.get("sectorName")
                if isinstance(t, str) and isinstance(s, str) and t and s:
                    m.setdefault(t.upper(), s)
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        for d in docs:
            walk(d)
        return m
    _sector_map = _harvest_sectors(_screener_doc, opportunities, dislocations,
                                   overlap, capital_flow_radar)

    def _roro_scalar(sector):
        """Regime gate: don't fight a risk-off tape; lean into risk-on. + on high-beta
        in risk-on, haircut high-beta in risk-off. Returns (mult, note|None)."""
        if not isinstance(_rr_score, (int, float)):
            return 1.0, None
        hb, dfn = sector in RR_HIGH_BETA, sector in RR_DEFENSIVE
        if _rr_score >= 35:
            m = 1.06 if hb else (0.97 if dfn else 1.02)
        elif _rr_score >= 12:
            m = 1.03 if hb else (0.99 if dfn else 1.0)
        elif _rr_score > -12:
            m = 1.0
        elif _rr_score > -35:
            m = 0.90 if hb else (1.03 if dfn else 0.95)
        else:
            m = 0.80 if hb else (1.05 if dfn else 0.88)
        if m == 1.0:
            return 1.0, None
        return m, f"RORO {_rr_regime} {'tailwind' if m > 1 else 'haircut'}"

    # ── INDUSTRY LEADERSHIP prior (Khalid doctrine: regime -> industry
    # ETF leadership -> strongest stocks; Moskowitz-Grinblatt). Each
    # setup's conviction is tilted by its parent sector's leadership
    # score from justhodl-industry-rotation, penalized on BREAKDOWN
    # (broken ladder in a strong tape) and lightly on CROWDED (2y RS
    # extreme rolling over). Bounded, failure-isolated: any problem
    # with the feed leaves conviction untouched. ──
    _FINVIZ_TO_GICS = {
        "Financial": "Financials", "Financial Services": "Financials",
        "Healthcare": "Health Care",
        "Consumer Cyclical": "Consumer Discretionary",
        "Consumer Defensive": "Consumer Staples",
        "Basic Materials": "Materials"}
    _ind_by_sector, _ind_meta, _ind_credit = {}, {}, {}
    try:
        _ir = read_json("data/industry-rotation.json") or {}
        _gen = _ir.get("generated_at") or ""
        _age_h = None
        try:
            _age_h = (datetime.now(timezone.utc)
                      - datetime.fromisoformat(_gen)
                      ).total_seconds() / 3600.0
        except Exception:
            pass
        if _age_h is not None and _age_h <= 60:
            _ind_by_sector = _ir.get("by_sector_name") or {}
            _ind_credit = _ir.get("industry_credit") or {}
            _ind_meta = {"generated_at": _gen,
                         "age_h": round(_age_h, 1),
                         "sectors": len(_ind_by_sector),
                         "market_regime": (_ir.get("market_regime")
                                           or {}).get("state")}
        else:
            _ind_meta = {"note": "industry-rotation feed stale/absent",
                         "age_h": _age_h}
    except Exception as _e:
        _ind_meta = {"note": "industry feed error: %s" % str(_e)[:80]}

    def _industry_prior(sector):
        g = _FINVIZ_TO_GICS.get(sector, sector)
        row = _ind_by_sector.get(g)
        if not row or row.get("leadership_score") is None:
            return 1.0, None, None
        sc = row["leadership_score"]
        m = 0.94 + 0.12 * (sc / 100.0)          # [0.94 .. 1.06]
        tag = row.get("tag")
        if tag == "CONFIRMED_DETERIORATION":
            # price BREAKDOWN + balance-sheet DANGER agreeing --
            # strictly worse than BREAKDOWN alone
            m = max(0.85, m * 0.90)
        elif tag == "BREAKDOWN":
            m = max(0.88, m * 0.93)
        if row.get("crowded"):
            m *= 0.98
        cred = (_ind_credit.get(row.get("etf")) or {}).get("read")
        if cred == "DANGER" and tag != "CONFIRMED_DETERIORATION":
            # many distressed balance sheets inside the industry
            # (Khalid's high-CDS rule) even without a price breakdown
            m *= 0.95
        m = round(max(0.85, min(1.08, m)), 3)
        note = ("INDUSTRY %s score %d%s%s%s"
                % (row.get("etf"), sc,
                   " CONFIRMED_DETERIORATION"
                   if tag == "CONFIRMED_DETERIORATION" else
                   (" BREAKDOWN" if tag == "BREAKDOWN" else
                    (" ABSORPTION" if tag == "ABSORPTION" else "")),
                   " CROWDED" if row.get("crowded") else "",
                   " CREDIT-DANGER" if cred == "DANGER" else ""))
        return m, (note if abs(m - 1.0) > 0.004 else None), row

    # ── FACTOR-REGIME appetite prior (style ratios: factors trend).
    # Deliberately SMALL [0.97..1.03] and only for high-beta sectors,
    # because RORO already regime-gates -- this is the style-ratio
    # layer, additive context not a second regime gate. ──
    _fa_score = None
    try:
        _fr = read_json("data/factor-regime.json") or {}
        _fa_score = _fr.get("risk_appetite_score")
    except Exception:
        pass

    def _factor_appetite_mult(sector):
        if not isinstance(_fa_score, (int, float)):
            return 1.0, None
        if sector not in RR_HIGH_BETA:
            return 1.0, None
        m = 1.0 + max(-60.0, min(60.0, _fa_score)) / 60.0 * 0.03
        m = round(m, 3)
        if abs(m - 1.0) <= 0.005:
            return 1.0, None
        return m, ("FACTOR appetite %+.0f %s"
                   % (_fa_score, "tailwind" if m > 1 else "haircut"))

    # ── Fed nowcast growth×inflation regime gate (GDPNow + underlying inflation) ──
    _nc_desk = read_json("data/nowcast-desk.json") or {}
    _nc_q = _nc_desk.get("nowcast_quadrant") or {}
    _nc_regime = _nc_q.get("regime")
    _nc_quad_growth = _nc_q.get("growth")
    _nc_quad_infl = _nc_q.get("inflation")
    RR_ENERGY_MAT = {"Energy", "Basic Materials", "Materials"}

    def _nowcast_scalar(sector):
        """Fed nowcast regime gate: GOLDILOCKS→high-beta, OVERHEAT/STAGFLATION→energy/
        materials, STAGFLATION/DISINFLATION→defensives & haircut cyclicals. (mult, note|None)."""
        if not _nc_regime or not sector:
            return 1.0, None
        hb, dfn, em = sector in RR_HIGH_BETA, sector in RR_DEFENSIVE, sector in RR_ENERGY_MAT
        r = _nc_regime
        m = 1.0
        if r == "GOLDILOCKS":
            m = 1.05 if hb else (0.98 if dfn else 1.0)
        elif r == "OVERHEAT":
            m = 1.06 if em else (1.02 if hb else (0.99 if dfn else 1.0))
        elif r == "STAGFLATION":
            m = 1.06 if em else (1.04 if dfn else (0.92 if hb else 1.0))
        elif r == "SOFT LANDING":
            m = 1.02 if hb else 1.0
        elif r.startswith("DISINFLATION"):
            m = 0.92 if em else (1.05 if dfn else (0.96 if hb else 1.0))
        m = round(m, 3)
        if m == 1.0:
            return 1.0, None
        return m, f"nowcast {r} {'tailwind' if m > 1 else 'haircut'}"

    bond_vol = read_json("data/bond-vol.json") or {}
    massive = read_json("data/massive-signals.json") or {}
    bottleneck = read_json("data/bottleneck-boom.json") or {}
    # ── The Brain: Khalid's pinned principles + watched tickers. We flag setups
    # that align with what's on his mind so the board surfaces HIS theses. ──
    brain = read_json("data/brain.json") or {}
    brain_directive = brain.get("directive") or {}
    brain_themes = [t.lower() for t in (brain_directive.get("themes") or [])]
    brain_tilts = {k.lower(): v for k, v in (brain_directive.get("sector_tilts") or {}).items()}
    def brain_match(sector, signal_keys=None):
        """KNOWLEDGE alignment only. The brain is a knowledge layer, not a
        watchlist: a setup is flagged because its SECTOR fits the user's
        directive (overweight tilt) or investing themes — never because the
        ticker happens to appear in the notes. Engines never suggest tickers
        from the brain; they apply its frameworks to names found by their own
        analysis."""
        sl = (sector or "").lower()
        for sec, stance in brain_tilts.items():
            if sl and (sl in sec or sec in sl):
                low = stance.lower()
                if "overweight" in low:
                    return f"Fits your overweight on {sector}: {stance[:90]}"
                if "avoid" in low or "underweight" in low:
                    return None  # don't flag setups in sectors you avoid
        for th in brain_themes:
            if sl and (sl in th or any(w in th for w in sl.split())):
                return f"Fits your theme: {th}"
        return None
    bv_regime = (bond_vol.get("regime") or "").upper()

    weights, weight_src = learned_weights(calibration)

    # Accumulate per-ticker signals
    sig = defaultdict(lambda: {"ticker": "", "name": "", "signals": [], "raw": {}})

    def add(ticker, name, key, strength, detail):
        if not ticker:
            return
        ticker = ticker.upper()
        rec = sig[ticker]
        rec["ticker"] = ticker
        if name and not rec["name"]:
            rec["name"] = name
        # de-dup signal types (one entry per signal family)
        if not any(s["key"] == key for s in rec["signals"]):
            rec["signals"].append({"key": key, "strength": round(strength, 3),
                                   "weight": weights.get(key, 0.5), "detail": detail})

    # 0a. FinViz technical events (ops 2695) — whole-market MA crosses, ATH
    #     breakouts, base breaks, confirmed bottoms from justhodl-finviz-signals v2.
    #     Only multi-confirmed confluence sets emit. New "technical" factor family
    #     -> counts as an independent bet in effective_bets().
    _fv_conf = finviz_sig.get("confluence") or {}
    _fv_sigs = finviz_sig.get("signals") or {}
    _fv_name = {}
    for _rows in _fv_sigs.values():
        for _r in _rows or []:
            if _r.get("ticker") and _r.get("company") and _r["ticker"] not in _fv_name:
                _fv_name[_r["ticker"]] = _r["company"]
    for _tk in (_fv_conf.get("ath_momentum") or [])[:40]:
        add(_tk, _fv_name.get(_tk), "ATH_BREAKOUT", 0.90, "new all-time high on >=2x relative volume")
    for _tk in (_fv_conf.get("base_breakout") or [])[:40]:
        add(_tk, _fv_name.get(_tk), "BASE_BREAKOUT", 0.85, "strong horizontal base breaking out on volume")
    for _tk in (_fv_conf.get("ma200_reclaim_vol") or [])[:40]:
        add(_tk, _fv_name.get(_tk), "MA200_RECLAIM", 0.80, "reclaimed its 200-DMA on elevated volume")
    for _tk in sorted({_r.get("ticker") for _r in (_fv_sigs.get("golden_cross") or []) if _r.get("ticker")})[:40]:
        add(_tk, _fv_name.get(_tk), "GOLDEN_CROSS", 0.70, "golden cross — 50-DMA crossed above 200-DMA")
    for _tk in sorted(set(_fv_conf.get("bottom_oversold") or []) | set(_fv_conf.get("bottom_insider") or []))[:40]:
        add(_tk, _fv_name.get(_tk), "DOUBLE_BOTTOM_FV", 0.75, "double/multiple bottom with oversold RSI or insider buying")

    # 0. Gamma squeeze — dealer short-gamma + call-heavy (Massive options/GEX). The matrix gap:
    #    best-setups already had options FLOW but not dealer GAMMA. Dormant until squeezes appear.
    for _sym, _t in ((massive.get("tickers") or {}).items()):
        _gs = _t.get("gamma_squeeze_score")
        if _gs:
            add(_sym, None, "GAMMA_SQUEEZE", normalize(_gs, 50, 95),
                f"dealer gamma squeeze {round(_gs)}" + (f" · {_t.get('massive_why')}" if _t.get("massive_why") else ""))

    # 1. Cascade
    for c in (cascade.get("alert_tier") or []):
        add(c.get("ticker"), c.get("industry_label") or c.get("industry"), "CASCADE_ALERT",
            normalize(c.get("combined_score"), 80, 200),
            f"cascade {round(c.get('combined_score') or 0)}, theme +{round(c.get('theme_acceleration') or 0)}%")
    for c in (cascade.get("laggards_hot_themes") or []):
        add(c.get("ticker"), c.get("industry_label") or c.get("industry"), "CASCADE_LAGGARD",
            normalize(c.get("combined_score"), 60, 160), f"laggard in hot theme {c.get('hot_etf') or ''}")

    # 2. Options flow
    for c in (options.get("extreme_call_flow") or []):
        add(c.get("ticker"), c.get("industry"), "OPTIONS_EXTREME",
            normalize(c.get("cv_pv_ratio"), 2, 8), f"C/P {round(c.get('cv_pv_ratio') or 0,1)}, smart-money {c.get('n_smart_money_blocks') or 0}")
    for c in (options.get("bullish_call_flow") or []):
        add(c.get("ticker"), c.get("industry"), "OPTIONS_BULLISH",
            normalize(c.get("cv_pv_ratio"), 1.5, 5), f"bullish call flow C/P {round(c.get('cv_pv_ratio') or 0,1)}")

    # 3. Insider clusters
    for c in (insider.get("clusters") or insider.get("items") or insider.get("top_clusters") or []):
        nb = c.get("n_insiders") or c.get("cluster_size") or 0
        base = normalize(nb, 2, 8)
        # CEO/CFO open-market buys are a 5–10x stronger signal than other officers.
        # Boost the cluster strength when a top-officer is among the buyers.
        roles = " ".join(str(r) for r in (c.get("roles") or c.get("titles") or [])).upper()
        rolestr = (roles + " " + str(c.get("top_role") or "")).upper()
        has_ceo = any(k in rolestr for k in ("CEO", "CHIEF EXECUTIVE", "PRESIDENT"))
        has_cfo = any(k in rolestr for k in ("CFO", "CHIEF FINANCIAL"))
        role_note = ""
        if has_ceo and has_cfo:
            base = min(1.0, base * 1.5); role_note = " · CEO+CFO buying"
        elif has_ceo:
            base = min(1.0, base * 1.4); role_note = " · CEO buying"
        elif has_cfo:
            base = min(1.0, base * 1.3); role_note = " · CFO buying"
        add(c.get("ticker"), c.get("company_name"), "INSIDER_CLUSTER",
            base, f"{nb} insiders, ${round((c.get('total_value_usd') or 0)/1e6,1)}M{role_note}")

    # 3b. Corporate buybacks (unified justhodl-buyback-engine: net-of-dilution + fresh
    #     authorizations). Feeds the dormant BUYBACK signal type (value_quality family).
    #     Only the GENUINE classes emit — DILUTION_OFFSET (fake buybacks) is excluded.
    buyback_eng = read_json("data/buyback-engine.json") or {}
    _BB_GOOD = {"🚀 FRESH_LARGE_AUTH", "💪 NET_SHRINKER", "🎯 CHEAP_REPURCHASER", "💰 HIGH_SHAREHOLDER_YIELD"}
    for _bt, _b in (buyback_eng.get("tickers") or {}).items():
        _cls = _b.get("class") or ""
        if not (_b.get("high_conviction_pump") or _cls in _BB_GOOD):
            continue
        _bs = float(_b.get("buyback_score") or 0)
        _strength = min(1.0, _bs / 100.0 + (0.12 if _b.get("high_conviction_pump") else 0.0))
        _bits = []
        if _b.get("auth_pct_mcap"):
            _bits.append(f"auth {_b.get('auth_pct_mcap')}% of mcap")
        if _b.get("net_buyback_yield"):
            _bits.append(f"net yield {_b.get('net_buyback_yield')}%")
        if (_b.get("share_count_reduction_yoy") or 0) > 0:
            _bits.append(f"shares -{_b.get('share_count_reduction_yoy')}% YoY")
        add(_bt, _b.get("company"), "BUYBACK", _strength,
            (_cls.split(" ", 1)[-1] if " " in _cls else _cls) + (" · " + ", ".join(_bits) if _bits else ""))

    # 4. Politician (committee-weighted)
    for tk, p in (political.get("by_ticker") or {}).items():
        if (p.get("n_buys") or 0) <= (p.get("n_sells") or 0):
            continue
        if p.get("committee_relevant"):
            add(tk, p.get("asset"), "POLITICIAN_COMMITTEE",
                normalize(p.get("conviction_score"), 30, 200),
                f"{p.get('n_buyers')} buyers · COMMITTEE edge")
        else:
            add(tk, p.get("asset"), "POLITICIAN_BUY",
                normalize(p.get("conviction_score"), 30, 200), f"{p.get('n_buyers')} congress buyers")

    # 5. Executive
    for tk, e in (executive.get("by_ticker") or {}).items():
        if (e.get("n_buys") or 0) <= (e.get("n_sells") or 0):
            continue
        add(tk, e.get("asset"), "EXECUTIVE_BUY",
            normalize(e.get("conviction_score"), 20, 150), f"{e.get('n_buyers')} executive filers")

    # 6. Retail
    for s in (retail.get("biggest_velocity_surges") or []):
        tk = s.get("ticker") or s.get("symbol")
        vel = s.get("velocity_pct") or 0
        key = "RETAIL_HOT" if vel >= 500 else "RETAIL_VELOCITY"
        add(tk, "", key, normalize(vel, 200, 2000), f"+{round(vel)}% mention velocity")

    # 7b. Dislocation (relative-value buy-the-laggard)
    for d in (dislocations.get("buy_the_laggard") or [])[:40]:
        tk = d.get("ticker")
        vs = (d.get("dislocated_vs") or {}).get("ticker")
        detail = f"cheap vs cohort, score {d.get('dislocation_score')}"
        if vs: detail += f" · dislocated vs {vs}"
        add(tk, d.get("industry"), "DISLOCATION",
            normalize(d.get("dislocation_score"), 60, 95), detail)

    # 7c. Compounders + estimate-revision momentum (from opportunity-engine)
    opp_rows = opportunities.get("all") or opportunities.get("top_opportunities") or []
    # top compounders
    comps = sorted([r for r in opp_rows if (r.get("compounder_score") or 0) >= 70],
                   key=lambda r: -(r.get("compounder_score") or 0))[:40]
    for r in comps:
        gi = r.get("growth_intel") or {}
        eg = gi.get("expected_company_growth_pct")
        add(r.get("ticker"), r.get("sector"), "COMPOUNDER",
            normalize(r.get("compounder_score"), 70, 100),
            f"compounder {r.get('compounder_score')}" + (f", {eg}% exp growth" if eg is not None else ""))

    # ── BUYBACK — aggressive share repurchase (price support + EPS lift) ──
    for r in opp_rows:
        gi = r.get("growth_intel") or {}
        bby = gi.get("buyback_yield_pct")
        if bby is not None and bby >= 4:   # strong/aggressive only
            add(r.get("ticker"), r.get("sector"), "BUYBACK",
                normalize(bby, 4, 12),
                f"{bby}% buyback yield ({gi.get('buyback_signal','strong')})")

    # ── CAPEX_ACCEL — surging capex in a buildout sector (AI/power demand) ──
    for r in opp_rows:
        gi = r.get("growth_intel") or {}
        csig = gi.get("capex_signal") or ""
        cgr = gi.get("capex_growth_pct")
        if cgr is not None and cgr >= 20 and ("buildout sector" in csig or "surging" in csig):
            add(r.get("ticker"), r.get("sector"), "CAPEX_ACCEL",
                normalize(cgr, 20, 80),
                f"capex +{cgr}% — {csig}")
    # estimate-revision UP (the alpha factor)
    for r in opp_rows:
        rev = r.get("estimate_revision") or {}
        if rev.get("direction") == "UP" and (rev.get("delta_pp") or 0) >= 1.0:
            add(r.get("ticker"), r.get("sector"), "REVISION_UP",
                normalize(rev.get("delta_pp"), 1, 8),
                f"analyst estimates revised +{rev.get('delta_pp')}pp")

    # 7d. Capital flow — institutions + capital accumulating (13F + inst QoQ + ETF)
    for c in (capital_flow.get("accumulating") or [])[:40]:
        add(c.get("ticker"), c.get("sector"), "CAPITAL_FLOW",
            normalize(c.get("flow_score"), 8, 60),
            "institutions accumulating · " + " · ".join(c.get("lenses") or []))

    # 7d2. Sector capital-flow radar — stocks riding an ACCELERATING sector ETF-complex inflow
    #      (real ETF Global creations/redemptions; the pump-setup window before the sector runs).
    for cx in (capital_flow_radar.get("pump_setups") or []):
        pp = cx.get("pump_probability")
        cxn = cx.get("complex")
        for s in (cx.get("ref_stocks") or []):
            add(s, None, "SECTOR_CAPITAL_FLOW", normalize(pp, 65, 100),
                "sector capital ACCELERATING in · " + (cxn or "") + " (pump " + str(pp) + ")")

    # 7f. Supply-bottleneck — demand outrunning supply (boom) + Druckenmiller early capital-cycle
    _btop = set(bottleneck.get("top_calls") or [])
    for r in (bottleneck.get("ranks") or [])[:40]:
        bsc = r.get("boom_score")
        if (r.get("ticker") in _btop) or (bsc is not None and bsc >= 55):
            add(r.get("ticker"), r.get("sector"), "BOTTLENECK_BOOM", normalize(bsc or 60, 52, 88),
                f"demand>supply · boom {bsc} · {r.get('pressure_group')} pressure {r.get('group_pressure')}")
    for c in (bottleneck.get("early_bottleneck_calls") or []):
        if c.get("phase") == "SCARCITY_BUILDING" and c.get("money_losing"):
            add(c.get("ticker"), c.get("group"), "CAPITAL_CYCLE_EARLY",
                normalize(c.get("consensus_gap_score") or c.get("score"), 60, 100),
                f"capital cycle: supply exiting · capex {c.get('capex_yoy_pct')}% · "
                f"capex/D&A {c.get('capex_to_da')} · Street fwd {c.get('consensus_fwd_growth_pct')}%")

    # 7e. FINRA short-squeeze setups (elevated short-volume z-score + price strength)
    for r in (finra_short.get("squeeze_candidates") or [])[:25]:
        add(r.get("ticker"), None, "SHORT_SQUEEZE",
            normalize(r.get("squeeze_score"), 50, 95),
            f"squeeze setup {r.get('squeeze_score')}" + (f" · short z {r.get('z_score')}" if r.get('z_score') is not None else ""))

    # 7g. Deep Value + Catalyst Overlap — the master-board prime setups
    for r in (overlap.get("prime_setups") or [])[:30]:
        det = f"{r.get('n_value_lenses')} value lenses + {r.get('n_catalysts')} catalysts"
        if r.get("n_inflection"): det += f" + {r.get('n_inflection')} inflection"
        add(r.get("ticker"), r.get("sector"), "DEEP_VALUE_OVERLAP",
            normalize(r.get("overlap_score"), 40, 90), det)

    # 7f. Catalyst calendar — FDA PDUFA + government contract awards (events[] schema)
    for ev in (catalysts.get("events") or []):
        et = ev.get("type"); tk = ev.get("ticker"); dt = ev.get("date")
        if not tk:
            continue
        if et == "FDA":
            add(tk, None, "FDA_CATALYST", 0.7, f"FDA {ev.get('title','event')} {dt or ''}")
        elif et == "GOV_CONTRACT":
            add(tk, None, "GOV_CONTRACT", 0.6, ev.get("title") or "federal award")

    # 7. Earnings / predictions extras
    for p in (preds_doc.get("predictions") or []):
        alerts = p.get("alerts") or []
        if "EARNINGS_FRESH" in alerts:
            add(p.get("ticker"), p.get("industry"), "EARNINGS_FRESH",
                normalize((p.get("features") or {}).get("earnings_score"), 0, 1), "fresh earnings")
        if any(a.startswith("CONVERGENCE_") for a in alerts):
            add(p.get("ticker"), p.get("industry"), "CONVERGENCE",
                normalize((p.get("features") or {}).get("convergence_score"), 0, 100), "multi-engine convergence")
        if "EARLY_MOVER_ALERT" in alerts:
            add(p.get("ticker"), p.get("industry"), "EARLY_MOVER",
                normalize((p.get("features") or {}).get("early_score"), 0, 100), "early mover")

    # Trade tickets (levels) + thesis
    tickets = {t.get("ticker"): t for t in (tickets_doc.get("tickets") or []) if not t.get("error")}
    rat = ai_rationale.get("by_ticker") or {}
    polai = pol_ai.get("by_ticker") or {}

    # ── REGIME / SECTOR / CAP-AWARE WEIGHTING + TIME DECAY ──
    # A signal's edge is not stationary. Modulate each signal weight by the
    # current bond-vol regime, the sector, the market-cap bucket, and time decay.
    REGIME_MULT = {
        "CRISIS":      {"COMPOUNDER":1.20,"CAPITAL_FLOW":1.15,"INSIDER_CLUSTER":1.10,"DISLOCATION":0.80,"REVISION_UP":0.85,"RETAIL_HOT":0.6,"OPTIONS_EXTREME":0.9},
        "ELEVATED":    {"COMPOUNDER":1.10,"CAPITAL_FLOW":1.08,"DISLOCATION":0.90,"RETAIL_HOT":0.8},
        "NORMAL":      {},
        "BOND_VOL_LOW":{"DISLOCATION":1.10,"RETAIL_HOT":1.10,"OPTIONS_EXTREME":1.08,"COMPOUNDER":0.97},
    }
    SECTOR_MULT = {
        ("INSIDER_CLUSTER","financ"):1.20,("INSIDER_CLUSTER","bank"):1.20,
        ("INSIDER_CLUSTER","health"):0.85,("INSIDER_CLUSTER","biotech"):0.80,
        ("CAPITAL_FLOW","technology"):1.10,("DISLOCATION","energy"):1.10,
        ("DISLOCATION","industrial"):1.08,("COMPOUNDER","technology"):1.08,
    }
    CAP_MULT = {
        ("RETAIL_HOT","micro"):1.25,("RETAIL_HOT","nano"):1.30,("RETAIL_HOT","mega"):0.7,
        ("CAPITAL_FLOW","mega"):1.12,("CAPITAL_FLOW","large"):1.08,("CAPITAL_FLOW","micro"):0.85,
        ("OPTIONS_EXTREME","micro"):1.15,("DISLOCATION","small"):1.06,
    }
    opp_cap = {}
    for r in (opportunities.get("all") or []):
        opp_cap[r.get("ticker")] = r.get("cap_bucket")
    def context_weight(sig_key, base_w, sector, cap, age_days):
        w = base_w
        w *= REGIME_MULT.get(bv_regime, {}).get(sig_key, 1.0)
        sec = (sector or "").lower()
        for (k, frag), m in SECTOR_MULT.items():
            if k == sig_key and frag in sec:
                w *= m; break
        if cap:
            w *= CAP_MULT.get((sig_key, cap), 1.0)
        if age_days is not None and age_days > 0:
            w *= 0.5 ** (age_days / 10.0)
        return w

    # ── Fuse ──
    setups = []
    pruned_signals = []   # (#3) dropped: engine is chronically ALPHA_NEGATIVE
    lifted_signals = []   # (#3) lifted: engine is ALPHA_PROVEN
    for tk, rec in sig.items():
        signals = rec["signals"]
        if not signals:
            continue
        # ── #3: fold proven-alpha trust into weights; hard-prune chronic losers ──
        kept = []
        for s in signals:
            mapped = SIGNAL_TRUST_MAP.get(s["key"])
            tinfo = trust_by.get(mapped) if mapped else None
            if tinfo:
                if tinfo.get("alpha_status") == "ALPHA_NEGATIVE":
                    pruned_signals.append({"ticker": tk, "signal": s["key"], "engine": mapped})
                    continue
                et = tinfo.get("effective_trust")
                if isinstance(et, (int, float)):
                    mult = max(0.5, min(1.35, et))
                    s["weight"] = round(s["weight"] * mult, 3)
                    s["alpha_trust"] = {"engine": mapped, "status": tinfo.get("alpha_status"),
                                        "effective_trust": et, "weight_mult": round(mult, 3)}
                    if tinfo.get("alpha_status") == "ALPHA_PROVEN":
                        lifted_signals.append({"ticker": tk, "signal": s["key"], "engine": mapped, "mult": round(mult, 3)})
            kept.append(s)
        signals = kept
        rec["signals"] = kept
        if not signals:
            continue
        n = len(signals)
        # ── correlation-adjusted confluence: reward INDEPENDENT bets, not echoes ──
        n_eff, n_fam, fam_list = effective_bets([s["key"] for s in signals])
        confluence = 1.0 + 0.26 * (n_eff - 1.0)
        confluence = min(confluence, 2.2)
        sector = rec.get("sector") or rec.get("industry")
        cap = opp_cap.get(tk)
        raw = 0.0
        for s in signals:
            cw = context_weight(s["key"], s["weight"], sector, cap, s.get("age_days"))
            s["context_weight"] = round(cw, 3)
            raw += s["strength"] * cw
        composite = round(min(100.0, raw * confluence * 22), 1)

        # ── RORO regime gate (sector-aware): scale conviction by risk-on/off tape ──
        _eff_sector = _sector_map.get(tk) or sector
        _rr_mult, _rr_note = _roro_scalar(_eff_sector)
        if _rr_mult != 1.0:
            composite = round(min(100.0, composite * _rr_mult), 1)

        # ── Fed nowcast regime gate (growth×inflation): tilt by macro backdrop ──
        _nc_mult, _nc_note = _nowcast_scalar(_eff_sector)
        if _nc_mult != 1.0:
            composite = round(min(100.0, composite * _nc_mult), 1)

        # ── Industry-leadership + factor-appetite priors (theory stack) ──
        _ind_mult, _ind_note, _ind_row = _industry_prior(_eff_sector)
        if _ind_mult != 1.0:
            composite = round(min(100.0, composite * _ind_mult), 1)
        _fa_mult, _fa_note = _factor_appetite_mult(_eff_sector)
        if _fa_mult != 1.0:
            composite = round(min(100.0, composite * _fa_mult), 1)

        # ── CYCLE gate (accumulation-radar): a buy signal on a name distributing at a
        # top is lower quality. Tag the phase; gently haircut only the strongest tell
        # (LIKELY_TOP + bearish OBV divergence). Confirmation only, proven math otherwise intact. ──
        _cyc = cycle_map.get(tk)
        _cyc_warning = None
        if _cyc:
            if _cyc.get("flag") == "LIKELY_TOP" or _cyc.get("phase") == "DISTRIBUTION":
                _cyc_warning = "distribution_at_top"
                if _cyc.get("flag") == "LIKELY_TOP" and _cyc.get("divergence") == "bearish":
                    composite = round(composite * 0.93, 1)
                    _cyc_warning = "likely_top_bearish_divergence"

        # ── RED-FLAG gate: a BUY board should not surface names being dumped by insiders,
        # flagged for accounting manipulation, or with poor earnings quality. Haircut + tag. ──
        _rf = red_flag_map.get(tk)
        if _rf:
            composite = round(composite * 0.80, 1)

        # Verdict from composite + confluence. STRONG BUY now requires genuine
        # cross-family independence (>= 2.5 effective bets), so three echoes of
        # one factor can no longer manufacture top conviction.
        if composite >= 55 and n >= 3 and n_eff >= 2.5:
            verdict = "STRONG BUY"
        elif composite >= 35 and n >= 2 and n_eff >= 1.75:
            verdict = "BUY"
        elif composite >= 18:
            verdict = "WATCH"
        else:
            verdict = "WATCH"

        tt = tickets.get(tk) or {}
        thesis = ""
        if polai.get(tk) and polai[tk].get("thesis"):
            thesis = polai[tk]["thesis"][:280]
        elif rat.get(tk) and rat[tk].get("rationale"):
            thesis = rat[tk]["rationale"][:280]

        # ── CREATIVE: "Triple Threat" — the rarest, highest-conviction setup.
        # A name that is simultaneously CHEAP (dislocation), a durable GROWER
        # (compounder), AND carrying a market/flow signal. Three independent
        # value lenses agreeing is the strongest possible confluence. ──
        keys = set(s["key"] for s in signals)
        value_signals = keys & {"DISLOCATION", "COMPOUNDER", "REVISION_UP", "BUYBACK", "CAPEX_ACCEL", "DEEP_VALUE_OVERLAP"}
        flow_signals = keys & {"INSIDER_CLUSTER", "OPTIONS_EXTREME", "OPTIONS_BULLISH",
                                "POLITICIAN_COMMITTEE", "POLITICIAN_BUY", "EXECUTIVE_BUY",
                                "CASCADE_ALERT", "RETAIL_HOT", "CAPITAL_FLOW"}
        has_capital = "CAPITAL_FLOW" in keys
        # cheap = any relative-value lens; grower = durable/growth or capex buildout
        cheap = bool(keys & {"DISLOCATION", "DEEP_VALUE_OVERLAP"})
        grower = bool(keys & {"COMPOUNDER", "CAPEX_ACCEL", "REVISION_UP"})
        triple_threat = (cheap and grower and len(flow_signals) >= 1)
        quad_threat = triple_threat and has_capital and len(flow_signals) >= 2
        # ── BUILDOUT THREAT: the specific 'cheap + buying back stock + AI/power
        # capex surging' stack — self-funded compounding into a demand buildout. ──
        buildout_threat = (cheap and "BUYBACK" in keys and "CAPEX_ACCEL" in keys)
        if quad_threat:
            verdict = "QUAD THREAT"
            composite = min(100.0, composite * 1.30)
        elif triple_threat:
            verdict = "TRIPLE THREAT"
            composite = min(100.0, composite * 1.15)
        elif buildout_threat:
            verdict = "BUILDOUT THREAT"
            composite = min(100.0, composite * 1.18)

        # ── Universal explainability: a plain-language "why" chain ──
        WHY_PHRASES = {
            "DISLOCATION": "trading cheap vs its peers",
            "COMPOUNDER": "a durable quality grower (high ROIC + margins + growth)",
            "REVISION_UP": "analysts revising estimates up",
            "BUYBACK": "aggressively buying back its own stock (price support + EPS lift)",
            "CAPEX_ACCEL": "ramping capex into an AI/power buildout (self-funding demand)",
            "CAPITAL_FLOW": "institutions accumulating it",
            "POLITICIAN_COMMITTEE": "bought by a politician on a relevant committee",
            "POLITICIAN_BUY": "recently bought by members of Congress",
            "EXECUTIVE_BUY": "bought by an executive-branch official",
            "INSIDER_CLUSTER": "a cluster of insiders buying",
            "OPTIONS_EXTREME": "extreme bullish options flow",
            "OPTIONS_BULLISH": "bullish options positioning",
            "CASCADE_ALERT": "a theme/cascade signal firing",
            "RETAIL_HOT": "surging retail attention",
            "EARNINGS_FRESH": "a fresh post-earnings catalyst",
            "CONVERGENCE": "multiple models converging",
            "ATH_BREAKOUT": "breaking to new all-time highs on heavy volume",
            "BASE_BREAKOUT": "breaking out of a strong horizontal base",
            "GOLDEN_CROSS": "a fresh golden cross (50>200-DMA)",
            "MA200_RECLAIM": "reclaiming its 200-day moving average",
            "DOUBLE_BOTTOM_FV": "a confirmed double-bottom reversal",
        }
        ranked_sigs = sorted(signals, key=lambda s: -s["strength"] * s["weight"])
        why_parts = []
        for sg in ranked_sigs[:4]:
            phrase = WHY_PHRASES.get(sg["key"])
            if phrase:
                det = sg.get("detail")
                why_parts.append(phrase + (f" ({det})" if det and len(str(det)) < 60 else ""))
        if why_parts:
            lead = f"{tk} screens as a {verdict.lower()} because it's " if verdict not in ("WATCH",) else f"{tk} is on watch — it's "
            why_text = lead + "; ".join(why_parts) + "."
            if n >= 3:
                why_text += f" {n} independent signals agree, which is the strongest form of confluence."
            if bv_regime in ("ELEVATED", "CRISIS"):
                why_text += f" Note: bond-vol regime is {bv_regime} — size accordingly."
            if _rr_note:
                why_text += f" Cross-asset RORO is {_rr_regime} — {_rr_note.split('RORO ')[-1]} applied."
            if _ind_note:
                why_text += f" {_ind_note}: its industry group is "
                why_text += ("breaking down with distressed balance "
                             "sheets — strong haircut."
                             if "CONFIRMED_DETERIORATION" in _ind_note
                             else "breaking down — conviction haircut."
                             if "BREAKDOWN" in _ind_note else
                             ("a leadership group — conviction boost."
                              if _ind_mult > 1.0 else
                              "lagging — mild haircut."))
            if _fa_note:
                why_text += f" {_fa_note}."
            if _nc_note:
                why_text += f" Fed nowcast regime is {_nc_regime} ({_nc_quad_growth}/{_nc_quad_infl}) — {_nc_note.split('nowcast ')[-1]} applied."
            if _cyc_warning:
                why_text += (f" Caution: the accumulation-radar has {tk} "
                             f"{'at a likely top with bearish volume divergence' if _cyc_warning == 'likely_top_bearish_divergence' else 'under distribution'} — "
                             "watch for a better entry.")
            if _rf:
                why_text += f" ⚠️ Red flag: {tk} {'; '.join(_rf)} — conviction discounted."
        else:
            why_text = None

        _ed = _edates.get(tk)
        _eid = None
        if _ed:
            try:
                from datetime import date as _date
                _eid = (_date.fromisoformat(str(_ed)[:10]) -
                        datetime.now(timezone.utc).date()).days
            except Exception:
                _eid = None
        setups.append({
            "why": why_text,
            "ticker": tk,
            "name": rec["name"],
            "conviction": round(composite * _wl_mult, 1),
            "khalid_panels": _wl_ctx,
            "khalid_panel_multiplier": _wl_mult,
            "khalid_panel_audit": _wl_audit or None,
            # ops 3145 fusion fields (additive)
            "earnings_date": _ed,
            "earnings_in_days": _eid,
            "earnings_flag": bool(_eid is not None and 0 <= _eid <= 7),
            "squeeze_fuel": _sq_idx.get(tk),
            "khalid_note": (lambda _n: {
                "n": _n["n_notes"], "stance": _n["stance"],
                "score": _n["stance_score"], "last": _n["last_note_at"],
                "view": (_n.get("llm_view") or {}).get("view"),
            } if _n else None)(_notes_idx.get(str(tk).upper())),
            "industry_flow_quadrant": ((_ind_row or {}).get("fund_flows")
                                        or {}).get("quadrant"),
            "industry_flow_z": ((_ind_row or {}).get("fund_flows")
                                 or {}).get("flow_zscore_90d"),
            "risk_regime_mult": _rr_mult,
            "industry_mult": _ind_mult,
            "industry_etf": (_ind_row or {}).get("etf"),
            "industry_score": (_ind_row or {}).get("leadership_score"),
            "industry_tag": ((_ind_row or {}).get("tag")
                             or ("CROWDED" if (_ind_row or {})
                                 .get("crowded") else None)),
            "factor_regime_mult": _fa_mult,
            "nowcast_regime_mult": _nc_mult,
            "cycle_phase": (_cyc or {}).get("phase"),
            "cycle_flag": (_cyc or {}).get("flag"),
            "cycle_warning": _cyc_warning,
            "red_flags": _rf,
            "quad_threat": quad_threat,
            "verdict": verdict,
            "triple_threat": triple_threat,
            "buildout_threat": buildout_threat,
            "brain_aligned": brain_match(rec.get("sector"), [s["key"] for s in signals]),
            "value_lenses": sorted(value_signals),
            "flow_lenses": sorted(flow_signals),
            "n_signals": n,
            "n_independent_bets": round(n_eff, 2),
            "n_factor_families": n_fam,
            "factor_families": fam_list,
            "confluence_mult": round(confluence, 3),
            "signals": sorted(signals, key=lambda s: -s["strength"] * s["weight"]),
            "signal_keys": [s["key"] for s in signals],
            "entry": tt.get("entry"),
            "stop": tt.get("stop_loss"),
            "tp3": tt.get("tp3"),
            "rr": tt.get("rr_tp3"),
            "horizon_days": tt.get("expected_horizon_days"),
            "horizon_regime": tt.get("horizon_regime"),
            "thesis": thesis,
        })

    setups.sort(key=lambda s: -s["conviction"])

    # ── BOND-VOL RISK-REGIME GATE ──
    # Bond vol is the leading cross-asset risk gauge. In ELEVATED/CRISIS regimes
    # (risk-off), temper long conviction (correlations rise, diversification
    # fails); in LOW/NORMAL, leave full conviction. This makes the whole board
    # regime-aware rather than firing the same in calm and crisis.
    bv_z = bond_vol.get("composite_z_score")
    bv_posture = bond_vol.get("risk_posture")
    regime_mult = {"CRISIS": 0.78, "ELEVATED": 0.90, "NORMAL": 1.0,
                   "BOND_VOL_LOW": 1.04, "DATA_UNAVAILABLE": 1.0}.get(bv_regime, 1.0)
    if regime_mult != 1.0:
        for s in setups:
            s["conviction"] = round(min(100.0, s["conviction"] * regime_mult), 1)
            s["bond_vol_adjusted"] = True
        setups.sort(key=lambda s: -s["conviction"])

    # ── Structural-chokepoint overlay ──
    # Tag setups the chokepoint engine marks as structurally indispensable (curated /
    # LLM-confirmed / supply-chain hub). This is durability CONTEXT, not an alpha boost:
    # a given setup is more robust on a name its industry can't route around. When a
    # structural name is ALSO at a cyclical trough or cheap, it's the system's best setup.
    _structural = chokepoint.get("structural_names") or {}
    _hiconv = {r.get("ticker"): r for r in (chokepoint.get("highest_conviction_book") or [])}
    for s in setups:
        sn = _structural.get(s.get("ticker"))
        if not sn:
            continue
        s["structural_chokepoint"] = True
        s["criticality"] = sn.get("criticality")
        hc = _hiconv.get(s.get("ticker"))
        if hc:
            s["structural_setup"] = hc.get("setup_type")
        if s.get("why"):
            note = "a structural chokepoint its industry can't route around"
            if hc:
                note += " — and " + str(hc.get("setup_type", "")).lower().replace("_", " ") + ", the system's highest-quality setup"
            s["why"] = s["why"].rstrip(".") + f". Note: {s['ticker']} is {note}."

    # ── Meta-intelligence overlay: brains you built but never wired into decisions ──
    # premortem kill-theses (failure mode per pick), engine-conflicts (the system arguing
    # with itself), lead-lag (a pick whose leader already moved). Each annotates the pick
    # so a setup carries its OWN failure mode and any internal disagreement, eyes-open.
    kill_by = {}
    for t in (kill_theses.get("theses") or []):
        tk = (t.get("symbol") or t.get("ticker") or "").upper()
        if not tk or t.get("error"): continue
        kc = sorted((t.get("kill_conditions") or []), key=lambda k: -(k.get("severity") or 0))
        kill_by[tk] = {"failure_mode": t.get("thesis_summary"), "top_kill": kc[0] if kc else None}
    conflict_by = {(c.get("ticker") or "").upper(): c for c in (engine_conflicts.get("conflicts") or [])}
    leadlag_by = {}
    for lp in (lead_lag.get("live_predictions") or []):
        for f in (lp.get("followers_expected") or []):
            sym = (f.get("symbol") or "").upper()
            if sym and sym not in leadlag_by:
                leadlag_by[sym] = {"leader": lp.get("leader"), "expected_dir": f.get("expected_dir"),
                                   "lag_days": f.get("lag_days"), "lead_corr": f.get("lead_corr"),
                                   "leader_move_2d_pct": lp.get("leader_move_2d_pct")}
    n_kill = n_conf = n_tail = 0
    for s in setups:
        tk = (s.get("ticker") or "").upper()
        k = kill_by.get(tk)
        if k:
            s["kill_thesis"] = k["failure_mode"]; s["kill_condition"] = k["top_kill"]; n_kill += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Kill-thesis: {k['failure_mode']}"
        c = conflict_by.get(tk)
        if c:
            s["conflict"] = {"type": c.get("type"), "bull": c.get("bull"), "bear": c.get("bear"), "resolution": c.get("resolution")}
            s["contested"] = True; n_conf += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". CONTESTED ({c.get('type')}) — {c.get('bear')}"
        ll = leadlag_by.get(tk)
        if ll and ll.get("expected_dir") == "UP":
            s["lead_lag_tailwind"] = ll; n_tail += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Lead-lag tailwind: {ll.get('leader')} moved {ll.get('leader_move_2d_pct')}% ({ll.get('lag_days')}d lead)."

    # ── #4a: wire the orphaned meta-engines (were islands feeding nothing) ──
    # equity-confluence = cross-family confluence synthesizer; resilience = absorption/
    # ignition radar; strategist = strategic verdict. A best-setups name independently
    # confirmed by these carries real corroboration, so we annotate it.
    ec_by = {}
    for r in (equity_conf.get("confluence_book") or []):
        r = r if isinstance(r, dict) else {}
        tk = (r.get("ticker") or "").upper()
        if tk: ec_by[tk] = {"composite": r.get("composite"), "super": r.get("super"),
                            "family": r.get("family"), "proven": False}
    for r in (equity_conf.get("proven_book") or []):
        r = r if isinstance(r, dict) else {}
        tk = (r.get("ticker") or "").upper()
        if tk: ec_by.setdefault(tk, {}); ec_by[tk]["proven"] = True; ec_by[tk]["composite"] = r.get("composite")
    res_by = {}
    for book, flag in [("about_to_boom", "about-to-boom"), ("igniting", "igniting"), ("coiled", "coiled"),
                       ("top_picks", "resilient-top"), ("flow_confirmed", "flow-confirmed")]:
        for r in (resilience_doc.get(book) or []):
            r = r if isinstance(r, dict) else {}
            tk = (r.get("ticker") or "").upper()
            if not tk: continue
            res_by.setdefault(tk, {"flags": [], "score": None})
            res_by[tk]["flags"].append(flag)
            if r.get("score") is not None: res_by[tk]["score"] = r.get("score")
    strat_by = {(r.get("ticker") or "").upper(): r for r in (strategist_doc.get("picks") or []) if isinstance(r, dict)}
    n_ec = n_res = n_strat = 0
    n_opt = n_flow = n_aligned = n_conflict = 0
    for s in setups:
        tk = (s.get("ticker") or "").upper()
        if tk in ec_by:
            s["meta_confluence"] = ec_by[tk]; n_ec += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Cross-family confluence (composite {ec_by[tk].get('composite')}, {ec_by[tk].get('super')})."
        if tk in res_by:
            s["resilience"] = res_by[tk]; n_res += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Resilience radar: {', '.join(res_by[tk]['flags'][:2])}."
        if tk in strat_by:
            s["strategist"] = {"verdict": strat_by[tk].get("verdict"), "conviction": strat_by[tk].get("conviction")}; n_strat += 1
        # ── #4b: options & flow synthesizers (the fused cluster postures) ──
        op = opt_map.get(tk); fp = flow_map.get(tk)
        if op:
            s["options_posture"] = op.get("posture"); s["options_confluence"] = op; n_opt += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Options: {op.get('posture')} ({op.get('n_engines')} engines)."
        if fp:
            s["flow_posture"] = fp.get("posture"); s["flow_confluence"] = fp; n_flow += 1
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Flow: {fp.get('posture')} ({fp.get('n_engines')} engines)."
        ec = earn_map.get(tk)
        if ec and (ec.get("n_dimensions") or 0) >= 2:
            s["earnings_confluence"] = {"composite": ec.get("composite"), "n_dimensions": ec.get("n_dimensions"),
                                        "dimensions": ec.get("dimensions")}
            if s.get("why"): s["why"] = s["why"].rstrip(".") + f". Earnings confluence: {ec.get('n_dimensions')} dims {ec.get('dimensions')}."
        if op and fp:
            obull = op.get("posture") in ("SQUEEZE_FUEL", "BULLISH_FLOW", "BULLISH_LEAN")
            obear = op.get("posture") in ("BEARISH_FLOW", "BEARISH_LEAN")
            fbull = fp.get("posture") in ("ACCUMULATION", "STEALTH_ACCUMULATION", "SHORT_SQUEEZE_SETUP", "ACCUMULATION_LEAN")
            fbear = fp.get("posture") in ("DISTRIBUTION", "DISTRIBUTION_LEAN")
            if obull and fbull:
                s["synth_corroboration"] = "ALIGNED_BULLISH"; n_aligned += 1
                if s.get("why"): s["why"] = s["why"].rstrip(".") + " Options + flow ALIGNED BULLISH."
            elif (obull and fbear) or (obear and fbull):
                s["synth_corroboration"] = "CONFLICTED"; n_conflict += 1
                if s.get("why"): s["why"] = s["why"].rstrip(".") + " Options/flow CONFLICTED — positioning vs institutional money disagree."

    by_verdict = defaultdict(list)
    for s in setups:
        by_verdict[s["verdict"]].append(s["ticker"])

    # ── ops 3403: entry gate (participation) + hold horizon + SELF-GRADING ──
    try:
        fz = read_json("data/finviz-signals.json", {}) or {}
        _rv = {}

        def _rvwalk(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                rv = o.get("rel_volume") or o.get("relvol")
                if tk and isinstance(rv, (int, float)):
                    k = str(tk).upper()
                    _rv[k] = max(_rv.get(k, 0.0), float(rv))
                for v in o.values():
                    _rvwalk(v)
            elif isinstance(o, list):
                for v in o:
                    _rvwalk(v)
        _rvwalk(fz)
        hl = read_json("data/signal-halflife.json", {}) or {}
        _hvals = []

        def _hlwalk(o):
            if isinstance(o, dict):
                v = o.get("half_life")
                if isinstance(v, (int, float)) and v > 0:
                    _hvals.append(float(v))
                for x in o.values():
                    _hlwalk(x)
            elif isinstance(o, list):
                for x in o:
                    _hlwalk(x)
        _hlwalk(hl)
        _hvals.sort()
        _fleet_hold = round(_hvals[len(_hvals) // 2], 0) if _hvals else 21
        for _s in setups:
            _tk = str(_s.get("ticker") or "").upper()
            _r = _rv.get(_tk)
            _s["rel_volume"] = _r
            _s["entry_confirmed"] = (bool(_r >= 1.3) if _r is not None else None)
            _s["hold_horizon_days"] = _fleet_hold
        # ops 3410 (#6): options expression bridge — v1 rule set. Uses the
        # options-confluence synthesizer score (no clean IVR feed yet — noted).
        _oc = read_json("data/options-confluence.json", {}) or {}
        _ocm = {}

        def _ocwalk(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                sc = o.get("score")
                if tk and isinstance(sc, (int, float)):
                    k = str(tk).upper()
                    _ocm[k] = max(_ocm.get(k, 0.0), float(sc))
                for v in o.values():
                    _ocwalk(v)
            elif isinstance(o, list):
                for v in o:
                    _ocwalk(v)
        _ocwalk(_oc)
        _sfs = read_json("data/sector-flow-state.json", {}) or {}
        _sec_ctx = {}

        def _sfwalk2(o):
            if isinstance(o, dict):
                sec = o.get("sector")
                if sec and ("posture" in o or "conviction" in o):
                    _sec_ctx.setdefault(str(sec), {
                        "posture": o.get("posture"),
                        "conviction": o.get("conviction")})
                for v in o.values():
                    _sfwalk2(v)
            elif isinstance(o, list):
                for v in o:
                    _sfwalk2(v)
        _sfwalk2(_sfs)
        _og = read_json("data/options-gamma.json", {}) or read_json("data/dealer-gex.json", {}) or {}
        _walls = {}

        def _ogwalk(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                lv = {k: o[k] for k in ("call_wall", "put_wall", "zero_gamma",
                                        "gamma_flip", "max_pain")
                      if isinstance(o.get(k), (int, float))}
                if tk and lv:
                    _walls.setdefault(str(tk).upper(), lv)
                for v in o.values():
                    _ogwalk(v)
            elif isinstance(o, list):
                for v in o:
                    _ogwalk(v)
        _ogwalk(_og)
        _playbook_ctx = []
        globals()["_PB_CTX"] = _playbook_ctx
        try:
            _pb = read_json("data/playbook-rules.json", {}) or {}
            _rules = [r for r in (_pb.get("rules") or []) if isinstance(r, dict)]
            _rules.sort(key=lambda r: -(r.get("hit_rate") or r.get("score") or 0))
            for _r in _rules[:3]:
                _playbook_ctx.append({k: _r.get(k) for k in
                                      ("series", "label", "when", "when_text",
                                       "hit_rate", "n", "then", "horizon")
                                      if _r.get(k) is not None})
        except Exception as _e:
            print(f"[playbook-ctx] {str(_e)[:60]}")
        for _s in setups[:25]:
            _tk = str(_s.get("ticker") or "").upper()
            _sc2 = _sec_ctx.get(str(_s.get("sector") or ""))
            if _sc2:
                _s["sector_context"] = _sc2
            if _tk in _walls:
                _s["gamma_levels"] = _walls[_tk]
            _os = _ocm.get(_tk)
            _s["options_confluence_score"] = _os
            if _os is not None and _os >= 70:
                _s["suggested_structure"] = ("call debit spread — options desks "
                                             "already confluent; defined risk "
                                             "into strength")
            elif _s.get("entry_confirmed"):
                _s["suggested_structure"] = ("3-6m calls, half size; add on "
                                             "participation follow-through")
            else:
                _s["suggested_structure"] = ("shares first; sell 30-45d covered "
                                             "calls after entry confirms")
        try:
            from signals_emit import log_signal, yprice
            _tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
            _logged = 0
            for _s in setups[:15]:
                _tk = str(_s.get("ticker") or "").upper()
                if not _tk:
                    continue
                _pr = yprice(_tk)
                _cv = float(_s.get("conviction") or _s.get("score") or 50)
                if log_signal(_tbl, "best-setup-stack", _tk, "UP", [5, 21, 63], _pr,
                              confidence=min(0.9, 0.5 + _cv / 250.0),
                              rationale="best-setups top stack (multi-lens confluence)",
                              signal_value=str(round(_cv, 1)),
                              metadata={"engine": "best-setups"}):
                    _logged += 1
            print(f"[best-setups] self-logged {_logged} stack signals")
        except Exception as _e:
            print(f"[best-setups] self-log failed: {str(_e)[:80]}")
    except Exception as _e:
        print(f"[best-setups] 3403 layer failed: {str(_e)[:80]}")

    output = {
        "schema_version": "1.0",
        "engine": "best-setups (unified conviction)",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "weight_source": weight_src,
        "bond_vol_regime": {"regime": bv_regime or None, "composite_z": bv_z,
                            "risk_posture": bv_posture, "conviction_multiplier": regime_mult},
        "nowcast_regime": {"regime": _nc_regime, "growth": _nc_quad_growth, "inflation": _nc_quad_infl,
                           "gdpnow": _nc_q.get("gdpnow"), "underlying_inflation": _nc_q.get("underlying_inflation")},
        "methodology": (
            "conviction = Σ(signal_strength × learned_weight) × confluence(1+0.22 "
            "per extra independent signal, cap 2.2) × 22, clamped 100. Weights are "
            "institutional priors blended toward per-tier hit rates as the self-"
            "improvement loop accumulates scored outcomes. Confluence across "
            "INDEPENDENT signal families is the core driver."
        ),
        "playbook_context": globals().get("_PB_CTX", []),
        "stats": {
            "n_setups": len(setups),
            "strong_buy": len(by_verdict["STRONG BUY"]),
            "buy": len(by_verdict["BUY"]),
            "watch": len(by_verdict["WATCH"]),
        },
        "top_setups": setups[:50],
        "quad_threats": [s for s in setups if s.get("quad_threat")][:15],
        "triple_threats": [s for s in setups if s.get("triple_threat")][:20],
        "buildout_threats": [s for s in setups if s.get("buildout_threat")][:20],
        "brain_aligned": [s for s in setups if s.get("brain_aligned")][:25],
        "structural_chokepoints": [s for s in setups if s.get("structural_chokepoint")][:30],
        "contested_picks": [s for s in setups if s.get("contested")][:20],
        "picks_with_kill_thesis": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                                    "failure_mode": s.get("kill_thesis"), "kill_condition": s.get("kill_condition")}
                                   for s in setups if s.get("kill_thesis")][:25],
        "lead_lag_tailwinds": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                                "lead_lag": s.get("lead_lag_tailwind")} for s in setups if s.get("lead_lag_tailwind")][:15],
        "meta_confluence_book": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                                  "meta_confluence": s.get("meta_confluence")} for s in setups if s.get("meta_confluence")][:25],
        "resilient_setups": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                              "resilience": s.get("resilience")} for s in setups if s.get("resilience")][:25],
        "synth_aligned_bullish": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                                   "options": s.get("options_posture"), "flow": s.get("flow_posture")}
                                  for s in setups if s.get("synth_corroboration") == "ALIGNED_BULLISH"][:25],
        "synth_conflicted": [{"ticker": s["ticker"], "conviction": s.get("conviction"),
                              "options": s.get("options_posture"), "flow": s.get("flow_posture")}
                             for s in setups if s.get("synth_corroboration") == "CONFLICTED"][:25],
        "synthesizer_wiring": {"n_options_tagged": n_opt, "n_flow_tagged": n_flow,
                               "n_aligned_bullish": n_aligned, "n_conflicted": n_conflict,
                               "note": ("The options-confluence and flow-confluence synthesizers (which fuse the "
                                        "fragmented 21-engine options and 35-engine flow clusters) now annotate every "
                                        "pick. ALIGNED_BULLISH = positioning and institutional money agree; CONFLICTED "
                                        "= they disagree (e.g. bullish options flow but institutions distributing).")},
        "orphan_meta_wiring": {"n_cross_family_confluence": n_ec, "n_resilient": n_res, "n_strategist": n_strat,
                               "note": ("equity-confluence, resilience and strategist were islands feeding nothing; "
                                        "they now corroborate best-setups picks — a name independently confirmed by the "
                                        "cross-family synthesizer and the resilience radar carries that weight.")},
        "alpha_trust_wiring": {
            "n_signals_lifted": len(lifted_signals), "n_signals_pruned": len(pruned_signals),
            "lifted_sample": lifted_signals[:10], "pruned_sample": pruned_signals[:10],
            "proven_squeeze_lift": trust_by.get("squeeze_risk", {}).get("effective_trust"),
            "note": ("squeeze signals inherit the ALPHA_PROVEN squeeze_risk lift (effective_trust folded into "
                     "weight); any signal mapping to an ALPHA_NEGATIVE engine is hard-pruned before confluence. "
                     "The 28 chronic-negative engines are macro signals this equity board does not ingest, so "
                     "pruning rarely fires here by design — the macro-board enforcement lives in the synthesizers."),
        },
        "meta_intelligence": {"n_with_kill_thesis": n_kill, "n_contested": n_conf, "n_lead_lag_tailwind": n_tail,
                              "signal_independence_gsi": orthogonality.get("gsi_total") or orthogonality.get("gsi"),
                              "orthogonality_mode": orthogonality.get("mode"),
                              "note": "Each pick now carries its premortem failure mode; contested picks show both sides; lead-lag tailwinds flag picks whose leader already moved."},
        "structural_at_trough": [s for s in setups if s.get("structural_setup")][:15],
        "by_verdict": dict(by_verdict),
        "industry_context": {"meta": _ind_meta,
                             "factor_appetite": _fa_score},
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[best-setups] {len(setups)} setups · {len(by_verdict['STRONG BUY'])} strong / "
          f"{len(by_verdict['BUY'])} buy · weights={weight_src} · {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "n_setups": len(setups),
                                 "strong_buy": len(by_verdict["STRONG BUY"]),
                                 "buy": len(by_verdict["BUY"]), "weight_source": weight_src})}
