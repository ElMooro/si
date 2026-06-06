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
    "COMPOUNDER":           0.80,   # durable quality growth (ROIC+margin+growth)
    "REVISION_UP":          0.78,   # analyst estimate-revision momentum
    "DISLOCATION":          0.78,   # relative-value buy-the-laggard
    "BUYBACK":              0.74,   # aggressive share repurchase (price support, ↑EPS)
    "CAPEX_ACCEL":          0.70,   # surging capex in a buildout sector (AI/power demand)
    "INSIDER_CLUSTER":      0.80,   # multi-insider buying
    "SHORT_SQUEEZE":        0.66,   # FINRA short-volume z-score + squeeze setup
    "FDA_CATALYST":         0.62,   # upcoming PDUFA/AdCom binary event
    "GOV_CONTRACT":         0.58,   # material federal contract award
    "EXECUTIVE_BUY":        0.72,   # executive-branch proximity
    "OPTIONS_EXTREME":      0.70,   # extreme smart-money call flow
    "CASCADE_ALERT":        0.65,   # theme cascade alert tier
    "CONVERGENCE":          0.60,   # multi-engine convergence
    "POLITICIAN_BUY":       0.55,   # congress buy w/o committee edge
    "OPTIONS_BULLISH":      0.55,
    "EARLY_MOVER":          0.55,
    "EARNINGS_FRESH":       0.52,   # post-earnings drift
    "CASCADE_LAGGARD":      0.50,   # catch-up play
    "RETAIL_HOT":           0.45,   # can be pump/noise
    "RETAIL_VELOCITY":      0.40,
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


def lambda_handler(event, context):
    t0 = time.time()
    cascade = read_json("data/theme-cascade.json") or {}
    options = read_json("data/polygon-options-flow.json") or {}
    insider = read_json("data/insider-clusters.json") or {}
    finra_short = read_json("data/finra-short.json") or {}
    catalysts = read_json("data/catalyst-calendar.json") or {}
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
    bond_vol = read_json("data/bond-vol.json") or {}
    # ── The Brain: Khalid's pinned principles + watched tickers. We flag setups
    # that align with what's on his mind so the board surfaces HIS theses. ──
    brain = read_json("data/brain.json") or {}
    brain_directive = brain.get("directive") or {}
    brain_tickers = set((brain.get("mentioned_tickers") or []))
    brain_themes = [t.lower() for t in (brain_directive.get("themes") or [])]
    brain_tilts = {k.lower(): v for k, v in (brain_directive.get("sector_tilts") or {}).items()}
    brain_pinned = []
    for n in (brain.get("notes") or []):
        if n.get("pinned") and n.get("text"):
            brain_pinned.append((n["text"].lower(), n["text"]))
    def brain_match(ticker, sector, signal_keys):
        """Smart brain alignment: watched ticker, sector tilt (overweight), theme
        keyword, or pinned-note match. Returns the matched reason or None."""
        if ticker in brain_tickers:
            return f"You're tracking {ticker}"
        sl = (sector or "").lower()
        # sector tilt from the AI directive
        for sec, stance in brain_tilts.items():
            if sl and (sl in sec or sec in sl):
                low = stance.lower()
                if "overweight" in low:
                    return f"Your brain is overweight {sector}: {stance[:90]}"
                if "avoid" in low or "underweight" in low:
                    return None  # don't flag setups you want to avoid
        # theme match against sector
        for th in brain_themes:
            if sl and (sl in th or any(w in th for w in sl.split())):
                return f"Fits your theme: {th}"
        tl = (ticker or "").lower()
        for low, orig in brain_pinned:
            if tl and tl in low.split():
                return orig[:110]
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
    for tk, rec in sig.items():
        signals = rec["signals"]
        if not signals:
            continue
        n = len(signals)
        confluence = 1.0 + 0.22 * (n - 1)
        confluence = min(confluence, 2.2)
        sector = rec.get("sector") or rec.get("industry")
        cap = opp_cap.get(tk)
        raw = 0.0
        for s in signals:
            cw = context_weight(s["key"], s["weight"], sector, cap, s.get("age_days"))
            s["context_weight"] = round(cw, 3)
            raw += s["strength"] * cw
        composite = round(min(100.0, raw * confluence * 22), 1)

        # Verdict from composite + confluence
        if composite >= 55 and n >= 3:
            verdict = "STRONG BUY"
        elif composite >= 35 and n >= 2:
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
        else:
            why_text = None

        setups.append({
            "why": why_text,
            "ticker": tk,
            "name": rec["name"],
            "conviction": round(composite, 1),
            "quad_threat": quad_threat,
            "verdict": verdict,
            "triple_threat": triple_threat,
            "buildout_threat": buildout_threat,
            "brain_aligned": brain_match(tk, rec.get("sector"), [s["key"] for s in signals]),
            "value_lenses": sorted(value_signals),
            "flow_lenses": sorted(flow_signals),
            "n_signals": n,
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

    by_verdict = defaultdict(list)
    for s in setups:
        by_verdict[s["verdict"]].append(s["ticker"])

    output = {
        "schema_version": "1.0",
        "engine": "best-setups (unified conviction)",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "weight_source": weight_src,
        "bond_vol_regime": {"regime": bv_regime or None, "composite_z": bv_z,
                            "risk_posture": bv_posture, "conviction_multiplier": regime_mult},
        "methodology": (
            "conviction = Σ(signal_strength × learned_weight) × confluence(1+0.22 "
            "per extra independent signal, cap 2.2) × 22, clamped 100. Weights are "
            "institutional priors blended toward per-tier hit rates as the self-"
            "improvement loop accumulates scored outcomes. Confluence across "
            "INDEPENDENT signal families is the core driver."
        ),
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
        "by_verdict": dict(by_verdict),
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
