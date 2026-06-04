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
    "COMPOUNDER":           0.80,   # durable quality growth (ROIC+margin+growth)
    "REVISION_UP":          0.78,   # analyst estimate-revision momentum
    "DISLOCATION":          0.78,   # relative-value buy-the-laggard
    "INSIDER_CLUSTER":      0.80,   # multi-insider buying
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
        add(c.get("ticker"), c.get("company_name"), "INSIDER_CLUSTER",
            normalize(nb, 2, 8), f"{nb} insiders, ${round((c.get('total_value_usd') or 0)/1e6,1)}M")

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
    # estimate-revision UP (the alpha factor)
    for r in opp_rows:
        rev = r.get("estimate_revision") or {}
        if rev.get("direction") == "UP" and (rev.get("delta_pp") or 0) >= 1.0:
            add(r.get("ticker"), r.get("sector"), "REVISION_UP",
                normalize(rev.get("delta_pp"), 1, 8),
                f"analyst estimates revised +{rev.get('delta_pp')}pp")

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

    # ── Fuse ──
    setups = []
    for tk, rec in sig.items():
        signals = rec["signals"]
        if not signals:
            continue
        n = len(signals)
        confluence = 1.0 + 0.22 * (n - 1)
        confluence = min(confluence, 2.2)
        raw = sum(s["strength"] * s["weight"] for s in signals)
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

        setups.append({
            "ticker": tk,
            "name": rec["name"],
            "conviction": composite,
            "verdict": verdict,
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
    by_verdict = defaultdict(list)
    for s in setups:
        by_verdict[s["verdict"]].append(s["ticker"])

    output = {
        "schema_version": "1.0",
        "engine": "best-setups (unified conviction)",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "weight_source": weight_src,
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
