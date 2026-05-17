"""
justhodl-conviction-engine — Full-System Conviction Layer.

The platform runs 270+ engines. signal-board aggregates eight of them into
one equal-weighted posture. This engine is the layer ABOVE that: it answers
the only question that matters to a desk — "what are the highest-conviction,
actionable setups RIGHT NOW, and why."

How a real multi-strategy fund builds a conviction sheet — replicated here:

  1. INGEST every directional engine's headline read, normalised to a
     5-state signal (-2..+2), the same contract signal-board uses.
  2. SKILL-WEIGHT each engine by its *proven* track record. signal-scorecard
     measures realised hit-rate per signal type and publishes a performance
     multiplier; an engine that has been right gets more vote than one that
     has not. Equal weighting is the naive version — this is not.
  3. DECORRELATE. A thesis confirmed by six *independent* engine families is
     real conviction; six momentum engines agreeing is one signal wearing six
     hats. Within a family, additional confirmations get diminishing weight.
  4. GROUP BY SUBJECT — a desk thinks in positions (broad risk, value tilt,
     crypto, ...), not in engines.
  5. SCORE conviction 0-100 from skill-weighted strength x agreement x
     independent breadth, and RANK.
  6. Emit each setup with its evidence trail and an explicit INVALIDATION
     trigger, plus a daily snapshot so a calibrator can later verify that
     high-conviction actually out-performs low-conviction.

OUTPUT: data/conviction.json  (+ daily snapshot)   SCHEDULE: every 3h
This is a synthesis of the platform's own engines — not investment advice.
"""
import json
import time
from datetime import datetime, timezone, timedelta

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/conviction.json"
STALE_HOURS = 30

SIG_LABEL = {2: "STRONG RISK-ON", 1: "RISK-ON", 0: "NEUTRAL",
             -1: "RISK-OFF", -2: "STRONG RISK-OFF"}


def read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"]
    except Exception:
        return None, None


def clamp(v):
    return max(-2, min(2, int(round(v))))


def sign(v):
    return 1 if v > 0 else -1 if v < 0 else 0


# ── per-engine normalisers — each returns (signal:-2..2, read:str) ──
# Eight reused verbatim from signal-board (proven contract); three new.
def n_pm_decision(d):
    pw = (d.get("posture_word") or "").upper()
    m = {"AGGRESSIVE": 2, "CONSTRUCTIVE": 1, "NEUTRAL": 0,
         "CAUTIOUS": -1, "DEFENSIVE": -2}
    return m.get(pw, 0), f"Desk posture {pw or 'n/a'}"


def n_cross_asset_rv(d):
    st = (d.get("rv_state") or "").upper()
    m = {"ALIGNED": 1, "STRETCHED": 0, "DISLOCATION_PRESENT": -1}
    return m.get(st, 0), f"RV {st.replace('_', ' ').lower() or 'n/a'}"


def n_fundamentals(d):
    s = d.get("summary") or {}
    uv, ov = s.get("n_undervalued") or 0, s.get("n_overvalued") or 0
    sig = 1 if uv > ov * 1.5 else -1 if ov > uv * 1.5 else 0
    return sig, f"{uv} undervalued vs {ov} overvalued (DCF)"


def n_construction_housing(d):
    rg = (d.get("regime") or "").upper()
    m = {"EXPANSION": 2, "RECOVERY": 1, "SLOWING": -1, "CONTRACTION": -2}
    return m.get(rg, 0), f"Housing cycle {rg or 'n/a'}"


def n_crypto_narratives(d):
    st = (d.get("stance") or "").upper()
    m = {"RISK-ON ROTATION": 2, "SELECTIVE": 0, "RISK-OFF": -2}
    br = d.get("narrative_breadth_pct")
    return m.get(st, 0), f"Crypto {st or 'n/a'} ({br}% breadth)"


def n_short_pressure(d):
    b = d.get("n_pressure_building") or 0
    c = d.get("n_shorts_covering") or 0
    sig = 1 if c > b * 1.5 else -1 if b > c * 1.5 else 0
    return sig, f"{b} building short pressure, {c} covering"


def n_mean_reversion(d):
    ch = d.get("n_cheap_vs_history") or 0
    ri = d.get("n_rich_vs_history") or 0
    sig = 1 if ch > ri * 1.3 else -1 if ri > ch * 1.3 else 0
    return sig, f"{ch} cheap vs {ri} rich on own multiple history"


def n_canary_grid(d):
    band = (d.get("band") or "").upper()
    m = {"CALM": 1, "WATCH": 0, "ELEVATED": -1, "WARNING": -2, "CRITICAL": -2}
    lvl = d.get("early_warning_level")
    return m.get(band, 0), f"Global early-warning {band or 'n/a'} ({lvl}/100)"


def n_crisis_composite(d):
    lvl = d.get("defcon_level")
    nm = d.get("defcon_name") or d.get("label") or ""
    # DEFCON 5 = calm (risk-on) ... DEFCON 1 = crisis (risk-off)
    m = {5: 2, 4: 1, 3: 0, 2: -2, 1: -2}
    return m.get(lvl, 0), f"Crisis DEFCON {lvl if lvl is not None else 'n/a'} {nm}".strip()


def n_leading_markets(d):
    reg = (d.get("dominant_regime") or d.get("regime") or "").upper()
    m = {"EXPANSION": 2, "RECOVERY": 1, "IMPROVING": 1, "MIXED": 0,
         "SLOWING": -1, "DETERIORATING": -1, "CONTRACTION": -2}
    sig = m.get(reg, 0)
    tp = d.get("turning_point_signal") or d.get("signal_read") or ""
    return sig, (f"Leading markets {reg or 'n/a'}"
                 + (f" — {tp}" if tp else ""))


def n_opportunity(d):
    vc = d.get("verdict_counts") or {}
    pos = neg = 0
    for k, v in vc.items():
        kl = str(k).lower()
        try:
            v = int(v)
        except (TypeError, ValueError):
            continue
        if "bargain" in kl or "under" in kl or "cheap" in kl:
            pos += v
        elif "expens" in kl or "over" in kl or "avoid" in kl or "rich" in kl:
            neg += v
    if pos == 0 and neg == 0:
        n = d.get("n_opportunities") or len(d.get("top_opportunities") or [])
        sig = 1 if n >= 8 else 0
        return sig, f"{n} opportunities flagged (full S&P 500 scan)"
    sig = (2 if pos > neg * 2 else 1 if pos > neg * 1.3
           else -2 if neg > pos * 2 else -1 if neg > pos * 1.3 else 0)
    return sig, f"{pos} bargains vs {neg} expensive/avoid (S&P 500 scan)"


# (engine, subject, family, s3_key, normaliser)
FEEDS = [
    ("PM Decision",        "Broad risk / equity beta", "macro-posture",
     "data/pm-decision.json",          n_pm_decision),
    ("Crisis Composite",   "Broad risk / equity beta", "macro-posture",
     "data/crisis-composite.json",     n_crisis_composite),
    ("Canary Grid",        "Broad risk / equity beta", "macro-posture",
     "data/canary-grid.json",          n_canary_grid),
    ("Leading Markets",    "Broad risk / equity beta", "macro-posture",
     "data/leading-markets.json",      n_leading_markets),
    ("Housing Cycle",      "US macro / housing cycle", "macro-fundamental",
     "data/construction-housing.json", n_construction_housing),
    ("Cross-Asset RV",     "Cross-asset relative value", "relative-value",
     "data/cross-asset-rv.json",       n_cross_asset_rv),
    ("Fundamentals X-Ray", "US equity — value tilt",   "equity-value",
     "data/fundamentals.json",         n_fundamentals),
    ("Mean Reversion",     "US equity — value tilt",   "equity-value",
     "screener/mean-reversion.json",   n_mean_reversion),
    ("Opportunity Engine", "US equity — value tilt",   "equity-value",
     "data/opportunities.json",        n_opportunity),
    ("Short Pressure",     "US equity — positioning",  "positioning",
     "data/short-pressure.json",       n_short_pressure),
    ("Crypto Narratives",  "Crypto",                   "crypto",
     "data/crypto-narratives.json",    n_crypto_narratives),
]

# per-subject invalidation tells
INVALIDATION = {
    "Broad risk / equity beta":
        "Crisis Composite DEFCON to <=2, Canary Grid prints WARNING/CRITICAL, "
        "or the PM desk turns DEFENSIVE",
    "US macro / housing cycle":
        "the housing regime flips to CONTRACTION",
    "Cross-asset relative value":
        "a fresh cross-asset DISLOCATION opens up",
    "US equity — value tilt":
        "the DCF / multiple scans flip to net-rich",
    "US equity — positioning":
        "short pressure flips from covering back to building",
    "Crypto":
        "crypto narratives flip to RISK-OFF",
}


def load_skill_weights():
    """engine-token -> performance multiplier, from signal-scorecard."""
    d, _ = read_json("data/signal-scorecard.json")
    w = {}
    if not d:
        return w
    mult = d.get("multipliers")
    if isinstance(mult, dict):
        for k, v in mult.items():
            try:
                w[str(k).lower()] = float(v)
            except (TypeError, ValueError):
                pass
    sc = d.get("scorecard")
    rows = (sc if isinstance(sc, list)
            else list(sc.values()) if isinstance(sc, dict) else [])
    for e in rows:
        if not isinstance(e, dict):
            continue
        nm = e.get("signal_type") or e.get("signal") or e.get("type")
        m = e.get("performance_multiplier") or e.get("multiplier")
        if nm and m is not None:
            try:
                w[str(nm).lower()] = float(m)
            except (TypeError, ValueError):
                pass
    return w


def skill_for(engine, weights):
    """Fuzzy-match an engine name to a scorecard multiplier (clamped)."""
    if not weights:
        return 1.0, False
    key = engine.lower()
    for wk, wv in weights.items():
        if wk and (wk in key or key in wk):
            return max(0.5, min(2.0, wv)), True
    toks = set(key.replace("-", " ").split())
    for wk, wv in weights.items():
        if toks & set(wk.replace("-", " ").replace("_", " ").split()):
            return max(0.5, min(2.0, wv)), True
    return 1.0, False


def build_setup(subject, members):
    """members: list of dicts {engine, family, signal, skill, read}."""
    # decorrelate: within a family, sort by conviction weight desc and
    # apply diminishing returns (1, 1/2, 1/3 ...).
    fams = {}
    for m in members:
        fams.setdefault(m["family"], []).append(m)
    eff_sig = eff_w = 0.0
    for fam, fm in fams.items():
        fm.sort(key=lambda x: abs(x["signal"]) * x["skill"], reverse=True)
        for i, m in enumerate(fm):
            w = m["skill"] / (i + 1)
            eff_sig += m["signal"] * w
            eff_w += w
    net = eff_sig / eff_w if eff_w else 0.0          # weighted mean, -2..2

    dirs = [sign(m["signal"]) for m in members if m["signal"] != 0]
    n_pos = sum(1 for x in dirs if x > 0)
    n_neg = sum(1 for x in dirs if x < 0)
    agree = (max(n_pos, n_neg) / len(dirs)) if dirs else 0.0
    n_fam = len(fams)

    strength = min(1.0, abs(net) / 2.0)
    breadth = min(1.0, n_fam / 4.0)
    quality = 0.5 + 0.3 * agree + 0.2 * breadth
    conviction = round(100 * strength * quality)
    band = ("HIGH" if conviction >= 70 else "MODERATE" if conviction >= 45
            else "LOW" if conviction >= 20 else "MARGINAL")
    direction = ("RISK-ON / LONG" if net > 0.15 else
                 "RISK-OFF / DEFENSIVE" if net < -0.15 else "NEUTRAL / MIXED")

    ranked = sorted(members, key=lambda x: abs(x["signal"]) * x["skill"],
                    reverse=True)
    tells = "; ".join(m["read"] for m in ranked[:3])
    thesis = (f"{len(members)} engines across {n_fam} independent "
              f"families, {agree:.0%} directional agreement -> "
              f"{direction.split(' / ')[0].lower()}. {tells}.")
    inv = (f"Flips if net signal crosses neutral — specifically if "
           f"{INVALIDATION.get(subject, 'the contributing engines reverse')}.")
    return {
        "subject": subject, "direction": direction,
        "conviction": conviction, "confidence": band,
        "net_signal": round(net, 2), "n_engines": len(members),
        "n_agree": max(n_pos, n_neg), "n_disagree": min(n_pos, n_neg),
        "agreement_pct": round(agree * 100), "n_families": n_fam,
        "thesis": thesis, "invalidation": inv,
        "contributing_engines": [
            {"engine": m["engine"], "family": m["family"],
             "signal": m["signal"], "signal_label": SIG_LABEL.get(m["signal"]),
             "skill_weight": round(m["skill"], 2),
             "skill_calibrated": m["calibrated"], "read": m["read"]}
            for m in ranked],
    }


def single_names():
    """Highest-conviction single names from the Opportunity Engine."""
    d, _ = read_json("data/opportunities.json")
    if not d:
        return []
    rows = d.get("top_opportunities") or d.get("opportunities") or []
    out = []
    for r in rows[:6]:
        if not isinstance(r, dict):
            continue
        out.append({
            "ticker": r.get("ticker") or r.get("symbol"),
            "company": r.get("company"),
            "verdict": r.get("verdict"),
            "score": r.get("opportunity_score") or r.get("score"),
            "upside_pct": r.get("under_pct") or r.get("upside_pct"),
        })
    return [x for x in out if x.get("ticker")]


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    weights = load_skill_weights()
    calibrated_any = bool(weights)

    engines, stale = [], 0
    for name, subject, family, key, fn in FEEDS:
        data, last_mod = read_json(key)
        skill, calib = skill_for(name, weights)
        if data is None:
            engines.append({"engine": name, "subject": subject,
                            "family": family, "signal": None,
                            "read": "sidecar missing", "stale": True,
                            "skill": skill, "calibrated": calib})
            stale += 1
            continue
        try:
            sig, read = fn(data)
            sig = clamp(sig)
        except Exception as e:
            sig, read = None, f"parse error: {str(e)[:80]}"
        is_stale = bool(last_mod and (now - last_mod) >
                        timedelta(hours=STALE_HOURS))
        if is_stale:
            stale += 1
        engines.append({"engine": name, "subject": subject, "family": family,
                        "signal": sig, "read": read, "stale": is_stale,
                        "skill": skill, "calibrated": calib,
                        "as_of": (data.get("generated_at")
                                  or (last_mod.isoformat() if last_mod else None))})

    live = [e for e in engines if e["signal"] is not None and not e["stale"]]

    # group live engines by subject -> build a conviction setup for each
    by_subj = {}
    for e in live:
        by_subj.setdefault(e["subject"], []).append(e)
    setups = [build_setup(s, ms) for s, ms in by_subj.items()]
    setups.sort(key=lambda x: x["conviction"], reverse=True)
    for i, s in enumerate(setups):
        s["rank"] = i + 1

    # book posture = breadth-weighted blend of the macro-posture subjects
    macro = [e for e in live if e["family"] in
             ("macro-posture", "macro-fundamental")]
    if macro:
        mnet = sum(e["signal"] * e["skill"] for e in macro) / \
               sum(e["skill"] for e in macro)
    else:
        mnet = 0.0
    book = ("RISK-ON" if mnet >= 1.0 else "MILDLY RISK-ON" if mnet >= 0.25
            else "NEUTRAL / MIXED" if mnet > -0.25
            else "MILDLY RISK-OFF" if mnet > -1.0 else "RISK-OFF")

    actionable = [s for s in setups if s["confidence"] in ("HIGH", "MODERATE")]
    headline = setups[0] if setups else None

    out = {
        "schema_version": "1.0",
        "method": "skill_weighted_decorrelated_conviction_synthesis",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "book_posture": book,
        "book_net_signal": round(mnet, 2),
        "headline_call": (
            None if not headline else
            {"subject": headline["subject"], "direction": headline["direction"],
             "conviction": headline["conviction"],
             "confidence": headline["confidence"]}),
        "n_setups": len(setups),
        "n_actionable": len(actionable),
        "setups": setups,
        "single_names": single_names(),
        "n_engines": len(engines),
        "n_live": len(live),
        "n_stale": stale,
        "skill_weighting": ("active — engines weighted by signal-scorecard "
                            "performance multipliers"
                            if calibrated_any else
                            "neutral — signal-scorecard not yet populated; "
                            "all engines at equal weight until it matures"),
        "note": ("Skill-weighted, decorrelated synthesis of the platform's "
                 "directional engines into ranked, actionable conviction "
                 "setups. Each engine is weighted by its proven hit-rate and "
                 "correlated engines share weight within a family. Stale "
                 f"feeds (>{STALE_HOURS}h) are excluded. Synthesis of the "
                 "platform's own engines — not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")

    # daily snapshot for the future conviction-calibrator
    try:
        snap = {"date": now.date().isoformat(), "book_posture": book,
                "setups": [{"subject": s["subject"], "direction": s["direction"],
                            "conviction": s["conviction"],
                            "net_signal": s["net_signal"]} for s in setups]}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"data/conviction/snapshots/{now.date().isoformat()}.json",
            Body=json.dumps(snap, default=str).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print(f"[conviction] snapshot skipped: {e}")

    print(f"[conviction] book={book} setups={len(setups)} "
          f"actionable={len(actionable)} live={len(live)}/{len(engines)} "
          f"stale={stale} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "book_posture": book, "n_setups": len(setups),
        "n_actionable": len(actionable),
        "headline": out["headline_call"]})}
