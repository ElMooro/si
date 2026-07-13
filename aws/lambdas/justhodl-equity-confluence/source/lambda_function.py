"""
justhodl-equity-confluence  ·  v1.0   —   CONFLUENCE RADAR (Tier 4 / Lever 6)
─────────────────────────────────────────────────────────────────────────────
The thesis you can't get from any single engine: a name lit up by SEVERAL
INDEPENDENT edges at once is a far stronger bet than any one signal. Resilience
+ dark-pool accumulation + positive revisions + a squeeze setup, stacked, is
asymmetric in a way none of them is alone.

Two hard rules, both designed to avoid fooling ourselves:

1. CORRELATION ADJUSTMENT (not signal-counting). Engines that measure the same
   thing co-move, so stacking three momentum signals is ONE bet wearing three
   hats, not three. We collapse the ~9 source engines into 9 FAMILIES, then map
   families onto 4 independent SUPER-FAMILIES (technical / flow / fundamental /
   structural). Confluence is scored on how many INDEPENDENT super-families
   light up — three correlated technical engines = 1 effective bet; technical +
   flow + fundamental = 3.

2. FDR GATING + AUTO-ACTIVATION (no noise-fitting). We do NOT learn weights from
   immature data. Each family carries a status read live from the scorecard /
   engine-alpha: ALPHA_PROVEN, ALPHA_NEGATIVE, or (default) PROVISIONAL. The
   engine runs as RESEARCH now (provisional book, clearly labelled), and the
   "proven_book" — names lit across >=2 INDEPENDENT proven super-families — is
   gated: it stays empty until the forward scorecard actually proves the
   components. The moment a 2nd independent super-family clears FDR, the engine
   flips to PROVEN mode on its own. Nothing to switch on by hand; nothing fit to
   noise in the meantime.

This is a VIEW over other engines, not a new signal — it deliberately does NOT
emit top_picks (no harvester logging) to avoid circular grading. Research, not advice.
"""
import json, time
from datetime import datetime, timezone

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/equity-confluence.json"

import boto3
s3 = boto3.client("s3", "us-east-1")

# family -> (super-family, scorecard signal_type candidates used for FDR gating)
FAMILY_SUPER = {
    "absorption":  ("technical",   ["eng:resilience", "resilience"]),
    "squeeze":     ("technical",   ["squeeze_risk", "eng:squeeze-fuel", "squeeze_fuel"]),
    "catalyst":    ("technical",   ["eng:boom-radar", "boom_radar"]),
    "flow_micro":  ("flow",        ["eng:dark-pool", "dark_pool", "eng:flow-lookthrough"]),
    "flow_macro":  ("flow",        ["etf_flow_extreme", "etf_rotation", "eng:capital-flow"]),
    "revision":    ("fundamental", ["eng:estimate-revisions", "estimate_revisions", "eng:analyst-actions"]),
    "pead":        ("fundamental", ["earnings_pead", "eng:earnings-tracker"]),
    "value":       ("fundamental", ["deep_value", "eng:deep-value-overlap", "value_overlap"]),
    "options":     ("structural",  ["eng:options-analytics", "options_analytics"]),
    "linkage":     ("structural",  ["eng:supply-chain-graph", "supply_chain_laggards"]),
    "convexity":   ("structural",  ["eng:convexity-scorer", "convexity", "right_tail"]),
    "insider":     ("flow",        ["insider_cluster", "eng:insider-clusters", "insider_buying"]),
}
SUPERS = ["technical", "flow", "fundamental", "structural"]

# source engine -> how to read bullish names: (file, family, [lists], strength_key, strength_div, bull_filter)
SOURCES = [
    ("resilience.json",        "absorption", ["about_to_boom", "all_resilient"],            "resilience", 100, None),
    ("squeeze-fuel.json",      "squeeze",    ["top_picks"],                                 "score",      100, ("direction", "long")),
    ("boom-radar.json",        "catalyst",   ["top_picks", "high_conviction"],              "score",      4,   None),
    ("dark-pool.json",         "flow_micro", ["top_accumulation"],                          "score",      100, None),
    ("flow-lookthrough.json",  "flow_micro", ["top_picks", "inflow_leaders", "actual_accumulation"], "score", 100, None),
    ("capital-flow.json",      "flow_macro", ["accumulating"],                              "score",      100, None),
    ("estimate-revisions.json","revision",   ["top_picks"],                                 "score",      100, None),
    ("analyst-actions.json",   "revision",   ["top_picks"],                                 "score",      40,  None),
    ("earnings-tracker.json",  "pead",       ["pead_signals"],                              "pead_score", 100, ("_eps_pos", True)),
    ("options-analytics.json", "options",    ["top_picks"],                                 "score",      100, ("direction", "long")),
    ("supply-chain-graph.json","linkage",    ["top_picks"],                                 "score",      100, ("direction", "long")),
    # ── added independent dimensions: a valuation edge (fundamental) + insider buying (flow) ──
    ("deep-value-overlap.json","value",      ["prime_setups", "elite_setups"],              "overlap_score", 10, None),
    ("insider-clusters.json",  "insider",    ["clusters"],                                  "n_insiders",  6,   None),
    ("convexity-scores.json",  "convexity",  ["scores"],                                    "convexity_score", 100, ("classification", "POSITIVE_GAMMA")),
]


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None

def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def family_status(scorecard_rows, alpha_proven, cands):
    """PROVEN if any candidate signal_type is alpha-proven; NEGATIVE if alpha-negative; else PROVISIONAL."""
    cset = set(c.lower() for c in cands)
    proven = set(str(x).lower() for x in (alpha_proven or []))
    if cset & proven:
        return "PROVEN"
    neg = False
    for r in scorecard_rows:
        st = str(r.get("signal_type", "")).lower()
        if st in cset:
            if r.get("alpha_status") == "ALPHA_PROVEN":
                return "PROVEN"
            if r.get("alpha_status") == "ALPHA_NEGATIVE":
                neg = True
    return "NEGATIVE" if neg else "PROVISIONAL"


def lambda_handler(event, context):
    t0 = time.time()
    diag = []

    # ── FDR gating inputs ──
    ea = _read("data/engine-alpha.json") or {}
    sc = _read("data/signal-scorecard.json") or {}
    alpha_proven = ea.get("alpha_proven_signals") or []
    sc_rows = sc.get("scorecard") or []

    fam_status = {}
    for fam, (sup, cands) in FAMILY_SUPER.items():
        fam_status[fam] = family_status(sc_rows, alpha_proven, cands)
    super_status = {}
    for sup in SUPERS:
        members = [f for f, (s, _) in FAMILY_SUPER.items() if s == sup]
        st = "PROVISIONAL"
        if any(fam_status[m] == "PROVEN" for m in members):
            st = "PROVEN"
        elif members and all(fam_status[m] == "NEGATIVE" for m in members):
            st = "NEGATIVE"
        super_status[sup] = {"status": st, "families": members,
                             "proven_members": [m for m in members if fam_status[m] == "PROVEN"]}
    proven_supers = [s for s in SUPERS if super_status[s]["status"] == "PROVEN"]
    mode = "PROVEN" if len(proven_supers) >= 2 else "PROVISIONAL"
    diag.append("family_status=%s" % fam_status)
    diag.append("proven_supers=%s mode=%s" % (proven_supers, mode))

    # ── collect bullish hits per ticker ──
    hits = {}   # ticker -> {family: {engine, strength}}
    engines_seen = []
    for fn, fam, lists, skey, sdiv, bull in SOURCES:
        d = _read("data/" + fn)
        if not d:
            diag.append("MISS %s" % fn); continue
        gen = str(d.get("generated_at", ""))[:10]
        engines_seen.append({"engine": fn[:-5], "family": fam, "asof": gen})
        n_here = 0
        for L in lists:
            for it in (d.get(L) or []):
                if not isinstance(it, dict):
                    continue
                tk = it.get("ticker") or it.get("symbol")
                if not tk:
                    continue
                tk = tk.upper()
                if bull:
                    bk, bv = bull
                    if bk == "_eps_pos":
                        eps = it.get("eps_surprise_pct")
                        if not (isinstance(eps, (int, float)) and eps > 0):
                            continue
                    elif str(it.get(bk, "")).lower() != str(bv).lower():
                        continue
                raw = it.get(skey)
                strength = clamp((raw / sdiv) if isinstance(raw, (int, float)) else 0.5)
                cur = hits.setdefault(tk, {})
                # keep the strongest read within a family
                if fam not in cur or strength > cur[fam]["strength"]:
                    cur[fam] = {"engine": fn[:-5], "strength": round(strength, 2)}
                n_here += 1
        diag.append("%s: %d bullish" % (fn[:-5], n_here))

    # ── score each ticker on INDEPENDENT super-family breadth ──
    book = []
    for tk, fams in hits.items():
        families_lit = list(fams.keys())
        supers_lit = sorted({FAMILY_SUPER[f][0] for f in families_lit})
        n_super = len(supers_lit)
        n_fam = len(families_lit)
        n_eff = n_super + 0.30 * (n_fam - n_super)            # extra correlated families add a little
        avg_strength = sum(v["strength"] for v in fams.values()) / n_fam
        composite = round(min(100, 22 * n_eff * (0.65 + 0.45 * avg_strength)), 1)
        # proven-mode score: independent PROVEN supers count full, others discounted
        proven_supers_hit = [s for s in supers_lit if super_status[s]["status"] == "PROVEN"]
        peff = sum(1.0 if super_status[s]["status"] == "PROVEN" else 0.35 for s in supers_lit) + 0.30 * (n_fam - n_super)
        proven_composite = round(min(100, 22 * peff * (0.65 + 0.45 * avg_strength)), 1)
        engines = [{"engine": v["engine"], "family": f, "super": FAMILY_SUPER[f][0],
                    "strength": v["strength"], "family_status": fam_status[f]} for f, v in fams.items()]
        engines.sort(key=lambda e: e["strength"], reverse=True)
        book.append({
            "ticker": tk, "n_super_families": n_super, "n_families": n_fam,
            "n_eff": round(n_eff, 2), "composite": composite,
            "super_families": supers_lit, "families": families_lit,
            "avg_strength": round(avg_strength, 2),
            "proven_super_families_hit": proven_supers_hit,
            "proven_composite": proven_composite,
            "engines": engines,
            "label": ("%d independent edge families" % n_super) + (" — all PROVISIONAL" if not proven_supers_hit else ""),
        })

    # provisional research book: rank by independent breadth then strength; require >=2 families to be "confluence"
    confluence = [b for b in book if b["n_families"] >= 2]
    confluence.sort(key=lambda b: (b["n_super_families"], b["composite"]), reverse=True)
    # proven book (GATED): only names lit across >=2 INDEPENDENT proven super-families
    proven_book = sorted([b for b in book if len(b["proven_super_families_hit"]) >= 2],
                         key=lambda b: b["proven_composite"], reverse=True)

    out = {
        "engine": "equity-confluence", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wl_research": __import__("wl_fusion").block(('BREADTH',)),
        "duration_s": round(time.time() - t0, 1),
        "mode": mode,
        "thesis": ("Names lit up by multiple INDEPENDENT edges at once — resilience, accumulation, revisions, "
                   "squeeze, options, supply-chain — are asymmetric in a way no single signal is. Scored on "
                   "independent factor-family breadth (correlation-adjusted), FDR-gated so the proven book only "
                   "activates when the forward scorecard actually proves the components."),
        "auto_activation": {
            "rule": ">=2 INDEPENDENT super-families must be ALPHA_PROVEN (FDR) for proven_book to activate",
            "active": mode == "PROVEN",
            "current_proven_super_families": proven_supers,
            "n_proven": len(proven_supers), "needed": 2,
            "note": ("Provisional research only — proven book empty until a 2nd independent family clears FDR. "
                     "Auto-activates with no manual switch." if mode == "PROVISIONAL"
                     else "ACTIVE — proven confluence book live."),
        },
        "family_status": fam_status,
        "super_family_status": super_status,
        "sources": engines_seen,
        "xray_map": {b["ticker"]: {"comp": b.get("composite_score", b.get("composite")),
                                   "fams": b.get("n_families"), "supers": b.get("n_super_families"),
                                   "top": (b.get("families") or b.get("family_list") or [])[:3]}
                     for b in book if b.get("ticker")},
        "confluence_book": confluence[:30],
        "proven_book": proven_book[:20],
        "counts": {
            "names_with_any_signal": len(book),
            "names_2plus_families": len(confluence),
            "names_3plus_super_families": sum(1 for b in confluence if b["n_super_families"] >= 3),
            "proven_book_size": len(proven_book),
        },
        "methodology": (
            "9 source engines collapsed into 9 families -> 4 INDEPENDENT super-families (technical: resilience/"
            "squeeze/boom; flow: dark-pool/flow-lookthrough/capital-flow; fundamental: estimate-revisions/"
            "analyst-actions/earnings-PEAD; structural: options-analytics/supply-chain). Within a family only the "
            "strongest read counts; correlated families inside a super-family give diminishing credit. Confluence "
            "= 22 * n_eff * (0.65 + 0.45*avg_strength), n_eff = #independent supers + 0.30*(extra families). "
            "FDR GATING: each family's status (PROVEN/NEGATIVE/PROVISIONAL) is read live from engine-alpha + "
            "signal-scorecard; proven_book requires >=2 independent PROVEN super-families and stays empty until "
            "then. No weights are learned from immature data. This is a VIEW over other engines (no harvester "
            "logging, to avoid circular grading). Research, not investment advice."),
        "disclaimer": "Research tool. Not investment advice. Confluence is provisional until the scorecard proves the components.",
        "diagnostics": diag[-12:],
    }
    try:
        _dr = _read("data/dollar-radar.json") or {}
        _rt = _dr.get("risk_transmission") or {}
        out["dollar_context"] = {
            "dollar_pressure": _dr.get("dollar_pressure"),
            "dollar_regime": _dr.get("regime"),
            "risk_transmission_score": _rt.get("score"),
            "risk_transmission_verdict": _rt.get("verdict"),
            "source": "justhodl-dollar-radar v2 dial (additive context; "
                      "scores untouched pending scorecard)"}
    except Exception as _e:
        print("[dollar-context] %s" % _e)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print("[equity-confluence v1] mode=%s proven_supers=%s | names any=%d 2+fam=%d 3+super=%d proven_book=%d" % (
        mode, proven_supers, out["counts"]["names_with_any_signal"], out["counts"]["names_2plus_families"],
        out["counts"]["names_3plus_super_families"], out["counts"]["proven_book_size"]))
    if confluence:
        print("  top confluence: " + ", ".join("%s(%dsup/%dfam,%s)" % (
            b["ticker"], b["n_super_families"], b["n_families"], b["composite"]) for b in confluence[:8]))
    return {"statusCode": 200, "body": json.dumps({"mode": mode, "confluence": len(confluence),
                                                    "proven_book": len(proven_book)})}
