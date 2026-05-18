"""
justhodl-desk-allocator -- the Multi-Strategy Capital Allocator.
================================================================
WHY THIS EXISTS
---------------
The platform grew five distinct STRATEGY DESKS, each producing its own
book every day:

  Best Ideas    long equity, cross-engine factor confluence
  Pairs Desk    market-neutral statistical arbitrage
  Trend Desk    cross-asset CTA / managed futures
  Merger-Arb    event-driven risk arbitrage
  Risk Radar    defensive / short fundamental-deterioration screen

Each desk answers "what do I want to own". Nothing answers the question
one level up -- "how much capital should each desk get". That is the
multi-manager capital-allocation problem, exactly how a pod shop
(Millennium, Citadel, Point72) runs: a central risk allocator sizes the
pods, it does not pick their trades.

The platform already has an `allocator` (asset CLASSES from regime
detectors), a `signal-board` (engine HEADLINES into one posture) and a
`pm-decision` (acts on the ACTUAL book). None of them sizes the desks.
This engine is that missing capstone layer.

THE METHOD -- a Bayesian shrinkage allocator
--------------------------------------------
Risk-parity needs a covariance matrix of desk returns, but three of
these desks are days old and have no track record. A real shop sizes a
new pod off a PRIOR -- the strategy archetype's documented long-run
characteristics -- and blends toward realized stats as history builds.

  1. Desk health gate   each desk JSON is checked for freshness and for
                        whether it is actually producing positions. A
                        stale desk is OFFLINE (zero capital); an empty
                        desk is DRY (half weight).
  2. Effective vol      shrink(prior_vol, realized_vol, N) with prior
                        weight K trading days. Realized desk-return
                        history is Phase 2 -- until a desk-return feed
                        exists N=0 and the documented archetype priors
                        govern. The machinery is built and dormant, the
                        same pattern the opportunity-calibrator ships
                        with.
  3. Inverse-vol parity base weight proportional to 1 / effective_vol --
                        the risk-parity backbone that equalises each
                        desk's risk contribution.
  4. Regime tilt        signal-board composite + crisis-composite score
                        blend into one risk axis. Each desk carries a
                        risk_beta (how it wants the world): in risk-off
                        the trend and defensive desks are boosted and
                        the long book is cut; in risk-on the reverse.
  5. Cap + normalise    no desk above MAX_DESK_W; weights sum to 100%.
  6. Firm aggregation   the desks' archetype betas and a prior
                        cross-desk correlation matrix roll up into the
                        firm-level net equity beta, an estimated
                        portfolio volatility and the DIVERSIFICATION
                        RATIO -- the headline number that proves the
                        multi-desk structure is actually buying
                        decorrelation.

Reads only existing S3 sidecars. Builds nothing redundant. Snapshots
its own decision history so allocation drift is auditable and the
realized-stat layer can warm up later.

OUTPUT    data/desk-allocator.json          SCHEDULE  daily 00:30 UTC
"""
import json
import math
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/desk-allocator.json"
HISTORY_KEY = "data/desk-allocator-history.json"
SCHEMA = "1.0"

s3 = boto3.client("s3", region_name="us-east-1")

# ---- tuning constants ------------------------------------------------------
SHRINK_K = 60          # prior weight, in trading days, for vol shrinkage
TILT_GAIN = 0.35       # how hard the regime tilt pushes desk weights
TILT_LO, TILT_HI = 0.45, 1.70   # regime multiplier clamp
MAX_DESK_W = 0.45      # concentration cap -- no desk above 45% of capital
STALE_HOURS = 48       # desk sidecar older than this -> OFFLINE
DRY_MULT = 0.50        # a live-but-empty desk runs at half weight
HISTORY_CAP = 400      # snapshots retained

# ---- desk registry: archetype priors --------------------------------------
# prior_vol / equity_beta / prior_sharpe are documented long-run
# characteristics of each strategy archetype (the HFRI / SG CTA / SG
# Merger Arb index families). They are the cold-start Bayesian prior,
# clearly labelled as priors, and are blended out as realized desk-return
# history accumulates. risk_beta is how the desk wants the macro world:
# +1 loves risk-on, -1 loves risk-off, 0 is regime-agnostic.
DESKS = [
    {
        "key": "best-ideas", "name": "Best Ideas",
        "json_key": "data/best-ideas.json",
        "archetype": "Long equity - cross-engine factor confluence",
        "prior_vol": 0.16, "equity_beta": 1.05,
        "risk_beta": 1.00, "prior_sharpe": 0.70,
        "count_keys": ["n_total"], "count_arrays": ["stack"],
    },
    {
        "key": "pairs-arb", "name": "Pairs Desk",
        "json_key": "data/pairs-arb.json",
        "archetype": "Market-neutral statistical arbitrage",
        "prior_vol": 0.07, "equity_beta": 0.05,
        "risk_beta": 0.00, "prior_sharpe": 0.80,
        "count_keys": ["summary.n_tradeable"], "count_arrays": ["pairs"],
    },
    {
        "key": "trend-engine", "name": "Trend Desk",
        "json_key": "data/trend-engine.json",
        "archetype": "Cross-asset CTA / managed futures",
        "prior_vol": 0.11, "equity_beta": -0.10,
        "risk_beta": -0.50, "prior_sharpe": 0.55,
        "count_keys": ["summary.n_long", "summary.n_short"],
        "count_arrays": ["positions"],
    },
    {
        "key": "merger-arb", "name": "Merger-Arb Desk",
        "json_key": "data/merger-arb.json",
        "archetype": "Event-driven risk arbitrage",
        "prior_vol": 0.06, "equity_beta": 0.15,
        "risk_beta": 0.25, "prior_sharpe": 0.85,
        "count_keys": ["summary.n_priced", "summary.priced"],
        "count_arrays": ["all_priced", "tight_carry"],
    },
    {
        "key": "risk-radar", "name": "Risk Radar",
        "json_key": "data/risk-radar.json",
        "archetype": "Defensive - fundamental-deterioration screen",
        "prior_vol": 0.13, "equity_beta": -0.55,
        "risk_beta": -1.00, "prior_sharpe": 0.40,
        "count_keys": ["n_carried"], "count_arrays": ["stack"],
    },
]

# prior cross-desk correlation matrix, same order as DESKS above. These
# are long-run archetype relationships -- the long book and the short /
# defensive book move opposite (-0.60), trend decorrelates from equities
# (-0.10), pairs is near-zero to everything. Used only for the firm-level
# portfolio-vol and diversification-ratio roll-up, never for sizing, and
# labelled a prior in the output.
DESK_ORDER = ["best-ideas", "pairs-arb", "trend-engine",
              "merger-arb", "risk-radar"]
PRIOR_CORR = {
    ("best-ideas", "best-ideas"): 1.00, ("pairs-arb", "pairs-arb"): 1.00,
    ("trend-engine", "trend-engine"): 1.00, ("merger-arb", "merger-arb"): 1.00,
    ("risk-radar", "risk-radar"): 1.00,
    ("best-ideas", "pairs-arb"): 0.20, ("best-ideas", "trend-engine"): -0.10,
    ("best-ideas", "merger-arb"): 0.30, ("best-ideas", "risk-radar"): -0.60,
    ("pairs-arb", "trend-engine"): 0.00, ("pairs-arb", "merger-arb"): 0.15,
    ("pairs-arb", "risk-radar"): -0.10, ("trend-engine", "merger-arb"): -0.05,
    ("trend-engine", "risk-radar"): 0.15, ("merger-arb", "risk-radar"): -0.25,
}


def corr(a, b):
    if (a, b) in PRIOR_CORR:
        return PRIOR_CORR[(a, b)]
    return PRIOR_CORR.get((b, a), 0.0)


# ---- helpers ---------------------------------------------------------------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def get_json(key):
    """Fetch and parse an S3 JSON sidecar; None if missing or unreadable."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def dig(d, dotted):
    """Resolve a dotted path like 'summary.n_tradeable' against a dict."""
    cur = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def parse_ts(d):
    """Best-effort extract of a UTC datetime from a desk sidecar."""
    if not isinstance(d, dict):
        return None
    for field in ("generated_at", "generatedAt", "ts", "timestamp"):
        v = d.get(field)
        if not v:
            continue
        s = str(v).strip().replace("Z", "+00:00")
        for fmt in (None, "%Y-%m-%d %H:%M UTC", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S"):
            try:
                if fmt is None:
                    dt = datetime.fromisoformat(s)
                else:
                    dt = datetime.strptime(str(v).strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
    return None


def count_active(spec, d):
    """How many live positions / ideas the desk is carrying right now."""
    if not isinstance(d, dict):
        return 0
    total = 0
    got = False
    for ck in spec.get("count_keys", []):
        v = dig(d, ck)
        if isinstance(v, (int, float)):
            total += int(v)
            got = True
    if got:
        return total
    for ak in spec.get("count_arrays", []):
        v = d.get(ak)
        if isinstance(v, list):
            return len(v)
    return 0


def shrink(prior_vol, realized_vol, n):
    """Bayesian shrink of realized vol toward the archetype prior."""
    if realized_vol is None or n <= 0:
        return prior_vol, 0
    w_real = n / (n + SHRINK_K)
    return (1 - w_real) * prior_vol + w_real * realized_vol, n


def realized_desk_vol(history, key):
    """Realized volatility of a desk's return series from snapshot history.

    Phase 2 hook: snapshots do not yet carry a per-desk realized return
    ('ret'), so this returns (None, 0) and the documented archetype prior
    governs. The instant a desk-return feed is wired into the snapshot
    this warms up automatically -- no allocator change needed.
    """
    rets = []
    for snap in history.get("snapshots", []):
        dk = (snap.get("desks") or {}).get(key) or {}
        r = dk.get("ret")
        if isinstance(r, (int, float)):
            rets.append(float(r))
    if len(rets) < 20:
        return None, len(rets)
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252), len(rets)


# ---- regime read -----------------------------------------------------------
def read_regime():
    """Blend signal-board posture and crisis-composite into one risk axis.

    Returns a dict with the blended risk axis in [-1, +1]: positive is
    risk-on, negative is risk-off.
    """
    sb = get_json("data/signal-board.json")
    cc = get_json("data/crisis-composite.json")

    sb_comp = None
    sb_posture = "UNAVAILABLE"
    if isinstance(sb, dict):
        sb_comp = sb.get("composite_signal")
        sb_posture = sb.get("composite_posture") or "UNAVAILABLE"
    sb_norm = clamp((sb_comp or 0.0) / 2.0, -1.0, 1.0) if sb_comp is not None \
        else None

    crisis_score = None
    defcon = None
    if isinstance(cc, dict):
        crisis_score = cc.get("master_crisis_score")
        defcon = cc.get("defcon_level")
    # crisis 0..100 (higher = worse) -> risk axis: 50 is neutral
    crisis_norm = clamp(-((crisis_score or 50.0) - 50.0) / 50.0, -1.0, 1.0) \
        if crisis_score is not None else None

    parts, wts = [], []
    if sb_norm is not None:
        parts.append(sb_norm)
        wts.append(0.60)
    if crisis_norm is not None:
        parts.append(crisis_norm)
        wts.append(0.40)
    if parts:
        blended = sum(p * w for p, w in zip(parts, wts)) / sum(wts)
    else:
        blended = 0.0

    if blended >= 0.45:
        label = "RISK-ON"
    elif blended >= 0.12:
        label = "MILDLY RISK-ON"
    elif blended > -0.12:
        label = "NEUTRAL / MIXED"
    elif blended > -0.45:
        label = "MILDLY RISK-OFF"
    else:
        label = "RISK-OFF"

    return {
        "blended_risk_axis": round(blended, 3),
        "label": label,
        "signal_board_posture": sb_posture,
        "signal_board_composite": sb_comp,
        "crisis_score": crisis_score,
        "defcon_level": defcon,
        "inputs_available": len(parts),
    }


# ---- core allocation -------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    history = get_json(HISTORY_KEY) or {"schema": SCHEMA, "snapshots": []}
    if not isinstance(history.get("snapshots"), list):
        history["snapshots"] = []

    regime = read_regime()
    risk_axis = regime["blended_risk_axis"]

    # ---- step 1-3: load each desk, health-gate, effective vol ----
    rows = []
    for spec in DESKS:
        d = get_json(spec["json_key"])
        ts = parse_ts(d)
        fresh_h = None
        if ts is not None:
            fresh_h = round((now - ts).total_seconds() / 3600.0, 1)
        active = count_active(spec, d) if d is not None else 0

        if d is None or fresh_h is None:
            status = "OFFLINE"
        elif fresh_h > STALE_HOURS:
            status = "OFFLINE"
        elif active <= 0:
            status = "DRY"
        else:
            status = "FIRING"

        rvol, rn = realized_desk_vol(history, spec["key"])
        eff_vol, used_n = shrink(spec["prior_vol"], rvol, rn)

        rows.append({
            "key": spec["key"], "name": spec["name"],
            "archetype": spec["archetype"], "json_key": spec["json_key"],
            "status": status, "freshness_hours": fresh_h,
            "active_count": active,
            "prior_vol": spec["prior_vol"],
            "realized_vol": round(rvol, 4) if rvol is not None else None,
            "realized_n": rn,
            "effective_vol": round(eff_vol, 4),
            "equity_beta": spec["equity_beta"],
            "risk_beta": spec["risk_beta"],
            "prior_sharpe": spec["prior_sharpe"],
            "_eff_vol": eff_vol,
        })

    # ---- step 4: regime tilt ----
    for r in rows:
        mult = 1.0 + TILT_GAIN * risk_axis * r["risk_beta"]
        r["regime_mult"] = round(clamp(mult, TILT_LO, TILT_HI), 3)

    # ---- step 5: inverse-vol parity x regime x health, cap, normalise ----
    for r in rows:
        if r["status"] == "OFFLINE":
            r["_health"] = 0.0
        elif r["status"] == "DRY":
            r["_health"] = DRY_MULT
        else:
            r["_health"] = 1.0
        inv_vol = 1.0 / r["_eff_vol"] if r["_eff_vol"] > 1e-6 else 0.0
        r["_raw"] = inv_vol * r["regime_mult"] * r["_health"]

    raw_sum = sum(r["_raw"] for r in rows)
    if raw_sum <= 0:
        # every desk offline -- nothing to allocate
        for r in rows:
            r["capital_weight_pct"] = 0.0
    else:
        for r in rows:
            r["_w"] = r["_raw"] / raw_sum
        # iterative concentration cap: pin any desk over MAX_DESK_W and
        # redistribute the remainder across the uncapped desks pro-rata
        for _ in range(8):
            over = [r for r in rows if r.get("_w", 0) > MAX_DESK_W + 1e-9]
            if not over:
                break
            for r in over:
                r["_w"] = MAX_DESK_W
            capped = sum(r["_w"] for r in rows
                         if r["_w"] >= MAX_DESK_W - 1e-9)
            free = [r for r in rows if r["_w"] < MAX_DESK_W - 1e-9
                    and r["_raw"] > 0]
            free_raw = sum(r["_raw"] for r in free)
            budget = 1.0 - capped
            if free_raw <= 0 or budget <= 0:
                break
            for r in free:
                r["_w"] = budget * r["_raw"] / free_raw
        tot = sum(r.get("_w", 0) for r in rows)
        for r in rows:
            r["capital_weight_pct"] = round(
                100.0 * r.get("_w", 0) / tot if tot > 0 else 0.0, 2)

    # ---- step 6: firm-level aggregation ----
    weights = {r["key"]: r.get("capital_weight_pct", 0.0) / 100.0
               for r in rows}
    vols = {r["key"]: r["_eff_vol"] for r in rows}

    net_beta = sum(weights[r["key"]] * r["equity_beta"] for r in rows)
    wavg_vol = sum(weights[r["key"]] * vols[r["key"]] for r in rows)

    # portfolio variance with the prior correlation matrix
    port_var = 0.0
    for a in DESK_ORDER:
        for b in DESK_ORDER:
            port_var += (weights.get(a, 0) * weights.get(b, 0)
                         * vols.get(a, 0) * vols.get(b, 0) * corr(a, b))
    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0
    div_ratio = (wavg_vol / port_vol) if port_vol > 1e-9 else None

    active_desks = [r for r in rows if r["status"] != "OFFLINE"]
    firing = [r for r in rows if r["status"] == "FIRING"]
    dominant = max(rows, key=lambda r: r.get("capital_weight_pct", 0)) \
        if rows else None

    # ---- per-desk explanation notes ----
    for r in rows:
        notes = []
        if r["status"] == "OFFLINE":
            why = "no sidecar" if r["freshness_hours"] is None \
                else "sidecar %.0fh stale" % r["freshness_hours"]
            notes.append("OFFLINE (%s) - zero capital until the desk "
                         "reports again." % why)
        elif r["status"] == "DRY":
            notes.append("Live but carrying no positions today - capital "
                         "halved, not zeroed.")
        else:
            notes.append("Firing: %d live position(s)." % r["active_count"])
        notes.append("Sized inverse to a %.0f%% effective vol (archetype "
                     "prior%s)." % (
                         r["effective_vol"] * 100,
                         "" if r["realized_n"] == 0
                         else ", %dd realized blended in" % r["realized_n"]))
        if r["regime_mult"] > 1.03:
            notes.append("Regime tilt +%.0f%%: this archetype is favoured "
                         "in the current %s tape."
                         % ((r["regime_mult"] - 1) * 100, regime["label"]))
        elif r["regime_mult"] < 0.97:
            notes.append("Regime tilt %.0f%%: this archetype is cut in the "
                         "current %s tape."
                         % ((r["regime_mult"] - 1) * 100, regime["label"]))
        else:
            notes.append("Regime-neutral archetype - no tilt applied.")
        r["notes"] = notes

    # ---- headline ----
    if not active_desks:
        headline = ("All five strategy desks are OFFLINE - no desk has "
                    "reported a fresh book. No capital allocated.")
    else:
        top = sorted(rows, key=lambda r: -r.get("capital_weight_pct", 0))[:2]
        top_txt = ", ".join("%s %.0f%%" % (t["name"],
                                           t["capital_weight_pct"])
                            for t in top)
        dr_txt = ("diversification ratio %.2f" % div_ratio) \
            if div_ratio else "diversification ratio n/a"
        headline = ("%d of 5 desks firing. Capital concentrates in %s. "
                    "Firm net equity beta %+.2f, %s. Tape: %s."
                    % (len(firing), top_txt, net_beta, dr_txt,
                       regime["label"]))

    # ---- assemble output rows (drop scratch fields) ----
    out_desks = []
    for r in rows:
        out_desks.append({
            "key": r["key"], "name": r["name"], "archetype": r["archetype"],
            "json_key": r["json_key"], "status": r["status"],
            "freshness_hours": r["freshness_hours"],
            "active_count": r["active_count"],
            "prior_vol_pct": round(r["prior_vol"] * 100, 1),
            "effective_vol_pct": round(r["effective_vol"] * 100, 1),
            "realized_vol_pct": (round(r["realized_vol"] * 100, 1)
                                 if r["realized_vol"] is not None else None),
            "realized_n": r["realized_n"],
            "equity_beta": r["equity_beta"],
            "risk_beta": r["risk_beta"],
            "prior_sharpe": r["prior_sharpe"],
            "regime_mult": r["regime_mult"],
            "capital_weight_pct": r["capital_weight_pct"],
            "notes": r["notes"],
        })
    out_desks.sort(key=lambda r: -r["capital_weight_pct"])

    payload = {
        "schema_version": SCHEMA,
        "engine": "justhodl-desk-allocator",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "headline": headline,
        "regime": regime,
        "desks": out_desks,
        "firm": {
            "desks_total": len(rows),
            "desks_firing": len(firing),
            "desks_offline": len(rows) - len(active_desks),
            "net_equity_beta": round(net_beta, 3),
            "weighted_avg_desk_vol_pct": round(wavg_vol * 100, 2),
            "portfolio_vol_est_pct": round(port_vol * 100, 2),
            "diversification_ratio": (round(div_ratio, 3)
                                      if div_ratio else None),
            "dominant_desk": dominant["name"] if dominant else None,
        },
        "history_days": len(history["snapshots"]),
        "parameters": {
            "shrink_k_days": SHRINK_K,
            "regime_tilt_gain": TILT_GAIN,
            "max_desk_weight_pct": MAX_DESK_W * 100,
            "stale_hours": STALE_HOURS,
            "dry_desk_multiplier": DRY_MULT,
        },
        "how_to_read": (
            "Each strategy desk is treated as a pod. Capital is split by "
            "inverse-volatility risk parity so every desk contributes a "
            "similar share of risk, then tilted by the macro regime - the "
            "trend and defensive desks gain weight when the tape turns "
            "risk-off, the long book gains in risk-on. A desk with a stale "
            "or missing sidecar is OFFLINE and gets nothing; a live desk "
            "with no positions is DRY and runs at half weight. The "
            "diversification ratio is the headline - the higher above 1.0, "
            "the more the multi-desk structure is cutting portfolio risk "
            "below the weighted average of the desks standing alone."),
        "methodology": (
            "Bayesian shrinkage allocator. Effective desk volatility = "
            "shrink(archetype prior, realized, N) with prior weight K=60 "
            "trading days. Realized desk-return history is Phase 2: until a "
            "per-desk return feed is wired N=0 and the documented archetype "
            "priors (HFRI / SG CTA / SG Merger Arb index families) govern - "
            "the shrinkage machinery is built and dormant. Base weight is "
            "1/effective_vol; the regime multiplier is "
            "1 + 0.35 * risk_axis * desk_risk_beta clamped to [0.45, 1.70]; "
            "the risk axis blends signal-board's composite (60%) and "
            "crisis-composite's score (40%). A 45% per-desk cap is applied "
            "with pro-rata redistribution. Firm net beta and the "
            "diversification ratio roll up through a prior cross-desk "
            "correlation matrix. The allocator sizes the desks; it never "
            "picks their trades."),
        "disclaimer": (
            "Research and education only - not investment advice. Capital "
            "weights are a risk-budgeting framework, not a directive. "
            "Archetype priors are long-run averages and will differ from "
            "any single desk's realized behaviour."),
    }

    # ---- write output ----
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUT_KEY,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")

    # ---- snapshot decision history (auditable allocation trail) ----
    snap = {
        "date": now.strftime("%Y-%m-%d"),
        "ts": now.isoformat(),
        "risk_axis": risk_axis,
        "regime_label": regime["label"],
        "signal_board_composite": regime["signal_board_composite"],
        "crisis_score": regime["crisis_score"],
        "desks": {
            r["key"]: {
                "status": r["status"],
                "active_count": r["active_count"],
                "weight": r["capital_weight_pct"],
                "effective_vol": round(r["effective_vol"], 4),
                # 'ret' intentionally absent -- Phase 2 hook
            } for r in rows
        },
        "firm": {
            "net_equity_beta": round(net_beta, 3),
            "portfolio_vol_est": round(port_vol * 100, 2),
            "diversification_ratio": (round(div_ratio, 3)
                                      if div_ratio else None),
        },
    }
    snaps = [x for x in history["snapshots"]
             if x.get("date") != snap["date"]]
    snaps.append(snap)
    history["snapshots"] = snaps[-HISTORY_CAP:]
    history["schema"] = SCHEMA
    history["updated_at"] = now.isoformat()
    s3.put_object(
        Bucket=S3_BUCKET, Key=HISTORY_KEY,
        Body=json.dumps(history, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")

    print("[desk-allocator] %s | firing=%d/%d net_beta=%+.2f div=%s"
          % (regime["label"], len(firing), len(rows), net_beta,
             ("%.2f" % div_ratio) if div_ratio else "n/a"))
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "desks_firing": len(firing),
            "desks_total": len(rows),
            "net_equity_beta": round(net_beta, 3),
            "diversification_ratio": (round(div_ratio, 3)
                                      if div_ratio else None),
            "headline": headline,
        }),
    }
