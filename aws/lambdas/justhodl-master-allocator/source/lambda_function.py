"""
justhodl-master-allocator -- the institutional Capital Allocation
Decision Engine.

Bridges the entire signal universe to a single, accountable, top-of-
firm portfolio target. Reads:

  - GSI                  data/global-stress.json
  - Crisis composite     data/crisis-composite.json
  - Cross-asset regime   data/cross-asset-regime.json
  - Dollar Radar         data/dollar-radar.json
  - Signal Board         data/signal-board.json
  - Macro Nowcast        data/macro-nowcast.json
  - Vol Regime           data/vol-regime.json
  - Eurodollar Stress    data/eurodollar-stress.json
  - Sentiment            data/sentiment.json (graceful if absent)

  - Fleet IC weights from SSM /justhodl/calibration-fleet/weights so
    each signal's influence on the final allocation is proportional to
    its empirical predictive power, not a static prior. The allocator
    falls back to equal weights if the SSM key is absent or thin.

Architecture:

  1. STRATEGIC BENCHMARK -- a configurable hedge-fund-style strategic
     allocation (default: 40% US eq / 15% intl eq / 5% EM / 15% UST
     short / 10% UST long / 5% IG / 5% gold / 2% BTC / 3% cash).

  2. PER-SIGNAL TILT FUNCTIONS -- each signal proposes a bounded tilt
     vector across asset classes. Tilts are signed: positive
     overweight, negative underweight. Magnitude is capped so no
     single signal can dominate.

  3. IC-WEIGHTED AGGREGATION -- the proposed tilts are blended,
     weighted by each signal's IC from the fleet calibrator (with a
     floor so signals with thin IC still contribute their priors).

  4. POSITION LIMITS -- each asset class has a hard (min, max) range;
     deviations beyond are clipped and the residual redistributed.

  5. ACTIVE RISK BUDGET -- the aggregate tracking error to benchmark
     is capped at MAX_ACTIVE_BPS (default 800bp). Tilts shrink
     proportionally if total deviation exceeds budget.

  6. RATIONALE -- per-asset, the engine reports which signals
     contributed how much. The allocation is fully auditable.

Output: data/master-allocation.json + SSM
/justhodl/master-allocation/target (machine-readable target for
downstream execution / position-sizer consumers).
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/master-allocation.json"
TARGET_PARAM = "/justhodl/master-allocation/target"
FLEET_WEIGHTS_PARAM = "/justhodl/calibration-fleet/weights"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ---- Strategic benchmark (configurable) ---------------------------------
BENCHMARK = {
    "us_equity":      40.0,
    "intl_dev_eq":    15.0,
    "em_equity":       5.0,
    "ust_short":      15.0,
    "ust_long":       10.0,
    "ig_credit":       5.0,
    "gold":            5.0,
    "btc":             2.0,
    "cash":            3.0,
}
ASSETS = list(BENCHMARK.keys())
ASSET_LABELS = {
    "us_equity":   "US Equity (SPY)",
    "intl_dev_eq": "Intl Developed Equity (EFA)",
    "em_equity":   "Emerging Markets Equity (EEM)",
    "ust_short":   "US Treasuries -- short (SHY)",
    "ust_long":    "US Treasuries -- long (TLT)",
    "ig_credit":   "Investment-Grade Credit (LQD)",
    "gold":        "Gold (GLD)",
    "btc":         "Bitcoin (BTC)",
    "cash":        "USD Cash (BIL)",
}

# ---- Position limits (min, max) ------------------------------------------
POSITION_LIMITS = {
    "us_equity":   (15.0, 60.0),
    "intl_dev_eq": ( 5.0, 25.0),
    "em_equity":   ( 0.0, 12.0),
    "ust_short":   ( 5.0, 35.0),
    "ust_long":    ( 0.0, 25.0),
    "ig_credit":   ( 0.0, 15.0),
    "gold":        ( 0.0, 15.0),
    "btc":         ( 0.0,  8.0),
    "cash":        ( 0.0, 30.0),
}
MAX_ACTIVE_BPS = 800.0   # cap on sum |delta| in pp

# ---- Per-signal tilt magnitudes (bounded) --------------------------------
# Each tilt vector is the MAX magnitude across asset classes that signal
# can move the allocation when it's at its extreme. Aggregation combines
# multiple signals' proposals.
TILT_MAGNITUDES = {
    "gsi":              {"us_equity": -10.0, "intl_dev_eq": -4.0,
                         "em_equity": -3.0, "ust_long": +6.0,
                         "ust_short": +4.0, "ig_credit": -2.0,
                         "gold": +5.0, "btc": -2.0, "cash": +6.0},
    "crisis_composite": {"us_equity": -8.0, "intl_dev_eq": -3.0,
                         "em_equity": -3.0, "ust_long": +4.0,
                         "ust_short": +5.0, "ig_credit": -3.0,
                         "gold": +4.0, "btc": -2.0, "cash": +6.0},
    "dollar_radar":     {"us_equity": 0.0, "intl_dev_eq": -3.0,
                         "em_equity": -5.0, "ust_long": +1.0,
                         "ust_short": +2.0, "ig_credit": 0.0,
                         "gold": -3.0, "btc": -2.0, "cash": +4.0},
    "signal_board":     {"us_equity": -7.0, "intl_dev_eq": -2.0,
                         "em_equity": -2.0, "ust_long": +3.0,
                         "ust_short": +3.0, "ig_credit": -1.0,
                         "gold": +3.0, "btc": -1.0, "cash": +4.0},
    "vol_regime":       {"us_equity": -5.0, "intl_dev_eq": -2.0,
                         "em_equity": -1.0, "ust_long": +3.0,
                         "ust_short": +2.0, "ig_credit": -1.0,
                         "gold": +2.0, "btc": -1.0, "cash": +3.0},
    "eurodollar_stress":{"us_equity": -3.0, "intl_dev_eq": -2.0,
                         "em_equity": -4.0, "ust_long": +2.0,
                         "ust_short": +4.0, "ig_credit": -2.0,
                         "gold": +2.0, "btc": -1.0, "cash": +4.0},
    "regime":           {"us_equity": -5.0, "intl_dev_eq": -2.0,
                         "em_equity": -3.0, "ust_long": +3.0,
                         "ust_short": +2.0, "ig_credit": -1.0,
                         "gold": +3.0, "btc": -2.0, "cash": +5.0},
    "macro_nowcast":    {"us_equity": -6.0, "intl_dev_eq": -3.0,
                         "em_equity": -4.0, "ust_long": +4.0,
                         "ust_short": +3.0, "ig_credit": -2.0,
                         "gold": +3.0, "btc": -1.0, "cash": +6.0},
    # Sleeve-specific cross-asset FLOW signals (orthogonal to the
    # risk-off composites above). gold_rotation tilts ONLY the gold
    # sleeve by gold's own momentum, so the allocator stops chasing a
    # falling metal that the risk-off signals would otherwise overweight.
    # foreign_bond_demand tilts the UST sleeve by actual foreign (TIC)
    # demand for Treasuries -- a structural bond driver none of the
    # composites capture. Intensity sign here is asset-direction (not
    # risk-off): + = own that sleeve more.
    "gold_rotation":    {"gold": +8.0},
    "foreign_bond_demand": {"ust_long": +4.0, "ust_short": +2.0,
                            "cash": -1.0},
    # brain_posture applies the user's OWN distilled macro worldview as a slow
    # lens (KNOWLEDGE, not tickers). A defensive macro lean tilts toward
    # cash/gold/USTs and away from beta; aggressive/risk-on tilts the other way.
    "brain_posture":    {"us_equity": -4.0, "intl_dev_eq": -2.0,
                         "em_equity": -2.0, "ust_long": +2.0,
                         "ust_short": +2.0, "ig_credit": -1.0,
                         "gold": +3.0, "btc": -1.0, "cash": +3.0},
}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


# ============== helpers ==================================================
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return {}


def deep_get(obj, path, default=None):
    cur = obj
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def load_ic_weights():
    """Pull per-engine IC weights from the fleet calibrator's SSM key.
    Returns dict {engine_name: ic_value} -- engines without IC just get
    None and fall back to equal weighting in the aggregation."""
    try:
        p = ssm.get_parameter(Name=FLEET_WEIGHTS_PARAM)
        payload = json.loads(p["Parameter"]["Value"])
        return payload.get("ic") or {}
    except Exception:
        return {}


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML"}).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}),
            timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


# ============== signal state extraction ==================================
def gather_signals():
    """Read every input the allocator consumes. Each signal returns
    {value, intensity, source, label}. Intensity is -1.0..+1.0 -- the
    sign tells us how aggressive the bearish/bullish tilt is (+1.0 =
    full bearish, equity-OFF; -1.0 = full bullish, equity-ON; 0 =
    neutral). Missing signals return None and are skipped."""
    out = {}

    # GSI -- map 0-100 stress to -1..+1 (35 neutral, 75+ -> +1)
    gs = read_json("data/global-stress.json")
    gsi = gs.get("global_stress_index")
    if isinstance(gsi, (int, float)):
        intensity = clamp((gsi - 35) / 40.0, -1.0, 1.0)
        out["gsi"] = {"value": gsi, "intensity": intensity,
                      "label": "Global Stress Index",
                      "level": gs.get("global_stress_level")}

    # Crisis composite -- 0-100 master_crisis_score
    cc = read_json("data/crisis-composite.json")
    crisis = cc.get("master_crisis_score") or cc.get("composite_score")
    if isinstance(crisis, (int, float)):
        intensity = clamp((crisis - 35) / 40.0, -1.0, 1.0)
        out["crisis_composite"] = {"value": crisis, "intensity": intensity,
                                   "label": "Crisis Composite",
                                   "regime": cc.get("regime")
                                   or cc.get("severity")}

    # Dollar Radar -- -100..+100 dollar_pressure (PUMP positive)
    dr = read_json("data/dollar-radar.json")
    dp = dr.get("dollar_pressure")
    if isinstance(dp, (int, float)):
        # positive (PUMP) -> negative for EM, positive for cash
        intensity = clamp(dp / 60.0, -1.0, 1.0)
        out["dollar_radar"] = {"value": dp, "intensity": intensity,
                               "label": "Dollar Radar",
                               "regime": dr.get("regime")}

    # Signal Board composite
    sb = read_json("data/signal-board.json")
    sbv = sb.get("composite") or sb.get("composite_score")
    if isinstance(sbv, (int, float)):
        intensity = clamp((sbv - 35) / 40.0, -1.0, 1.0)
        out["signal_board"] = {"value": sbv, "intensity": intensity,
                               "label": "Signal Board",
                               "regime": sb.get("posture")
                               or sb.get("regime")}

    # Vol Regime
    vr = read_json("data/vol-regime.json")
    vrv = vr.get("score") or vr.get("composite_score")
    if isinstance(vrv, (int, float)):
        intensity = clamp((vrv - 35) / 40.0, -1.0, 1.0)
        out["vol_regime"] = {"value": vrv, "intensity": intensity,
                             "label": "Vol Regime",
                             "regime": vr.get("regime")}

    # Eurodollar Stress (funding plumbing)
    es = read_json("data/eurodollar-stress.json")
    esv = es.get("composite_score")
    if isinstance(esv, (int, float)):
        intensity = clamp((esv - 30) / 50.0, -1.0, 1.0)
        out["eurodollar_stress"] = {"value": esv, "intensity": intensity,
                                    "label": "Eurodollar Stress",
                                    "regime": es.get("severity")
                                    or es.get("regime")}

    # Cross-asset regime -- if the engine writes a numeric stress proxy
    car = read_json("data/cross-asset-regime.json")
    carv = (car.get("score") or car.get("composite_score")
            or car.get("stress_score"))
    if isinstance(carv, (int, float)):
        intensity = clamp((carv - 35) / 40.0, -1.0, 1.0)
        out["regime"] = {"value": carv, "intensity": intensity,
                         "label": "Cross-Asset Regime",
                         "regime": car.get("regime")}

    # Macro nowcast -- expecting a recession-probability-style score
    mn = read_json("data/macro-nowcast.json")
    mnv = (mn.get("recession_probability") or mn.get("score")
           or mn.get("composite_score"))
    if isinstance(mnv, (int, float)):
        # interpret 0-100 with 35 neutral; >65 strong recession risk
        intensity = clamp((mnv - 35) / 40.0, -1.0, 1.0)
        out["macro_nowcast"] = {"value": mnv, "intensity": intensity,
                                "label": "Macro Nowcast",
                                "regime": mn.get("regime")
                                or mn.get("phase")}

    # Gold / metals rotation -- directly informs the GOLD sleeve. None of
    # the risk-off composites check whether gold is actually working;
    # this tilts gold by its own 20d momentum so the allocator does not
    # chase a falling metal. Intensity here is gold-direction (+ = gold up).
    ger = read_json("data/gold-equity-rotation.json")
    gm = ger.get("current_metrics") or {}
    moms = [x for x in (gm.get("gld_20d_pct"), gm.get("gdx_20d_pct"))
            if isinstance(x, (int, float))]
    if moms:
        gold_mom = sum(moms) / len(moms)
        out["gold_rotation"] = {"value": round(gold_mom, 2),
                                "intensity": clamp(gold_mom / 8.0, -1.0, 1.0),
                                "label": "Gold/Metals Rotation",
                                "regime": ger.get("state")}

    # Foreign cross-border demand for US Treasuries (TIC) -- a structural
    # bond-sleeve driver not captured by the risk-off composites. Strong
    # foreign bid for USTs relative to its 12mo run-rate supports the UST
    # weight. Intensity is UST-direction (+ = foreign accumulating).
    ci = read_json("data/capital-inflows.json")
    ust = (ci.get("by_asset_class") or {}).get("treasuries") or {}
    lm, r12 = ust.get("latest_month_b"), ust.get("rolling_12mo_b")
    if isinstance(lm, (int, float)) and isinstance(r12, (int, float)) and r12:
        run_rate_m = r12 / 12.0
        out["foreign_bond_demand"] = {
            "value": round(lm, 1),
            "intensity": clamp((lm - run_rate_m) / max(15.0, abs(run_rate_m)),
                               -1.0, 1.0),
            "label": "Foreign UST Demand (TIC)",
            "regime": ci.get("regime")}

    # Brain macro posture -- the user's distilled worldview as a slow macro lens.
    # KNOWLEDGE only: read the directive's macro risk_posture, never tickers.
    # "defensive at the macro" -> risk-off lean; macro "aggressive"/"risk-on" ->
    # risk-on. "selectively aggressive in small caps" is security selection, not
    # a macro allocation call, so it does NOT flip the macro tilt.
    brain = read_json("data/brain.json")
    rp = ((brain.get("directive") or {}).get("risk_posture") or "")
    rpl = rp.lower()
    if rpl:
        intensity = 0.0
        if "defensive at the macro" in rpl or "defensive macro" in rpl:
            intensity += 0.6
        elif "defensive" in rpl:
            intensity += 0.4
        if "risk-on" in rpl or "risk on" in rpl or "aggressive at the macro" in rpl:
            intensity -= 0.5
        out["brain_posture"] = {"value": round(intensity, 2),
                                "intensity": clamp(intensity, -1.0, 1.0),
                                "label": "Brain Macro Posture",
                                "regime": rp[:60]}

    return out
def aggregate_tilts(signals, ic_weights):
    """Combine signal-by-signal tilt proposals into a final per-asset
    tilt vector, weighted by each signal's IC from the fleet calibrator.
    Signals not in TILT_MAGNITUDES are skipped (no tilt function). The
    contribution of each signal to each asset's tilt is recorded for
    the rationale section of the report."""
    tilts = {a: 0.0 for a in ASSETS}
    weight_sum = 0.0
    contributions = {a: [] for a in ASSETS}

    for sig_name, sig in signals.items():
        if sig_name not in TILT_MAGNITUDES:
            continue
        # IC weight with a generous floor so signals lacking IC still
        # contribute something close to equal weight
        ic = ic_weights.get(sig_name)
        if ic is None:
            w = 0.10   # default weight when no IC available
            ic_state = "no-ic"
        elif ic <= 0:
            w = 0.05   # near-zero weight for non-predictive engines
            ic_state = "non-predictive"
        else:
            w = max(0.05, ic)  # floor at 5%
            ic_state = "ic-weighted"
        weight_sum += w

        intensity = sig["intensity"]
        for asset, mag in TILT_MAGNITUDES[sig_name].items():
            tilt_proposal = mag * intensity   # signed contribution
            contribution = w * tilt_proposal
            tilts[asset] += contribution
            if abs(contribution) >= 0.05:
                contributions[asset].append({
                    "signal": sig["label"],
                    "value": round(sig["value"], 2),
                    "intensity": round(intensity, 3),
                    "tilt_pp": round(contribution, 2),
                    "ic": round(ic, 4) if ic is not None else None,
                    "weight": round(w, 4),
                    "state": ic_state,
                })

    # Normalise the tilt magnitudes by the active weight sum so the
    # final tilts are interpretable as weighted-average percentage-
    # point deviations from benchmark.
    if weight_sum > 0:
        tilts = {a: tilts[a] / weight_sum for a in ASSETS}
        for a in contributions:
            for c in contributions[a]:
                c["tilt_pp"] = round(c["tilt_pp"] / weight_sum, 2)

    return tilts, contributions, weight_sum


def apply_limits_and_renormalise(target):
    """Clamp each asset to its (min, max), then renormalise so the
    total sums to 100%. If a single clip causes total drift, the free
    assets absorb the residual proportionally."""
    after_clip = {}
    clipped_total = 0.0
    for a in ASSETS:
        lo, hi = POSITION_LIMITS[a]
        after_clip[a] = clamp(target[a], lo, hi)
        clipped_total += after_clip[a]
    if abs(clipped_total - 100.0) < 0.01:
        return after_clip
    # redistribute to make total = 100%
    residual = 100.0 - clipped_total
    # free dimensions = not at a binding constraint (after this clip)
    free = []
    for a in ASSETS:
        lo, hi = POSITION_LIMITS[a]
        if (residual > 0 and after_clip[a] < hi - 1e-6) or \
           (residual < 0 and after_clip[a] > lo + 1e-6):
            free.append(a)
    if not free:
        return after_clip   # all binding -- the constraints are infeasible
    free_sum = sum(after_clip[a] for a in free)
    if free_sum <= 0:
        even = residual / len(free)
        for a in free:
            after_clip[a] = clamp(after_clip[a] + even,
                                   *POSITION_LIMITS[a])
    else:
        for a in free:
            adj = residual * after_clip[a] / free_sum
            after_clip[a] = clamp(after_clip[a] + adj,
                                   *POSITION_LIMITS[a])
    return after_clip


def enforce_risk_budget(target, benchmark, max_active_bps):
    """If the sum of |delta| (active percent across all assets) exceeds
    the budget, shrink all deltas proportionally."""
    active = sum(abs(target[a] - benchmark[a]) for a in ASSETS)
    if active <= max_active_bps / 100.0:
        return target, False, active
    scale = (max_active_bps / 100.0) / active
    shrunk = {a: benchmark[a] + (target[a] - benchmark[a]) * scale
              for a in ASSETS}
    return shrunk, True, active


# ============== handler ==================================================
# ═══════════ compass bridge (v1.2 fusion — ops 2985) ═══════════
COMPASS_SLEEVE_MAP = {
    "us_equity": "SPY", "intl_dev_eq": "EFA", "em_equity": "EEM",
    "ust_long": "TLT", "ust_short": "IEF", "ig_credit": "LQD",
    "gold": "GLD", "btc": "BTC", "cash": "CASH",
}
COMPASS_TILT_K = 0.35        # pp of tilt per pp of relative excess ER
COMPASS_TILT_CAP = 2.0       # per-sleeve bound, pp
COMPASS_LAYER_CAP = 6.0      # total |layer| bound, pp


def compass_bridge(tilts, contributions):
    """Fuse asset-compass v1.2 into the allocation: each sleeve is
    tilted toward/away by its compass excess-vs-cash ER relative to
    the mapped-universe average, bounded per sleeve and in total.
    The v1.2 correlation matrix gates the duration tilt: in a
    positive stock-bond-correlation regime (SPY-TLT 90d corr > +0.30)
    duration is not a hedge, so a positive compass duration tilt is
    zeroed (never forced negative). Failure-isolated: any error
    leaves the allocation untouched."""
    info = {"used": False}
    try:
        comp = read_json("data/asset-compass.json")
        if str(comp.get("schema_version", "")) < "1.2":
            info["note"] = "compass schema < 1.2"
            return tilts, info
        rf = (comp.get("hurdle") or {}).get("cash_rf_pct") or 0.0
        by_t = {a.get("ticker"): a for a in comp.get("assets") or []}
        excess = {}
        for sleeve, tkr in COMPASS_SLEEVE_MAP.items():
            er = (by_t.get(tkr) or {}).get("er_1y_pct")
            if er is not None:
                excess[sleeve] = er - rf
        if len(excess) < 6:
            info["note"] = "only %d sleeves mapped" % len(excess)
            return tilts, info
        avg = sum(excess.values()) / len(excess)
        # stock-bond regime from the v1.2 matrix
        co = comp.get("correlations") or {}
        spy_tlt = None
        try:
            tk = co.get("tickers") or []
            i, j = tk.index("SPY"), tk.index("TLT")
            spy_tlt = (co.get("matrix") or [])[i][j]
        except Exception:
            pass
        layer = {}
        for sleeve, ex in excess.items():
            t = clamp(COMPASS_TILT_K * (ex - avg),
                      -COMPASS_TILT_CAP, COMPASS_TILT_CAP)
            layer[sleeve] = t
        dur_gated = False
        if isinstance(spy_tlt, (int, float)) and spy_tlt > 0.30:
            for sl in ("ust_long", "ust_short"):
                if layer.get(sl, 0.0) > 0:
                    layer[sl] = 0.0
                    dur_gated = True
        tot = sum(abs(v) for v in layer.values())
        if tot > COMPASS_LAYER_CAP:
            k = COMPASS_LAYER_CAP / tot
            layer = {a: v * k for a, v in layer.items()}
        for sleeve, t in layer.items():
            if abs(t) < 0.05:
                continue
            tilts[sleeve] = tilts.get(sleeve, 0.0) + t
            contributions.setdefault(sleeve, []).append({
                "signal": "Asset Compass ER (v1.2)",
                "value": round(excess[sleeve] + rf, 2),
                "intensity": round((excess[sleeve] - avg) / 10.0, 3),
                "tilt_pp": round(t, 2),
                "ic": None, "weight": COMPASS_TILT_K,
                "state": "compass-er"})
        info = {"used": True, "cash_rf_pct": rf,
                "spy_tlt_corr_90d": spy_tlt,
                "duration_hedge_gated": dur_gated,
                "sleeves_mapped": len(excess),
                "tilts_pp": {a: round(v, 2) for a, v in layer.items()
                             if abs(v) >= 0.05},
                "note": ("positive stock-bond-corr regime: compass "
                         "duration overweight zeroed" if dur_gated
                         else "ER-relative tilts, bounded +/-%.1fpp"
                         % COMPASS_TILT_CAP)}
    except Exception as e:
        info = {"used": False, "error": str(e)[:120]}
    return tilts, info
# ═══════════════════ end compass bridge ═══════════════════





# ═══ BEST ASSET NOW (ops 3287) — Khalid: "somewhere in my system it
# should show the best asset to invest in, and sometimes it can just be
# the dollar or a money-market fund and that should be fine." Dual
# momentum (Antonacci) with an EXPLICIT T-bill hurdle: every risk asset
# must beat cash over 12-1m or CASH outranks it, plus a risk-regime
# override (crisis composite / GSI / us10y-sentinel RED+) that
# restricts the podium to CASH / USD / GOLD / short-duration. Failure-
# isolated like compass_bridge. Real prices only (Yahoo v8 daily). ═══
_BA_UNIV = [("SPY", "US Stocks"), ("QQQ", "US Tech"),
            ("IWM", "US Small Cap"), ("EFA", "Intl Developed"),
            ("EEM", "Emerging Mkts"), ("TLT", "Long Treasuries"),
            ("IEF", "7-10y Treasuries"), ("GLD", "Gold"),
            ("SLV", "Silver"), ("DBC", "Commodities"),
            ("UUP", "US Dollar"), ("BTC-USD", "Bitcoin")]
_BA_DEFENSIVE = {"CASH", "UUP", "GLD", "IEF"}


def _ba_closes(sym):
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           + urllib.parse.quote(sym) + "?range=13mo&interval=1d")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (jh-allocator)"})
    j = json.loads(urllib.request.urlopen(req, timeout=20).read())
    cl = j["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    return [c for c in cl if c is not None]


def _ba_mom(px):
    if len(px) < 220:
        return None
    r12_1 = px[-21] / px[-min(len(px), 252)] - 1
    r6 = px[-1] / px[-126] - 1
    r3 = px[-1] / px[-63] - 1
    return {"r12_1": r12_1, "r6": r6, "r3": r3,
            "score": round(100 * (.5 * r12_1 + .3 * r6 + .2 * r3), 2)}


def _ba_s3json(key):
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _best_asset_now():
    rows = []
    for sym, label in _BA_UNIV:
        try:
            m = _ba_mom(_ba_closes(sym))
            if m:
                rows.append(dict(asset=sym, label=label, **m))
        except Exception as e:
            print("best_asset %s: %s" % (sym, e))
        time.sleep(0.12)
    hurdle = None
    try:
        hurdle = _ba_mom(_ba_closes("BIL"))
    except Exception:
        pass
    hscore = hurdle["score"] if hurdle else 4.0
    rows.append({"asset": "CASH",
                 "label": "Cash / T-Bills (BIL·money market)",
                 "r12_1": (hurdle or {}).get("r12_1"),
                 "r6": (hurdle or {}).get("r6"),
                 "r3": (hurdle or {}).get("r3"), "score": hscore})
    for r in rows:
        r["beats_cash"] = bool(r["asset"] == "CASH"
                               or r["score"] > hscore)

    reasons = []
    sent = _ba_s3json("data/us10y-sentinel.json")
    if sent.get("tier") in ("RED", "CRITICAL"):
        reasons.append("us10y-sentinel %s (%s)" % (
            sent.get("tier"),
            str(sent.get("tier_reason", ""))[:90]))
    cc = _ba_s3json("data/crisis-composite.json")
    for k in ("composite", "score", "level"):
        v = cc.get(k)
        if isinstance(v, (int, float)) and v >= 70:
            reasons.append("crisis-composite %s=%.0f" % (k, v))
            break
    gsi = _ba_s3json("data/global-stress.json")
    for k in ("gsi", "score", "composite"):
        v = gsi.get(k)
        if isinstance(v, (int, float)) and v >= 70:
            reasons.append("global-stress %s=%.0f" % (k, v))
            break
    override = bool(reasons)

    pool = [r for r in rows if r["beats_cash"]]
    if override:
        pool = [r for r in rows if r["asset"] in _BA_DEFENSIVE]
    if not pool:
        pool = [r for r in rows if r["asset"] == "CASH"]
    pool.sort(key=lambda r: -(r["score"] if r["score"] is not None
                              else -999))
    ranked = sorted(rows, key=lambda r: -(r["score"] or -999))
    win = pool[0]
    why = ("Highest blended momentum (50/30/20 of 12-1m/6m/3m) among "
           "assets clearing the T-bill hurdle."
           if not override else
           "Risk override active (%s) — podium restricted to cash/"
           "USD/gold/short-duration; capital preservation IS the "
           "trade." % "; ".join(reasons))
    if win["asset"] == "CASH" and not override:
        why = ("No risk asset beats the T-bill hurdle over 12-1m — "
               "dual momentum says sit in cash and get paid to wait.")
    return {"winner": {"asset": win["asset"], "label": win["label"],
                       "score": win["score"], "why": why},
            "top3": [{"asset": r["asset"], "label": r["label"],
                      "score": r["score"]} for r in pool[:3]],
            "ranked": ranked,
            "cash_hurdle_score": hscore,
            "risk_override": {"active": override, "reasons": reasons},
            "methodology": (
                "Dual momentum with explicit cash hurdle: blended "
                "12-1m/6m/3m total-return momentum per asset class; a "
                "risk asset is only eligible if it beats the same "
                "blend on T-bills (BIL). Crisis-composite / GSI ≥70 "
                "or us10y-sentinel RED+ overrides the podium to "
                "defensive assets. Cash and the dollar are first-"
                "class answers, not fallbacks.")}


def lambda_handler(event, context):
    t0 = time.time()

    signals = gather_signals()
    ic_weights = load_ic_weights()
    raw_tilts, contributions, weight_sum = aggregate_tilts(
        signals, ic_weights)

    # compass v1.2 fusion layer (additive, bounded, failure-isolated)
    raw_tilts, compass_info = compass_bridge(raw_tilts, contributions)

    # provisional target = benchmark + tilts
    target = {a: BENCHMARK[a] + raw_tilts[a] for a in ASSETS}

    # apply position limits + renormalise
    target = apply_limits_and_renormalise(target)

    # enforce active-risk budget
    target, budget_clipped, active_bps = enforce_risk_budget(
        target, BENCHMARK, MAX_ACTIVE_BPS)

    # apply position limits again after risk-budget shrinkage
    target = apply_limits_and_renormalise(target)

    # rationale: top contributing signals per asset
    deltas = {a: round(target[a] - BENCHMARK[a], 2) for a in ASSETS}

    # confidence: how many signals contributed, weighted by IC
    contributing = sum(1 for s in signals if s in TILT_MAGNITUDES)
    ic_present = sum(1 for s in signals if s in TILT_MAGNITUDES
                     and ic_weights.get(s) is not None
                     and ic_weights.get(s) > 0)
    confidence = round(
        min(100, contributing * 8 + ic_present * 4 + 30))   # 0-100

    # tactical posture summary
    eq_total = (target["us_equity"] + target["intl_dev_eq"]
                + target["em_equity"])
    bench_eq = (BENCHMARK["us_equity"] + BENCHMARK["intl_dev_eq"]
                + BENCHMARK["em_equity"])
    duration_total = target["ust_long"] + target["ust_short"]
    bench_dur = BENCHMARK["ust_long"] + BENCHMARK["ust_short"]
    eq_tilt = eq_total - bench_eq
    duration_tilt = duration_total - bench_dur

    if eq_tilt < -3:
        posture = "DEFENSIVE"
    elif eq_tilt < -1:
        posture = "DEFENSIVE_TILT"
    elif eq_tilt < 1:
        posture = "NEUTRAL"
    elif eq_tilt < 3:
        posture = "RISK_ON_TILT"
    else:
        posture = "RISK_ON"

    # publish
    report = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "compass_bridge": compass_info,
        "posture": posture,
        "confidence": confidence,
        "active_risk_bps": round(active_bps * 100, 0),
        "risk_budget_clipped": budget_clipped,
        "horizon": "1-month tactical",
        "benchmark": BENCHMARK,
        "target_allocation": {a: round(target[a], 2) for a in ASSETS},
        "deltas_from_benchmark": deltas,
        "asset_labels": ASSET_LABELS,
        "position_limits": {a: list(POSITION_LIMITS[a]) for a in ASSETS},
        "max_active_bps": MAX_ACTIVE_BPS,
        "signals_used": {
            sig_name: {
                "value": signals[sig_name]["value"],
                "intensity": round(signals[sig_name]["intensity"], 3),
                "label": signals[sig_name]["label"],
                "regime": signals[sig_name].get("regime")
                or signals[sig_name].get("level"),
                "ic": ic_weights.get(sig_name),
                "in_allocator": sig_name in TILT_MAGNITUDES,
            }
            for sig_name in signals
        },
        "contributions": {a: contributions[a] for a in ASSETS
                          if contributions[a]},
        "summary": {
            "equity_tilt_pp": round(eq_tilt, 2),
            "duration_tilt_pp": round(duration_tilt, 2),
            "gold_tilt_pp": round(target["gold"] - BENCHMARK["gold"], 2),
            "cash_tilt_pp": round(target["cash"] - BENCHMARK["cash"], 2),
            "btc_tilt_pp": round(target["btc"] - BENCHMARK["btc"], 2),
        },
        "rationale": (
            ("Risk posture %s (equity tilt %+.1fpp vs benchmark, "
             "duration tilt %+.1fpp, gold tilt %+.1fpp, cash tilt "
             "%+.1fpp). Active risk %d bps of %d budget%s. Confidence "
             "%d based on %d/%d signals contributing%s.") % (
                posture, eq_tilt, duration_tilt,
                target["gold"] - BENCHMARK["gold"],
                target["cash"] - BENCHMARK["cash"],
                round(active_bps * 100), MAX_ACTIVE_BPS,
                " (CLIPPED)" if budget_clipped else "",
                confidence, contributing, len(TILT_MAGNITUDES),
                ", " + str(ic_present) + " IC-weighted"
                if ic_present else "")),
        "methodology": (
            "Strategic benchmark + signal-driven tactical tilts. Each "
            "signal proposes a bounded per-asset tilt vector; tilts are "
            "weighted by the signal's IC from the calibration fleet "
            "(SSM /justhodl/calibration-fleet/weights). Position limits "
            "and an %d bps active-risk budget bound the deviation. The "
            "allocation is renormalised to sum to 100%%." %
            int(MAX_ACTIVE_BPS)),
        "duration_s": round(time.time() - t0, 1),
    }

    # BEST ASSET NOW (ops 3287) — failure-isolated
    try:
        report["best_asset"] = _best_asset_now()
        print("best_asset winner: %s" %
              report["best_asset"]["winner"]["asset"])
    except Exception as _e:
        report["best_asset"] = {"error": str(_e)[:140]}

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(report,
                                  default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=600")

    # SSM target for downstream consumers
    try:
        ssm.put_parameter(Name=TARGET_PARAM, Type="String",
                          Overwrite=True,
                          Value=json.dumps({
                              "target": report["target_allocation"],
                              "posture": posture,
                              "confidence": confidence,
                              "as_of": report["as_of"],
                          }))
    except Exception as e:
        print("ssm put fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "posture": posture, "confidence": confidence,
        "active_bps": round(active_bps * 100),
        "signals_used": contributing,
        "elapsed_s": round(time.time() - t0, 1)})}
