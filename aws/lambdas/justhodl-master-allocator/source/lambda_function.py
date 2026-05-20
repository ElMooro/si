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

    return out


# ============== aggregation ==============================================
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
def lambda_handler(event, context):
    t0 = time.time()

    signals = gather_signals()
    ic_weights = load_ic_weights()
    raw_tilts, contributions, weight_sum = aggregate_tilts(
        signals, ic_weights)

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
