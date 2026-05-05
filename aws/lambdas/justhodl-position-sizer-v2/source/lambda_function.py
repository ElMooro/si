"""justhodl-position-sizer-v2 — horizon-aware Kelly recommender.

For each currently-open paper position AND for each top asymmetric setup,
compute a horizon-aware Kelly recommendation by:

  1. Identifying the trade's predicted horizon (day_3 / day_7 / day_30 / etc.)
  2. Pulling the per-horizon calibration weight from
     /justhodl/calibration/weights/{horizon}, falling back to the flat aggregate
     if the (signal, horizon) pair has insufficient data
  3. Computing horizon-aware Kelly:
        f* = (weight × p - q) × KELLY_FRACTION
     where:
        p = prob_correct = base_confidence × horizon_weight_multiplier
        weight = horizon-specific calibration weight (or flat fallback)

The output is a side-by-side comparison vs the existing risk-sizer's flat Kelly
so Khalid can see the "horizon premium" — how much position size shifts when
we use the right horizon's measured reliability.

Schedule: cron(0 14 * * ? *)  — daily 14:00 UTC, after calibrator + risk-sizer
Output: portfolio/sizer-v2.json
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

# Position sizing parameters — match risk-sizer's defaults for apples-to-apples
KELLY_FRACTION = 0.25         # Quarter-Kelly (conservative)
MAX_SINGLE_POSITION_PCT = 0.08  # 8% of NAV cap
BASE_NAV = 100_000.0          # paper portfolio nominal


def to_float(v, default=0.0):
    if v is None:
        return default
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return default


def load_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {e}")
        return default if default is not None else {}


def get_flat_weights():
    try:
        v = SSM.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]["Value"]
        return {k: to_float(v) for k, v in json.loads(v).items()}
    except Exception as e:
        print(f"[ssm-flat] {e}")
        return {}


def get_horizon_weights():
    out = {}
    try:
        paginator = SSM.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(Path="/justhodl/calibration/weights/", Recursive=False):
            for p in page.get("Parameters", []):
                window = p["Name"].split("/")[-1]
                try:
                    out[window] = {k: to_float(v) for k, v in json.loads(p["Value"]).items()}
                except Exception as e:
                    print(f"[ssm-horizon-parse] {p['Name']}: {e}")
    except Exception as e:
        print(f"[ssm-horizon] {e}")
    return out


def resolve_weight(stype, window, flat, horizon):
    if window and window in horizon:
        h = horizon[window].get(stype)
        if h is not None:
            return h, f"horizon:{window}"
    f = flat.get(stype)
    if f is not None:
        return f, "flat"
    return 0.7, "default"


def kelly_size(conviction_pct, weight=1.0, edge_pct=0.05):
    """Horizon-aware Kelly.
    conviction_pct: base prob_correct from signal confidence (0.5-1.0)
    weight: calibration weight at the relevant horizon (≈0 to 1.5+)
    Returns position size as fraction of NAV, capped at MAX_SINGLE_POSITION_PCT.

    The weight modulates conviction: a w=1.5 signal with p=0.65 conviction
    behaves as if p=0.725; a w=0.31 signal with p=0.65 behaves as if p=0.55.
    """
    if conviction_pct <= 0.50:
        return 0.0
    # Adjust conviction by calibration weight (centered at 1.0)
    # weight=1.0 → no change. weight=1.5 → ×1.25 boost. weight=0.31 → ×0.655 penalty.
    weight_multiplier = (1.0 + weight) / 2.0
    adjusted_p = 0.5 + (conviction_pct - 0.5) * weight_multiplier
    adjusted_p = max(0.0, min(1.0, adjusted_p))
    if adjusted_p <= 0.50:
        return 0.0
    q = 1 - adjusted_p
    full_kelly = max(0, adjusted_p - q)
    fractional = full_kelly * KELLY_FRACTION
    return round(min(MAX_SINGLE_POSITION_PCT, fractional), 4)


def horizon_for_position(p):
    """Map a paper-portfolio open position to its target horizon."""
    # Try max_hold_days first
    mh = p.get("max_hold_days")
    if mh:
        try:
            mh = int(mh)
            if mh <= 3:
                return "day_3"
            if mh <= 7:
                return "day_7"
            if mh <= 14:
                return "day_14"
            if mh <= 30:
                return "day_30"
            if mh <= 60:
                return "day_60"
            return "day_90"
        except Exception:
            pass
    # Fall back to source-based heuristic
    src = (p.get("source") or "").lower()
    if "screener" in src:
        return "day_30"
    if "earnings" in src or "pead" in src:
        return "day_14"
    if "momentum" in src:
        return "day_7"
    if "carry" in src or "regime" in src:
        return "day_30"
    return "day_30"


def signal_type_for_position(p):
    """Map a paper-portfolio open position to its source signal_type."""
    src = (p.get("source") or "").lower()
    if "screener" in src:
        return "screener_top_pick"
    if "earnings" in src or "pead" in src:
        return "earnings_pead"
    if "asym" in src:
        return "edge_composite"
    if "momentum" in src:
        return "momentum_spy"
    return "edge_composite"


def write_json(key, data, max_age=300):
    body = json.dumps(data, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=key, Body=body,
        ContentType="application/json",
        CacheControl=f"public, max-age={max_age}",
    )


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)

    # 1. Load weights — both flat and per-horizon
    flat = get_flat_weights()
    horizon = get_horizon_weights()
    n_horizon_pairs = sum(len(v) for v in horizon.values())
    print(f"[sizer-v2] {len(flat)} flat weights + {len(horizon)} horizons "
          f"({n_horizon_pairs} measured pairs)")

    # 2. Load open paper positions
    portfolio = load_json("portfolio/signal-portfolio-state.json")
    open_positions = portfolio.get("open_positions") or []

    # 3. Load top asymmetric setups
    asym = load_json("opportunities/asymmetric-equity.json", default={})
    setups = (asym.get("top_setups") or [])[:15]

    # 4. Load decisive call (modulates risk-on appetite)
    history = (load_json("data/decisive-call-history.json") or {}).get("snapshots") or []
    latest_call = history[-1] if history else {}
    call_verb = (latest_call.get("call_verb") or "UNKNOWN").upper()
    # Risk-multiplier based on call:
    risk_mult = {
        "EXIT_ALL_RISK": 0.0,
        "EXIT": 0.25,
        "TRIM": 0.5,
        "HEDGE": 0.6,
        "WAIT": 0.7,
        "HOLD": 1.0,
        "LONG": 1.2,
        "LOAD": 1.4,
        "LEVER": 1.6,
        "UNKNOWN": 1.0,
    }.get(call_verb, 1.0)
    print(f"[sizer-v2] decisive call: {call_verb} → risk_mult={risk_mult}")

    # 5. For each open position, compute horizon-aware Kelly
    pos_recs = []
    for p in open_positions:
        ticker = p.get("ticker")
        if not ticker:
            continue
        cur_pct = (to_float(p.get("position_pct")) or
                   (to_float(p.get("position_size_dollars"), 0) / BASE_NAV))
        sig_type = signal_type_for_position(p)
        win = horizon_for_position(p)
        weight, src = resolve_weight(sig_type, win, flat, horizon)
        # Conviction from confidence
        conf = to_float(p.get("confidence"), 0.6)
        if conf < 0.5:
            conf = 0.6  # paper portfolio entries default to 60% if unset
        flat_kelly = kelly_size(conf, weight=flat.get(sig_type, 1.0))
        horizon_kelly = kelly_size(conf, weight=weight)
        adj_kelly = horizon_kelly * risk_mult

        # Action recommendation
        if adj_kelly == 0:
            action = "EXIT"
        elif adj_kelly < cur_pct * 0.7:
            action = "TRIM"
        elif adj_kelly > cur_pct * 1.3:
            action = "ADD"
        else:
            action = "HOLD"

        pos_recs.append({
            "ticker": ticker,
            "source": p.get("source"),
            "signal_type": sig_type,
            "horizon": win,
            "weight_used": round(weight, 3),
            "weight_source": src,
            "current_pct": round(cur_pct, 4),
            "current_pnl_pct": to_float(p.get("current_pnl_pct"), 0),
            "confidence": round(conf, 3),
            "flat_kelly_pct": flat_kelly,
            "horizon_kelly_pct": horizon_kelly,
            "horizon_premium_pp": round(horizon_kelly - flat_kelly, 4),
            "call_adjusted_pct": round(adj_kelly, 4),
            "recommended_action": action,
            "delta_pct": round(adj_kelly - cur_pct, 4),
            "dollar_size": round(adj_kelly * BASE_NAV, 2),
        })
    pos_recs.sort(key=lambda x: -abs(x["delta_pct"]))

    # 6. For each top setup, compute horizon-aware Kelly recommendation
    setup_recs = []
    for s in setups:
        ticker = s.get("ticker") or s.get("symbol")
        if not ticker:
            continue
        # Asymmetric setups generally use day_14 or day_30 horizons
        win = "day_30"  # default; override below if scorer specifies
        if s.get("horizon_days"):
            try:
                d = int(s["horizon_days"])
                if d <= 7:
                    win = "day_7"
                elif d <= 14:
                    win = "day_14"
                elif d <= 30:
                    win = "day_30"
                elif d <= 60:
                    win = "day_60"
                else:
                    win = "day_90"
            except Exception:
                pass
        sig_type = "edge_composite"  # asymmetric scorer = edge_composite signal
        weight, src = resolve_weight(sig_type, win, flat, horizon)
        conf = to_float(s.get("composite_score"), 0) / 100.0  # 0-100 → 0-1
        if conf < 0.5:
            conf = 0.55
        flat_k = kelly_size(conf, weight=flat.get(sig_type, 1.0))
        h_k = kelly_size(conf, weight=weight)
        adj = h_k * risk_mult
        setup_recs.append({
            "ticker": ticker,
            "composite_score": s.get("composite_score"),
            "horizon": win,
            "weight_used": round(weight, 3),
            "weight_source": src,
            "confidence": round(conf, 3),
            "flat_kelly_pct": flat_k,
            "horizon_kelly_pct": h_k,
            "horizon_premium_pp": round(h_k - flat_k, 4),
            "call_adjusted_pct": round(adj, 4),
            "dollar_size": round(adj * BASE_NAV, 2),
        })
    setup_recs.sort(key=lambda x: -x["call_adjusted_pct"])

    # 7. Aggregate stats
    total_open_pct = sum(r["current_pct"] for r in pos_recs)
    total_recommended_pct = sum(r["call_adjusted_pct"] for r in pos_recs)
    n_action = defaultdict(int)
    for r in pos_recs:
        n_action[r["recommended_action"]] += 1

    out = {
        "v": "1.0",
        "generated_at": now.isoformat(),
        "method": "horizon_aware_kelly_v1",
        "kelly_fraction": KELLY_FRACTION,
        "max_single_position_pct": MAX_SINGLE_POSITION_PCT,
        "decisive_call": call_verb,
        "risk_multiplier": risk_mult,
        "summary": {
            "n_open_positions": len(pos_recs),
            "n_setups_evaluated": len(setup_recs),
            "total_current_exposure_pct": round(total_open_pct, 4),
            "total_recommended_exposure_pct": round(total_recommended_pct, 4),
            "exposure_change_pp": round(total_recommended_pct - total_open_pct, 4),
            "actions": dict(n_action),
            "n_horizon_aware": sum(1 for r in pos_recs if r["weight_source"].startswith("horizon:"))
                                + sum(1 for r in setup_recs if r["weight_source"].startswith("horizon:")),
            "n_flat_fallback": sum(1 for r in pos_recs if r["weight_source"] == "flat")
                                + sum(1 for r in setup_recs if r["weight_source"] == "flat"),
        },
        "positions": pos_recs,
        "setups": setup_recs,
    }

    write_json("portfolio/sizer-v2.json", out)
    duration = round(time.time() - started, 2)
    print(f"[sizer-v2] wrote portfolio/sizer-v2.json ({len(json.dumps(out)):,}b) in {duration}s")
    print(f"[sizer-v2] {len(pos_recs)} positions evaluated, actions: {dict(n_action)}")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_positions": len(pos_recs),
        "n_setups": len(setup_recs),
        "actions": dict(n_action),
        "current_exposure_pct": round(total_open_pct, 4),
        "recommended_exposure_pct": round(total_recommended_pct, 4),
        "decisive_call": call_verb,
        "risk_mult": risk_mult,
        "duration_s": duration,
    })}
