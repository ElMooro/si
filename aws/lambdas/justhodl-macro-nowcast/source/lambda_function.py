"""justhodl-macro-nowcast

Composite real-time nowcast indicator. Reuses FRED series already
fetched by daily-report-v3 (data/report.json) — no new API calls.

For each input series, compute YoY % change and convert to a z-score
vs trailing 5y mean/stdev. Weighted sum becomes the composite nowcast.

INPUT WEIGHTS (sum to 1.0 in absolute value)

  INDPRO   industrial production     +0.20  (leading)
  PAYEMS   nonfarm payrolls          +0.25  (heavy: best monthly indicator)
  RSAFS    retail sales              +0.20
  HOUST    housing starts            +0.10  (interest-rate sensitive)
  UMCSENT  consumer sentiment        +0.10  (soft data, leading)
  T10Y2Y   2s10s yield curve         +0.10  (curve = forward growth)
  UNRATE   unemployment              -0.05  (inverse: rising unemp = slowing)

OUTPUT INTERPRETATION

  composite > +1.0  → strong expansion
  +0.3 to +1.0      → expansion
  -0.3 to +0.3      → muddle
  -1.0 to -0.3      → slowing
  composite < -1.0  → contraction risk

Schedule: rate(6 hours) — only matters when daily-report writes new data.

Written to: data/macro-nowcast.json
"""
from __future__ import annotations
import json
import os
import statistics
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SOURCE_KEY = "data/report.json"
OUTPUT_KEY = "data/macro-nowcast.json"

# Weights for the composite. Positive weight = higher value of input
# pushes nowcast UP. Negative weight = higher value pushes nowcast DOWN.
# Absolute weights sum to 1.0.
WEIGHTS = {
    "INDPRO":  {"weight": +0.20, "label": "Industrial Production",
                "transform": "yoy_pct", "rationale": "Leading manufacturing pulse"},
    "PAYEMS":  {"weight": +0.25, "label": "Nonfarm Payrolls",
                "transform": "yoy_pct", "rationale": "Single best monthly growth indicator"},
    "RSAFS":   {"weight": +0.20, "label": "Retail Sales",
                "transform": "yoy_pct", "rationale": "Consumer demand"},
    "HOUST":   {"weight": +0.10, "label": "Housing Starts",
                "transform": "yoy_pct", "rationale": "Rate-sensitive, leading"},
    "UMCSENT": {"weight": +0.10, "label": "Consumer Sentiment (UMich)",
                "transform": "level_z", "rationale": "Soft data, leads spending"},
    "T10Y2Y":  {"weight": +0.10, "label": "2s10s Yield Curve",
                "transform": "level_z", "rationale": "Inversion = forward slowing"},
    "UNRATE":  {"weight": -0.05, "label": "Unemployment Rate",
                "transform": "level_z", "rationale": "Inverse: rising unemp = slowing"},
}

s3 = boto3.client("s3", region_name=REGION)


def safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


def get_series_history(report: dict, fred_id: str):
    """Find a FRED series's history in report.json. The schema can vary
    slightly across versions — try the canonical paths."""
    # Pattern 1: fredSeries[fred_id] -> {"data": [{"date": ..., "value": ...}, ...]}
    fred = report.get("fredSeries") or report.get("fred") or report.get("series") or {}
    s = fred.get(fred_id) if isinstance(fred, dict) else None
    if isinstance(s, dict):
        if isinstance(s.get("data"), list):
            return [(o.get("date"), safe_float(o.get("value"))) for o in s["data"] if o.get("date")]
        if isinstance(s.get("series"), list):
            return [(o.get("date"), safe_float(o.get("value"))) for o in s["series"] if o.get("date")]
    # Pattern 2: macros[fred_id] = {"current_value": ..., "history": [{...}]}
    macros = report.get("macros") or {}
    if fred_id in macros and isinstance(macros[fred_id], dict):
        h = macros[fred_id].get("history") or macros[fred_id].get("data") or []
        if isinstance(h, list):
            return [(o.get("date"), safe_float(o.get("value"))) for o in h if o.get("date")]
    # Pattern 3: data flat — { "PAYEMS": [...] }
    flat = report.get(fred_id)
    if isinstance(flat, list):
        return [(o.get("date"), safe_float(o.get("value"))) for o in flat if o.get("date")]
    return []


def yoy_pct(history):
    """Year-over-year percent change. history = [(date, value), ...] sorted asc."""
    if len(history) < 13:
        return None, "insufficient_history"
    history = sorted([(d, v) for d, v in history if v is not None],
                     key=lambda x: x[0])
    if len(history) < 13:
        return None, "insufficient_after_filter"
    cur = history[-1][1]
    # Find observation closest to 12 months ago
    cur_d = history[-1][0]
    target_year = cur_d[:4]
    target_month = cur_d[5:7]
    try:
        target_y = int(target_year) - 1
        target_str = f"{target_y}-{target_month}-"
    except Exception:
        return None, "date_parse"
    # Walk backwards to find closest matching month
    for d, v in reversed(history[:-1]):
        if d.startswith(target_str):
            if v is None or v == 0:
                return None, "zero_or_null_yoy_base"
            return ((cur - v) / v) * 100, None
    # Fallback: 12 obs back
    if len(history) >= 13:
        v_yoy = history[-13][1]
        if v_yoy is None or v_yoy == 0:
            return None, "zero_yoy_base_fallback"
        return ((cur - v_yoy) / v_yoy) * 100, None
    return None, "no_yoy_match"


def trailing_zscore(history, current_value, lookback_obs=60):
    """Z-score of current value vs trailing N obs. Used for level-z transforms."""
    history = sorted([(d, v) for d, v in history if v is not None], key=lambda x: x[0])
    if len(history) < 24:
        return None, "insufficient_history"
    window = history[-lookback_obs:-1] if len(history) > lookback_obs else history[:-1]
    vals = [v for _, v in window if v is not None]
    if len(vals) < 12:
        return None, "insufficient_window"
    m = statistics.mean(vals)
    sd = statistics.stdev(vals) if len(vals) >= 2 else 0
    if sd == 0:
        return None, "zero_stdev"
    return (current_value - m) / sd, None


def transform_zscore(history, transform: str):
    """Convert a series's current state to a z-score of its YoY change
    or level. Returns (z, raw_value, error)."""
    history = sorted([(d, v) for d, v in history if v is not None], key=lambda x: x[0])
    if not history:
        return None, None, "empty_series"
    current = history[-1][1]

    if transform == "yoy_pct":
        # Compute YoY for every month, then z-score the latest YoY
        yoys = []
        for i in range(12, len(history)):
            d_t = history[i][0]
            v_t = history[i][1]
            if v_t is None:
                continue
            d_p = history[i - 12][0]
            v_p = history[i - 12][1]
            if v_p is None or v_p == 0:
                continue
            # Date should be ~12 months earlier
            if d_t[:4] == d_p[:4]:
                continue  # same year
            yoys.append({"d": d_t, "yoy": ((v_t - v_p) / v_p) * 100})
        if len(yoys) < 12:
            return None, current, "insufficient_yoy_history"
        latest_yoy = yoys[-1]["yoy"]
        recent_yoys = [y["yoy"] for y in yoys[-60:-1]] if len(yoys) > 60 else [y["yoy"] for y in yoys[:-1]]
        if len(recent_yoys) < 12:
            return None, current, "insufficient_yoy_window"
        m = statistics.mean(recent_yoys)
        sd = statistics.stdev(recent_yoys) if len(recent_yoys) >= 2 else 0
        if sd == 0:
            return None, latest_yoy, "zero_yoy_stdev"
        z = (latest_yoy - m) / sd
        return z, latest_yoy, None

    if transform == "level_z":
        z, err = trailing_zscore(history, current, 60)
        return z, current, err

    return None, current, f"unknown_transform:{transform}"


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[nowcast] start {datetime.now(timezone.utc).isoformat()}")

    try:
        report = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SOURCE_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"can't read {SOURCE_KEY}: {e}"})}

    components = []
    weighted_sum = 0.0
    weight_used_abs = 0.0

    for fred_id, spec in WEIGHTS.items():
        history = get_series_history(report, fred_id)
        if not history:
            components.append({
                "fred_id": fred_id,
                "label": spec["label"],
                "transform": spec["transform"],
                "weight": spec["weight"],
                "rationale": spec["rationale"],
                "z": None,
                "raw_value": None,
                "contribution": None,
                "error": "series_not_in_report",
            })
            continue

        z, raw_value, err = transform_zscore(history, spec["transform"])
        if z is None:
            components.append({
                "fred_id": fred_id,
                "label": spec["label"],
                "transform": spec["transform"],
                "weight": spec["weight"],
                "rationale": spec["rationale"],
                "z": None,
                "raw_value": raw_value,
                "contribution": None,
                "error": err,
                "n_obs": len(history),
            })
            continue

        contribution = spec["weight"] * z
        components.append({
            "fred_id": fred_id,
            "label": spec["label"],
            "transform": spec["transform"],
            "weight": spec["weight"],
            "rationale": spec["rationale"],
            "z": round(z, 3),
            "raw_value": round(raw_value, 3) if raw_value is not None else None,
            "contribution": round(contribution, 4),
            "n_obs": len(history),
            "latest_date": history[-1][0],
        })
        weighted_sum += contribution
        weight_used_abs += abs(spec["weight"])

    # Normalize against weights actually used (so missing components don't deflate the score)
    if weight_used_abs > 0:
        total_abs_weight = sum(abs(s["weight"]) for s in WEIGHTS.values())
        coverage = weight_used_abs / total_abs_weight
        normalized_score = weighted_sum * (total_abs_weight / weight_used_abs) if coverage > 0 else 0
    else:
        coverage = 0
        normalized_score = 0

    # Regime classification
    score = normalized_score
    if score > 1.0:
        regime, regime_color = "STRONG EXPANSION", "green"
    elif score > 0.3:
        regime, regime_color = "EXPANSION", "green"
    elif score > -0.3:
        regime, regime_color = "MUDDLE", "yellow"
    elif score > -1.0:
        regime, regime_color = "SLOWING", "amber"
    else:
        regime, regime_color = "CONTRACTION RISK", "red"

    # Sort components by abs(contribution) so leaders surface
    components.sort(key=lambda c: -abs(c.get("contribution") or 0))

    output = {
        "v": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - started, 2),
        "raw_score": round(weighted_sum, 4),
        "normalized_score": round(normalized_score, 4),
        "regime": regime,
        "regime_color": regime_color,
        "coverage_pct": round(coverage * 100, 1),
        "n_components_used": sum(1 for c in components if c.get("contribution") is not None),
        "n_components_failed": sum(1 for c in components if c.get("error")),
        "components": components,
        "thresholds": {
            "strong_expansion": 1.0,
            "expansion": 0.3,
            "muddle": -0.3,
            "slowing": -1.0,
        },
        "methodology": (
            "Composite z-score nowcast: each component is converted to a "
            "z-score (vs trailing 5y of YoY changes for flow series, vs "
            "trailing 5y of levels for level series), weighted, summed, "
            "and renormalized against the weights actually used so missing "
            "components don't deflate the headline score."
        ),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    print(f"[nowcast] regime={regime}  score={round(normalized_score, 3)}  "
          f"coverage={round(coverage*100, 0)}%  duration={round(time.time()-started, 2)}s")
    return {"statusCode": 200, "body": json.dumps({
        "regime": regime,
        "score": round(normalized_score, 4),
        "coverage_pct": round(coverage * 100, 1),
    })}
