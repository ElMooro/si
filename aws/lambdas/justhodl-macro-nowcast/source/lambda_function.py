"""justhodl-macro-nowcast (v2)

Composite real-time nowcast indicator. Fetches 7 FRED series directly
(data/report.json only stores current snapshots, no history needed for
z-scoring against trailing 5y).

For each input series, compute YoY % change for flow series (INDPRO,
PAYEMS, RSAFS, HOUST) or level for level series (UMCSENT, T10Y2Y,
UNRATE), then convert to z-score vs trailing 60 monthly observations.

Output: data/macro-nowcast.json with composite score + regime label.

Schedule: rate(6 hours).
"""
from __future__ import annotations
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/macro-nowcast.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FRED_START = "2000-01-01"   # 25 years of history → solid 60-obs trailing window

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


def fred_fetch(series_id: str):
    """Fetch monthly observations for a FRED series."""
    url = ("https://api.stlouisfed.org/fred/series/observations?"
           + urllib.parse.urlencode({
               "series_id": series_id,
               "api_key": FRED_KEY,
               "file_type": "json",
               "observation_start": FRED_START,
               "frequency": "m",  # monthly aggregation (FRED auto-resamples)
               "limit": 100000,
           }))
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-macro-nowcast/2.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        out = []
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v in (".", "", None):
                continue
            try:
                out.append((o["date"], float(v)))
            except (ValueError, KeyError):
                continue
        out.sort(key=lambda x: x[0])
        return out, None
    except Exception as e:
        return [], str(e)[:200]


def transform_zscore(history, transform: str):
    """Convert series to (z, raw_value, error)."""
    if not history:
        return None, None, "empty_series"
    current = history[-1][1]

    if transform == "yoy_pct":
        # Compute YoY % change for every observation, z-score the latest
        yoys = []
        for i in range(12, len(history)):
            v_t = history[i][1]
            v_p = history[i - 12][1]
            if v_p is None or v_p == 0:
                continue
            yoys.append({"d": history[i][0], "yoy": ((v_t - v_p) / v_p) * 100})
        if len(yoys) < 12:
            return None, current, "insufficient_yoy_history"
        latest_yoy = yoys[-1]["yoy"]
        # Trailing 60-obs window for baseline
        baseline = [y["yoy"] for y in yoys[-60:-1]] if len(yoys) > 60 else [y["yoy"] for y in yoys[:-1]]
        if len(baseline) < 12:
            return None, latest_yoy, "insufficient_yoy_baseline"
        m = statistics.mean(baseline)
        sd = statistics.stdev(baseline) if len(baseline) >= 2 else 0
        if sd == 0:
            return None, latest_yoy, "zero_yoy_stdev"
        z = (latest_yoy - m) / sd
        return z, latest_yoy, None

    if transform == "level_z":
        # Z-score current level vs trailing 60 obs
        baseline = [v for _, v in history[-60:-1]] if len(history) > 60 else [v for _, v in history[:-1]]
        if len(baseline) < 12:
            return None, current, "insufficient_level_baseline"
        m = statistics.mean(baseline)
        sd = statistics.stdev(baseline) if len(baseline) >= 2 else 0
        if sd == 0:
            return None, current, "zero_level_stdev"
        z = (current - m) / sd
        return z, current, None

    return None, current, f"unknown_transform:{transform}"


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[nowcast-v2] start {datetime.now(timezone.utc).isoformat()}")

    fred_data = {}
    fred_errors = {}
    with ThreadPoolExecutor(max_workers=7) as ex:
        futs = {ex.submit(fred_fetch, sid): sid for sid in WEIGHTS.keys()}
        for f in as_completed(futs):
            sid = futs[f]
            history, err = f.result()
            fred_data[sid] = history
            if err:
                fred_errors[sid] = err
            print(f"[nowcast-v2] {sid}: {len(history)} obs  err={err}")

    components = []
    weighted_sum = 0.0
    weight_used_abs = 0.0

    for fred_id, spec in WEIGHTS.items():
        history = fred_data.get(fred_id, [])
        if not history:
            components.append({
                "fred_id": fred_id, "label": spec["label"],
                "transform": spec["transform"], "weight": spec["weight"],
                "rationale": spec["rationale"], "z": None, "raw_value": None,
                "contribution": None, "error": fred_errors.get(fred_id, "no_data"),
            })
            continue

        z, raw_value, err = transform_zscore(history, spec["transform"])
        if z is None:
            components.append({
                "fred_id": fred_id, "label": spec["label"],
                "transform": spec["transform"], "weight": spec["weight"],
                "rationale": spec["rationale"], "z": None,
                "raw_value": round(raw_value, 3) if raw_value is not None else None,
                "contribution": None, "error": err, "n_obs": len(history),
                "latest_date": history[-1][0],
            })
            continue

        contribution = spec["weight"] * z
        components.append({
            "fred_id": fred_id, "label": spec["label"],
            "transform": spec["transform"], "weight": spec["weight"],
            "rationale": spec["rationale"], "z": round(z, 3),
            "raw_value": round(raw_value, 3) if raw_value is not None else None,
            "contribution": round(contribution, 4),
            "n_obs": len(history),
            "latest_date": history[-1][0],
        })
        weighted_sum += contribution
        weight_used_abs += abs(spec["weight"])

    if weight_used_abs > 0:
        total_abs_weight = sum(abs(s["weight"]) for s in WEIGHTS.values())
        coverage = weight_used_abs / total_abs_weight
        normalized_score = weighted_sum * (total_abs_weight / weight_used_abs)
    else:
        coverage = 0
        normalized_score = 0

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

    components.sort(key=lambda c: -abs(c.get("contribution") or 0))

    output = {
        "v": "2.0",
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
            "strong_expansion": 1.0, "expansion": 0.3,
            "muddle": -0.3, "slowing": -1.0,
        },
        "data_sources": {"all": "FRED (st. louis fed)"},
        "methodology": (
            "Weighted z-score nowcast. Each FRED series fetched directly "
            "as monthly observations since 2000. Flow series (INDPRO, "
            "PAYEMS, RSAFS, HOUST) get YoY %-change z-scored against "
            "trailing 60 monthly YoYs. Level series (UMCSENT, T10Y2Y, "
            "UNRATE) get current level z-scored against trailing 60 "
            "monthly levels. Composite = weighted sum, renormalized "
            "against weights actually used so missing components don't "
            "deflate the headline score."
        ),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    print(f"[nowcast-v2] regime={regime}  score={round(normalized_score, 3)}  "
          f"coverage={round(coverage*100, 0)}%  duration={round(time.time()-started, 2)}s")
    return {"statusCode": 200, "body": json.dumps({
        "regime": regime,
        "score": round(normalized_score, 4),
        "coverage_pct": round(coverage * 100, 1),
    })}
