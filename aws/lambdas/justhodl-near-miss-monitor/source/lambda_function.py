"""justhodl-near-miss-monitor — config-driven near-miss extraction from
engine S3 snapshots, NO ENGINE COOPERATION REQUIRED.

THE PROBLEM
───────────
miss-calibrator needs miss-summary.near_misses_by_signal populated to make
threshold proposals. Engines only log signals they FIRE, never the ones
that ALMOST fired. So that field stays empty.

THE OBSERVATION
───────────────
Most engines already write their FULL ranked output to S3 (not just the
fired items). earnings-pead emits all_qualifying with tier_s (fires) AND
tier_a (just below threshold). opportunity-engine emits opportunities
with opportunity_score for every evaluated ticker. The near-miss data
is ALREADY THERE in the snapshots.

THE SOLUTION
────────────
A small Lambda runs hourly, reads each tracked engine's snapshot, and
extracts the near-miss count using a per-engine adapter:

  - tier_band_count    : count items in a specific tier label
  - score_band_count   : count items with score in [lower, upper)
  - z_score_band_count : count items with z-score in absolute [lower, upper)

Output → data/near-misses-by-signal.json, which miss-detector then
folds into miss-summary.near_misses_by_signal each nightly run.

ADDING A NEW SIGNAL
───────────────────
Append one entry to NEAR_MISS_CONFIGS — signal_type, snapshot_key,
extractor + params. No code changes elsewhere.

INTENTIONALLY MINIMAL V1
────────────────────────
Coverage starts with the 4 promoted signals where the extraction is
obvious (tier-based or clear score band). The remaining 6 promoted
signals (mostly macro composites firing on z-score extremes) need
adapters — added in later sessions as patterns become clear.

SCHEDULE
────────
cron(30 * * * ? *) — every hour at :30, before the hourly aggregation
points (signal-board at :15, alpha-compass at :50).
"""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/near-misses-by-signal.json"

s3 = boto3.client("s3", region_name=REGION)


# ─── Per-signal configuration ────────────────────────────────────────────
# Each entry describes ONE signal_type and how to extract near-miss counts
# from its engine's S3 snapshot.
#
#   signal_type:     name used in scorecard + miss-summary
#   snapshot_key:    S3 key for the engine's output
#   extractor:       one of: tier_label, score_band, z_score_band
#   params:          extractor-specific kwargs
#
NEAR_MISS_CONFIGS = [
    # earnings-pead: tier_a items (score 60-74) are textbook near-misses
    # for the tier_s threshold (score >=75 AND beat_streak >=3).
    {
        "signal_type":  "earnings_pead",
        "snapshot_key": "data/earnings-pead.json",
        "extractor":    "tier_label",
        "params": {
            "array_path": "all_qualifying",
            "tier_field": "tier",
            "near_tier":  "TIER_A_HIGH_QUALITY_BEAT",
        },
    },

    # opportunity-engine: opportunities.json schema (verified ops/1015):
    #   top_opportunities[]  — top items already firing (verdict=STRONG OPPORTUNITY)
    #   all[]                — every evaluated ticker with verdict + opportunity_score
    # Near-miss zone: opportunity_score in [60, 80) — strong fundamentals
    # but not quite a "STRONG OPPORTUNITY" call.
    {
        "signal_type":  "screener_top_pick",
        "snapshot_key": "data/opportunities.json",
        "extractor":    "score_band",
        "params": {
            "array_path":  "all",
            "score_field": "opportunity_score",
            "lower":       60,
            "upper":       80,
        },
    },
    {
        "signal_type":  "momentum_top_pick",
        "snapshot_key": "data/opportunities.json",
        "extractor":    "score_band",
        "params": {
            "array_path":  "all",
            "score_field": "scores.momentum",
            "lower":       60,
            "upper":       80,
        },
    },
    {
        "signal_type":  "valuation_composite",
        "snapshot_key": "data/opportunities.json",
        "extractor":    "score_band",
        "params": {
            "array_path":  "all",
            "score_field": "scores.value",
            "lower":       60,
            "upper":       80,
        },
    },

    # earnings-cascade: emerging_cascades represent the near-miss zone
    # for becoming a confirmed cascade (titan).
    {
        "signal_type":  "earnings_cascade",
        "snapshot_key": "data/earnings-cascade.json",
        "extractor":    "array_length",
        "params": {"array_path": "emerging_cascades"},
    },

    # crisis-composite: master_crisis_score 0-100, DEFCON level 1-5.
    # Score in [55, 75) → near-DEFCON-4 territory (we'd fire on DEFCON_4).
    # ops/1015 confirmed master_crisis_score is the right field.
    {
        "signal_type":  "crisis_composite_near_extreme",
        "snapshot_key": "data/crisis-composite.json",
        "extractor":    "score_band_single",
        "params": {
            "score_field": "master_crisis_score",
            "lower":       55,
            "upper":       75,
        },
    },
]


def _to_float(v, default=None):
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def get_nested(d: dict, path: str):
    """Traverse a dotted path through a dict ('scores.value' → d['scores']['value'])."""
    if not isinstance(d, dict):
        return None
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def load_snapshot(key: str) -> dict:
    """Load an engine snapshot from S3. Returns {} on any failure."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError) as e:
        print(f"[near-miss] cannot load {key}: {e}")
        return {}


# ─── Extractors ──────────────────────────────────────────────────────────

def extract_tier_label(snapshot: dict, params: dict) -> int:
    """Count items in `array_path` whose `tier_field` equals `near_tier`."""
    arr = get_nested(snapshot, params["array_path"])
    if not isinstance(arr, list):
        return 0
    tier_field = params["tier_field"]
    target = params["near_tier"]
    return sum(1 for r in arr
                if isinstance(r, dict) and r.get(tier_field) == target)


def extract_score_band(snapshot: dict, params: dict) -> int:
    """Count items where `score_field` is in [lower, upper)."""
    arr = get_nested(snapshot, params["array_path"])
    if not isinstance(arr, list):
        return 0
    field = params["score_field"]
    lower = float(params["lower"])
    upper = float(params["upper"])
    count = 0
    for r in arr:
        if not isinstance(r, dict):
            continue
        score = _to_float(get_nested(r, field))
        if score is None:
            continue
        if lower <= score < upper:
            count += 1
    return count


def extract_z_score_band(snapshot: dict, params: dict) -> int:
    """Single z-score reading. Returns 1 if in band, 0 otherwise.

    Useful for composite-z signals where there is one reading per run
    (not a ranked list). A 'near-miss' is one observation where the
    composite was approaching but not at extreme.
    """
    val = _to_float(get_nested(snapshot, params["score_field"]))
    if val is None:
        return 0
    lower = float(params["lower"])
    upper = float(params["upper"])
    abs_val = abs(val)
    return 1 if (lower <= abs_val < upper) else 0


def extract_score_band_single(snapshot: dict, params: dict) -> int:
    """Single non-z-score reading. Returns 1 if value is in [lower, upper).

    Differs from extract_z_score_band by NOT taking absolute value —
    used for 0-100 composite scores (e.g., master_crisis_score) where
    the band is a directional 'elevated but not extreme' zone.
    """
    val = _to_float(get_nested(snapshot, params["score_field"]))
    if val is None:
        return 0
    lower = float(params["lower"])
    upper = float(params["upper"])
    return 1 if (lower <= val < upper) else 0


def extract_array_length(snapshot: dict, params: dict) -> int:
    """Length of an array — used when the engine already buckets items
    into a 'near-miss' category itself (e.g., earnings-cascade.emerging_cascades)."""
    arr = get_nested(snapshot, params["array_path"])
    return len(arr) if isinstance(arr, list) else 0


EXTRACTORS = {
    "tier_label":         extract_tier_label,
    "score_band":         extract_score_band,
    "z_score_band":       extract_z_score_band,
    "score_band_single":  extract_score_band_single,
    "array_length":       extract_array_length,
}


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    near_misses_by_signal = {}
    diagnostics = []

    # Cache snapshots so we only load each S3 key once
    snapshot_cache = {}

    for cfg in NEAR_MISS_CONFIGS:
        sig = cfg["signal_type"]
        key = cfg["snapshot_key"]
        extractor_name = cfg["extractor"]
        params = cfg.get("params") or {}

        if key not in snapshot_cache:
            snapshot_cache[key] = load_snapshot(key)
        snap = snapshot_cache[key]

        if not snap:
            diagnostics.append({"signal_type": sig, "skipped": True,
                                 "reason": f"snapshot {key} unavailable"})
            continue

        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            diagnostics.append({"signal_type": sig, "skipped": True,
                                 "reason": f"unknown extractor {extractor_name}"})
            continue

        try:
            count = fn(snap, params)
        except Exception as e:
            diagnostics.append({"signal_type": sig, "skipped": True,
                                 "reason": f"extractor error: {type(e).__name__}: {str(e)[:100]}"})
            continue

        # Accumulate (multiple configs may report into the same signal_type
        # — sum them so we don't lose data)
        near_misses_by_signal[sig] = near_misses_by_signal.get(sig, 0) + count
        diagnostics.append({
            "signal_type": sig, "snapshot": key,
            "extractor": extractor_name, "count": count,
        })

    # Sort the output by count desc for human readability
    sorted_counts = dict(sorted(near_misses_by_signal.items(),
                                  key=lambda x: -x[1]))

    output = {
        "schema_version":      "1.0",
        "method":              "snapshot_band_extraction_v1",
        "generated_at":        started.isoformat(),
        "near_misses_by_signal": sorted_counts,
        "totals": {
            "n_signals_configured":  len(NEAR_MISS_CONFIGS),
            "n_signals_with_count":  sum(1 for v in sorted_counts.values() if v > 0),
            "n_total_near_misses":   sum(sorted_counts.values()),
            "n_snapshots_loaded":    sum(1 for v in snapshot_cache.values() if v),
            "n_snapshots_failed":    sum(1 for v in snapshot_cache.values() if not v),
        },
        "diagnostics": diagnostics,
    }

    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    print(f"[near-miss] {sum(sorted_counts.values())} near-misses across "
          f"{len(sorted_counts)} signals from {len(snapshot_cache)} snapshots")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "signals_with_count": output["totals"]["n_signals_with_count"],
            "total_near_misses":  output["totals"]["n_total_near_misses"],
        }),
    }


lambda_handler = handler
