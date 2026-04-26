"""
justhodl-calibrator
Runs weekly after outcome-checker. Aggregates all scored outcomes,
computes accuracy per signal type, and writes updated weights to SSM
so every agent automatically uses better-calibrated signals.
"""

import json
import boto3
import math
from datetime import datetime, timezone
from decimal import Decimal
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

# Phase 2 KA rebrand — recursive khalid_* → ka_* alias helper.
try:
    from ka_aliases import add_ka_aliases
except Exception as _e:
    print(f"WARN: ka_aliases unavailable: {_e}")
    def add_ka_aliases(obj, **_kwargs):
        return obj

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ssm      = boto3.client("ssm",       region_name="us-east-1")
s3       = boto3.client("s3",        region_name="us-east-1")

OUTCOMES_TABLE  = "justhodl-outcomes"
SIGNALS_TABLE   = "justhodl-signals"
S3_BUCKET       = "justhodl-dashboard-live"
SSM_WEIGHTS_PATH = "/justhodl/calibration/weights"
SSM_ACCURACY_PATH = "/justhodl/calibration/accuracy"
SSM_REPORT_PATH  = "/justhodl/calibration/report"

# ─── Default weights (used before enough data exists) ─────────────────────
DEFAULT_WEIGHTS = {
    # ─── Core signals (well-validated)
    "khalid_index":         1.00,
    "screener_top_pick":    0.85,
    "valuation_composite":  0.80,

    # ─── CFTC positioning signals
    "cftc_gold":            0.80,
    "cftc_spx":             0.80,
    "cftc_bitcoin":         0.75,
    "cftc_crude":           0.70,

    # ─── Edge / regime
    "edge_regime":          0.75,
    "edge_composite":       0.70,
    "market_phase":         0.75,

    # ─── Crypto signals
    "crypto_btc_signal":    0.70,
    "crypto_eth_signal":    0.65,
    "crypto_fear_greed":    0.55,  # NOTE: sentiment indicator, accuracy historically low
    "crypto_risk_score":    0.55,  # NOTE: sentiment indicator, accuracy historically low
    "btc_mvrv":             0.70,

    # ─── Risk / stress
    "carry_risk":           0.65,
    "ml_risk":              0.65,
    "plumbing_stress":      0.70,

    # ─── Momentum
    "momentum_spy":         0.55,  # short-horizon, more noise
    "momentum_gld":         0.55,
    "momentum_uso":         0.55,

    # ─── Valuation
    "cape_ratio":           0.75,
    "buffett_indicator":    0.75,

    # ─── Screener individual
    "screener_buy":         0.65,
    "screener_sell":        0.65,
}

# Minimum samples needed before we trust computed accuracy over defaults
MIN_SAMPLES_FOR_WEIGHT = 10


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_float(v) for v in obj]
    return obj


def scan_all(table_name, filter_expr=None):
    """Full table scan with pagination."""
    table   = dynamodb.Table(table_name)
    kwargs  = {"FilterExpression": filter_expr} if filter_expr else {}
    results = table.scan(**kwargs)
    items   = results.get("Items", [])

    while "LastEvaluatedKey" in results:
        kwargs["ExclusiveStartKey"] = results["LastEvaluatedKey"]
        results = table.scan(**kwargs)
        items  += results.get("Items", [])

    return items


def compute_accuracy_stats(outcomes):
    """
    Compute accuracy statistics from a list of outcome records.
    Returns dict with accuracy, precision, recall, avg_return, etc.
    """
    if not outcomes:
        return {"n": 0, "accuracy": None}

    n_total   = len(outcomes)
    n_correct = sum(1 for o in outcomes if o.get("correct") is True)
    n_wrong   = sum(1 for o in outcomes if o.get("correct") is False)
    n_unknown = n_total - n_correct - n_wrong

    # Returns for directional signals
    returns = []
    for o in outcomes:
        outcome_data = o.get("outcome", {})
        ret = outcome_data.get("return_pct") or outcome_data.get("excess_return")
        if ret is not None:
            returns.append(float(ret))

    # Directional breakdown
    ups    = [o for o in outcomes if o.get("outcome", {}).get("actual_direction") == "UP"]
    downs  = [o for o in outcomes if o.get("outcome", {}).get("actual_direction") == "DOWN"]

    # Accuracy when predicted UP
    up_preds    = [o for o in outcomes if o.get("predicted_dir") == "UP"]
    up_correct  = sum(1 for o in up_preds if o.get("correct") is True)

    down_preds   = [o for o in outcomes if o.get("predicted_dir") == "DOWN"]
    down_correct = sum(1 for o in down_preds if o.get("correct") is True)

    stats = {
        "n":              n_total,
        "n_correct":      n_correct,
        "n_wrong":        n_wrong,
        "n_unknown":      n_unknown,
        "accuracy":       round(n_correct / max(n_correct + n_wrong, 1), 4),
        "up_precision":   round(up_correct / max(len(up_preds), 1), 4),
        "down_precision": round(down_correct / max(len(down_preds), 1), 4),
        "avg_return":     round(sum(returns) / len(returns), 4) if returns else None,
        "positive_returns": sum(1 for r in returns if r > 0),
        "negative_returns": sum(1 for r in returns if r < 0),
    }

    # Sharpe-like ratio: avg_return / std_dev of returns
    if len(returns) >= 3:
        mean = stats["avg_return"]
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance)
        stats["sharpe_proxy"] = round(mean / std_dev, 4) if std_dev > 0 else 0
    else:
        stats["sharpe_proxy"] = None

    return stats


def accuracy_to_weight(accuracy, n_samples, default_weight):
    """
    Convert accuracy score to a confidence weight.

    Formula:
    - Less than MIN_SAMPLES: blend default + computed (bayesian shrinkage)
    - >= MIN_SAMPLES: use computed accuracy scaled to [0.3, 1.5]
    - Accuracy > 0.6 → weight > 1.0 (amplify this signal)
    - Accuracy < 0.45 → weight < 0.5 (suppress this signal)
    - Accuracy ~= 0.5 → weight ~= 0.5 (random, not useful)
    """
    if n_samples < MIN_SAMPLES_FOR_WEIGHT:
        # Bayesian blend: shrink toward default
        trust = n_samples / MIN_SAMPLES_FOR_WEIGHT
        computed = max(0.3, min(1.5, accuracy * 2.0))
        return round(default_weight * (1 - trust) + computed * trust, 4)

    # Sigmoid-like scaling: accuracy 50% = weight 0.5, 70% = weight 1.0, 80% = weight 1.4
    # weight = 1.5 * sigmoid(8 * (accuracy - 0.6))
    x       = 8 * (accuracy - 0.6)
    sigmoid = 1 / (1 + math.exp(-x))
    weight  = round(0.3 + 1.2 * sigmoid, 4)
    return weight


def compute_khalid_component_weights(accuracy_by_type):
    """
    Recompute Khalid Index component weights based on signal accuracy.
    Current weights: 75% original + 15% CFTC + 10% smart money

    With calibration, we adjust based on which components are most predictive.
    """
    # Get accuracy for CFTC signals
    cftc_signals    = ["cftc_gold", "cftc_spx", "cftc_bitcoin", "cftc_crude"]
    cftc_accuracies = [accuracy_by_type[s]["accuracy"] for s in cftc_signals
                       if s in accuracy_by_type and accuracy_by_type[s]["accuracy"] is not None]
    cftc_avg = sum(cftc_accuracies) / len(cftc_accuracies) if cftc_accuracies else 0.5

    # Get accuracy for edge/regime
    edge_acc = accuracy_by_type.get("edge_regime", {}).get("accuracy") or 0.5

    # Get accuracy for valuation
    val_acc  = accuracy_by_type.get("valuation_composite", {}).get("accuracy") or 0.5

    # Normalize to weights that sum to ~1.0
    # Base weight is always at least 50% for the core Khalid score
    cftc_w  = max(0.05, min(0.25, cftc_avg * 0.3))
    edge_w  = max(0.05, min(0.15, edge_acc * 0.2))
    val_w   = max(0.05, min(0.15, val_acc  * 0.15))
    core_w  = max(0.50, 1.0 - cftc_w - edge_w - val_w)

    total   = core_w + cftc_w + edge_w + val_w
    return {
        "core_khalid": round(core_w  / total, 4),
        "cftc":        round(cftc_w  / total, 4),
        "edge_regime": round(edge_w  / total, 4),
        "valuation":   round(val_w   / total, 4),
    }


def run_calibration():
    """Full calibration run — the brain of the learning system."""
    now = datetime.now(timezone.utc)

    # ── Load all complete outcomes ─────────────────────────────────────────
    # Filter: correct must be True or False (excludes correct=None / legacy
    # records from pre-baseline-fix era — see step 163, 2026-04-25). Also
    # explicitly exclude is_legacy=true tagged records as defense in depth
    # (they\'ll auto-purge via TTL ~30 days from tagging).
    all_outcomes = scan_all(
        OUTCOMES_TABLE,
        (Attr("correct").eq(True) | Attr("correct").eq(False)) &
        Attr("is_legacy").ne(True)
    )
    all_outcomes = [decimal_to_float(o) for o in all_outcomes]

    print(f"[CALIBRATE] Total outcomes to analyze: {len(all_outcomes)}")

    # ── Group by signal type ───────────────────────────────────────────────
    by_type = defaultdict(list)
    for outcome in all_outcomes:
        stype = outcome.get("signal_type")
        if stype:
            by_type[stype].append(outcome)

    # ── Group by window (short/medium/long term) ──────────────────────────
    by_type_window = defaultdict(lambda: defaultdict(list))
    for outcome in all_outcomes:
        stype  = outcome.get("signal_type")
        window = outcome.get("window_key")
        if stype and window:
            by_type_window[stype][window].append(outcome)

    # ── Compute accuracy per signal type ──────────────────────────────────
    accuracy_by_type   = {}
    weights            = {}

    for stype, outcomes in by_type.items():
        stats  = compute_accuracy_stats(outcomes)
        accuracy_by_type[stype] = stats

        default_weight = DEFAULT_WEIGHTS.get(stype, 0.7)
        acc = stats.get("accuracy")
        if acc is not None:
            weights[stype] = accuracy_to_weight(acc, stats["n"], default_weight)
        else:
            weights[stype] = default_weight

        print(f"[CALIBRATE] {stype}: "
              f"n={stats['n']} accuracy={acc:.2%} → weight={weights[stype]:.3f}"
              if acc is not None else
              f"[CALIBRATE] {stype}: n={stats['n']} → no accuracy yet, using default={default_weight}")

    # Fill in defaults for any signal types with no data yet
    for stype, default_w in DEFAULT_WEIGHTS.items():
        if stype not in weights:
            weights[stype] = default_w

    # ── Compute per-window accuracy (short vs medium vs long term) ─────────
    window_accuracy = {}
    for stype, windows in by_type_window.items():
        window_accuracy[stype] = {}
        for window, outcomes in windows.items():
            stats = compute_accuracy_stats(outcomes)
            window_accuracy[stype][window] = stats

    # ── Recompute Khalid Index component weights ───────────────────────────
    khalid_component_weights = compute_khalid_component_weights(accuracy_by_type)
    print(f"[CALIBRATE] Khalid weights: {khalid_component_weights}")

    # ── Build full calibration report ────────────────────────────────────
    report = {
        "generated_at":           now.isoformat(),
        "total_outcomes":         len(all_outcomes),
        "signal_types_tracked":   len(by_type),
        "weights":                weights,
        "accuracy_by_type":       accuracy_by_type,
        "window_accuracy":        window_accuracy,
        "khalid_component_weights": khalid_component_weights,
        "top_performing_signals": [],
        "worst_performing_signals": [],
        "recommendations":        [],
    }

    # Find best/worst signals
    ranked = sorted(
        [(stype, stats) for stype, stats in accuracy_by_type.items()
         if stats.get("accuracy") is not None and stats.get("n", 0) >= 5],
        key=lambda x: x[1]["accuracy"],
        reverse=True
    )
    report["top_performing_signals"]   = [
        {"type": s, "accuracy": round(v["accuracy"], 3), "n": v["n"]}
        for s, v in ranked[:3]
    ]
    report["worst_performing_signals"] = [
        {"type": s, "accuracy": round(v["accuracy"], 3), "n": v["n"]}
        for s, v in ranked[-3:]
    ]

    # Generate recommendations
    recs = []
    for stype, stats in accuracy_by_type.items():
        acc = stats.get("accuracy")
        n   = stats.get("n", 0)
        if acc is None or n < 5:
            continue
        if acc < 0.45:
            recs.append(f"⚠️ {stype}: accuracy {acc:.0%} (n={n}) — consider removing or inverting this signal")
        elif acc > 0.65:
            recs.append(f"✅ {stype}: accuracy {acc:.0%} (n={n}) — high confidence, increase weighting")
        elif acc > 0.55:
            recs.append(f"👍 {stype}: accuracy {acc:.0%} (n={n}) — performing above random")

    report["recommendations"] = recs

    # ── Save to SSM ────────────────────────────────────────────────────────
    ssm.put_parameter(
        Name=SSM_WEIGHTS_PATH,
        Value=json.dumps(weights),
        Type="String",
        Overwrite=True
    )
    ssm.put_parameter(
        Name=SSM_ACCURACY_PATH,
        Value=json.dumps({k: {
            "accuracy": v.get("accuracy"),
            "n": v.get("n"),
            "avg_return": v.get("avg_return"),
        } for k, v in accuracy_by_type.items()}),
        Type="String",
        Overwrite=True
    )
    # Phase 2 dual-write — duplicate khalid_component_weights, khalid_new_weights,
    # khalid_index keys throughout the report dict as ka_* aliases. All
    # downstream serializations (SSM_REPORT_PATH + 2 S3 writes) inherit them.
    report = add_ka_aliases(report)
    ssm.put_parameter(
        Name=SSM_REPORT_PATH,
        Value=json.dumps(report),
        Type="String",
        Overwrite=True
    )
    print(f"[CALIBRATE] Weights and accuracy saved to SSM")

    # ── Save full report to S3 ────────────────────────────────────────────
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="calibration/latest.json",
        Body=json.dumps(report, indent=2, default=str),
        ContentType="application/json"
    )

    # Keep history
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"calibration/history/{now.strftime('%Y-%m-%d')}.json",
        Body=json.dumps(report, indent=2, default=str),
        ContentType="application/json"
    )
    print(f"[CALIBRATE] Report saved to S3: calibration/latest.json")

    return report


def lambda_handler(event, context):
    report = run_calibration()

    print("\n=== CALIBRATION SUMMARY ===")
    print(f"Total outcomes analyzed: {report['total_outcomes']}")
    print(f"Khalid components: {report['khalid_component_weights']}")
    print("Recommendations:")
    for rec in report.get("recommendations", []):
        print(f"  {rec}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "success":              True,
            "total_outcomes":       report["total_outcomes"],
            "weights_updated":      report["weights"],
            "top_signals":          report["top_performing_signals"],
            "recommendations":      report["recommendations"],
            "khalid_new_weights":   report["khalid_component_weights"],
        }, default=str)
    }
