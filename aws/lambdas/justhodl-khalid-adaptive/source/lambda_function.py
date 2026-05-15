"""
justhodl-khalid-adaptive — Adaptive Khalid Index using calibrator-learned weights.

Builds an A/B parallel score next to the standard Khalid Index. Standard uses
hardcoded blend (75% core + 15% CFTC + 10% smart_money). Adaptive uses the
calibrator's 60-day rolling accuracy per signal type as the blend weights.

When standard and adaptive diverge significantly (>15pt), the divergence is the
signal — the learned weights "see" something the hardcoded weights don't.

Schedule: cron(20 * ? * * *) — hourly at :20, after daily-report-v3 (:05) +
calibrator updates throughout the day.

Outputs: data/khalid-adaptive.json with both scores, components, divergence,
top contributors, and history-aware regime classification.

Telegram alert when:
  - Adaptive regime changes (vs prior run)
  - Standard vs adaptive divergence > 15 points (the system is "seeing
    two different markets")
"""
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/khalid-adaptive.json"
S3_KEY_HISTORY = "data/khalid-adaptive-history.json"
S3_CALIBRATION = "data/calibration-snapshot.json"
S3_REPORT = "data/report.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Regime thresholds (match daily-report-v3 conventions)
REGIME_THRESHOLDS = [
    (75, "STRONG_BULL"),
    (60, "BULL"),
    (45, "NEUTRAL"),
    (30, "BEAR"),
    (-1, "STRONG_BEAR"),
]

# Min accuracy floor — signals below this contribute nothing
MIN_ACCURACY_60D = 0.35
# Min observations for a signal to be considered (avoid noise)
MIN_OBS_60D = 20
# History buffer size
MAX_HISTORY = 168  # 7 days * 24 hours

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def direction_to_int(d):
    """Convert direction string to -1/0/+1."""
    if not d: return 0
    d = str(d).upper()
    if d in ("UP", "BULL", "BULLISH", "RISK_ON", "POSITIVE"): return 1
    if d in ("DOWN", "BEAR", "BEARISH", "RISK_OFF", "NEGATIVE"): return -1
    return 0


def regime_for_score(score):
    for threshold, name in REGIME_THRESHOLDS:
        if score >= threshold:
            return name
    return "STRONG_BEAR"


def compute_adaptive_score(calibration, report):
    """Compute adaptive Khalid Index using calibrator weights as blend."""
    signals = calibration.get("signals", []) if calibration else []

    contributions = []
    total_abs_weight = 0
    weighted_sum = 0

    for sig in signals:
        sig_type = sig.get("signal_type", "")
        weight = sig.get("weight", 1.0)
        rolling = sig.get("rolling", {})
        latest = sig.get("latest", {}) or {}
        accuracy_60d = rolling.get("accuracy_60d")
        n_60d = rolling.get("n_60d", 0)

        # Skip noisy signals
        if accuracy_60d is None or accuracy_60d < MIN_ACCURACY_60D:
            continue
        if (n_60d or 0) < MIN_OBS_60D:
            continue

        direction = direction_to_int(latest.get("direction"))
        confidence = latest.get("confidence", 0.5) or 0.5

        # Per-signal contribution: weight × accuracy × direction × confidence
        # If direction is unknown (0), use signal value sign instead
        if direction == 0:
            val = latest.get("signal_value")
            try:
                v = float(val) if val is not None else 0
                # Sign-based fallback when value is numeric
                if v > 0: direction = 1
                elif v < 0: direction = -1
            except (TypeError, ValueError):
                pass  # leave direction at 0

        effective_weight = weight * accuracy_60d
        contribution = effective_weight * direction * confidence

        weighted_sum += contribution
        total_abs_weight += effective_weight

        contributions.append({
            "signal_type": sig_type,
            "weight": round(weight, 3),
            "accuracy_60d": round(accuracy_60d, 3),
            "n_60d": int(n_60d or 0),
            "direction": direction,
            "confidence": round(confidence, 3),
            "contribution": round(contribution, 4),
            "effective_weight": round(effective_weight, 4),
        })

    if total_abs_weight == 0:
        return {
            "adaptive_score": 50.0,
            "raw_composite": 0.0,
            "n_signals_used": 0,
            "contributions": [],
            "error": "no_qualifying_signals",
        }

    # Normalize to 0-100 with 50 as neutral
    # raw_composite is in [-1, +1] domain
    raw_composite = weighted_sum / total_abs_weight
    adaptive_score = 50 + (raw_composite * 50)
    adaptive_score = max(0, min(100, adaptive_score))

    contributions.sort(key=lambda c: -abs(c["contribution"]))

    return {
        "adaptive_score": round(adaptive_score, 1),
        "raw_composite": round(raw_composite, 4),
        "n_signals_used": len(contributions),
        "total_effective_weight": round(total_abs_weight, 3),
        "top_5_contributors": contributions[:5],
        "bottom_5_contributors": contributions[-5:] if len(contributions) > 5 else [],
        "all_contributors_count": len(contributions),
    }


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds, would send: {msg[:80]}")
        return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[khalid-adaptive] starting")

    # Load inputs
    calibration = get_s3_json(S3_CALIBRATION, {})
    report = get_s3_json(S3_REPORT, {})
    prior_run = get_s3_json(S3_KEY_OUT, {})

    # Extract standard Khalid Index
    ki_standard = report.get("khalid_index", {}) if isinstance(report.get("khalid_index"), dict) else {}
    standard_score = float(ki_standard.get("score", 50)) if ki_standard else 50.0
    standard_regime = ki_standard.get("regime", "UNKNOWN")
    standard_signals = ki_standard.get("signals", [])

    # Compute adaptive
    adaptive = compute_adaptive_score(calibration, report)
    adaptive_score = adaptive["adaptive_score"]
    adaptive_regime = regime_for_score(adaptive_score)

    # Divergence
    divergence = round(adaptive_score - standard_score, 1)

    # Build output
    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "standard": {
            "score": round(standard_score, 1),
            "regime": standard_regime,
            "method": "hardcoded_blend_75_15_10",
            "n_signals": len(standard_signals) if isinstance(standard_signals, list) else 0,
        },
        "adaptive": {
            "score": adaptive["adaptive_score"],
            "regime": adaptive_regime,
            "method": "calibrator_60d_weighted_v1",
            "raw_composite": adaptive["raw_composite"],
            "n_signals_used": adaptive["n_signals_used"],
            "total_effective_weight": adaptive.get("total_effective_weight"),
        },
        "divergence": {
            "score_delta": divergence,
            "regime_match": standard_regime == adaptive_regime,
            "interpretation": (
                "Aligned" if abs(divergence) <= 5 else
                "Mild divergence" if abs(divergence) <= 10 else
                "Moderate divergence — learned weights see different signal" if abs(divergence) <= 20 else
                "Strong divergence — significant disagreement between hardcoded and learned"
            ),
        },
        "top_contributors": adaptive.get("top_5_contributors", []),
        "drag_contributors": adaptive.get("bottom_5_contributors", []),
        "calibration_meta": {
            "version": calibration.get("version"),
            "generated_at": calibration.get("generated_at"),
            "weighted_avg_accuracy_60d": (calibration.get("summary", {}) or {}).get("weighted_avg_accuracy_60d"),
            "n_signal_types_tracked": (calibration.get("summary", {}) or {}).get("n_signal_types_tracked"),
        },
        "duration_s": round(time.time() - t0, 2),
    }

    put_s3_json(S3_KEY_OUT, output)
    print(f"[khalid-adaptive] standard={standard_score:.1f}({standard_regime}) "
          f"adaptive={adaptive_score:.1f}({adaptive_regime}) divergence={divergence:+.1f}")

    # Update history sidecar
    try:
        history = get_s3_json(S3_KEY_HISTORY, {"snapshots": []})
        snaps = history.get("snapshots", [])
        snaps.append({
            "ts": output["generated_at"],
            "standard_score": output["standard"]["score"],
            "adaptive_score": output["adaptive"]["score"],
            "standard_regime": output["standard"]["regime"],
            "adaptive_regime": output["adaptive"]["regime"],
            "divergence": divergence,
        })
        snaps = snaps[-MAX_HISTORY:]
        put_s3_json(S3_KEY_HISTORY, {"snapshots": snaps,
                                       "updated_at": output["generated_at"]})
    except Exception as e:
        print(f"[history] err: {e}")

    # ─── ALERTS ────────────────────────────────────────────────────────
    try:
        prior_adaptive_regime = (prior_run.get("adaptive", {}) or {}).get("regime")
        prior_divergence = (prior_run.get("divergence", {}) or {}).get("score_delta", 0)

        # 1. Adaptive regime change
        if prior_adaptive_regime and prior_adaptive_regime != adaptive_regime:
            top_str = ", ".join(
                f"{c['signal_type']} (w={c['effective_weight']:.2f}, d={c['direction']:+d})"
                for c in adaptive.get("top_5_contributors", [])[:3]
            )
            maybe_telegram(
                f"🎯 <b>ADAPTIVE KHALID REGIME CHANGE</b>\n"
                f"<b>{prior_adaptive_regime} → {adaptive_regime}</b>\n"
                f"Adaptive: {adaptive_score:.1f}  ·  Standard: {standard_score:.1f}\n"
                f"Top drivers: {top_str}\n\n"
                f"<a href='https://justhodl.ai/composite/'>justhodl.ai/composite/</a>"
            )

        # 2. Strong divergence (>15pt) when prior was aligned
        if abs(divergence) > 15 and abs(prior_divergence) <= 15:
            direction_str = "adaptive MORE bullish" if divergence > 0 else "adaptive MORE bearish"
            maybe_telegram(
                f"⚠️ <b>STANDARD vs ADAPTIVE DIVERGENCE: {divergence:+.1f}pt</b>\n"
                f"Standard: {standard_score:.1f} ({standard_regime})\n"
                f"Adaptive: {adaptive_score:.1f} ({adaptive_regime})\n"
                f"Learned weights say {direction_str}.\n"
                f"The 60-day calibrator is seeing something the hardcoded blend isn't."
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "standard_score": output["standard"]["score"],
            "adaptive_score": output["adaptive"]["score"],
            "divergence": divergence,
            "adaptive_regime": adaptive_regime,
        }),
    }
