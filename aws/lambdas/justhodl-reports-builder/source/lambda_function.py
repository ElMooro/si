"""
justhodl-reports-builder

Reads SSM calibration + DynamoDB signals/outcomes, computes per-signal
scorecard + Khalid Index timeline, writes scorecard.json to S3 for
reports.html to consume.

Schedule: rate(1 hour) — calibration weights only update weekly but
hourly keeps Khalid timeline fresh.
"""
import boto3
import json
import os
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

S3_BUCKET = "justhodl-dashboard-live"
SCORECARD_KEY = "reports/scorecard.json"

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def d2f(o):
    if isinstance(o, Decimal): return float(o)
    if isinstance(o, dict):    return {k: d2f(v) for k, v in o.items()}
    if isinstance(o, list):    return [d2f(v) for v in o]
    return o


def get_ssm_json(name):
    try:
        v = ssm.get_parameter(Name=name)["Parameter"]["Value"]
        return json.loads(v)
    except Exception:
        return None


def scan_table(table_name, since_iso=None, max_items=10000):
    """Full scan of a table, optionally filtered by logged_at >= since_iso."""
    t = dynamodb.Table(table_name)
    items = []
    kwargs = {}
    while True:
        resp = t.scan(**kwargs)
        for item in resp.get("Items", []):
            items.append(d2f(item))
            if len(items) >= max_items:
                return items
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def signal_type_of(s):
    """Pull the signal_type/source from a signal item."""
    return s.get("signal_type") or s.get("source") or s.get("signal") or "unknown"


def parse_iso(s):
    if not s: return None
    try:
        # Strip trailing Z and parse
        s = str(s).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def compute_scorecard(signals, outcomes):
    """Group outcomes by signal_type and compute metrics."""
    # Build signal_id -> signal map
    sig_by_id = {s.get("signal_id"): s for s in signals if s.get("signal_id")}

    # Group outcomes by signal_type
    by_type = defaultdict(list)
    for o in outcomes:
        sid = o.get("signal_id")
        sig = sig_by_id.get(sid)
        if not sig:
            continue
        st = signal_type_of(sig)
        by_type[st].append({**o, "signal": sig})

    now = datetime.now(timezone.utc)
    scorecard = []
    for st, items in by_type.items():
        total = len(items)
        correct = sum(1 for i in items if i.get("correct") is True)
        hit_rate = (correct / total) if total else 0.0

        # Magnitude error
        mag_errors = []
        for i in items:
            pred = i.get("signal", {}).get("predicted_magnitude_pct")
            actual = i.get("actual_change_pct") or i.get("actual_pct")
            if pred is not None and actual is not None:
                try:
                    mag_errors.append(abs(float(pred) - float(actual)))
                except Exception:
                    pass
        avg_mag_err = (sum(mag_errors) / len(mag_errors)) if mag_errors else None

        # By horizon
        by_horizon = defaultdict(lambda: {"total": 0, "correct": 0})
        for i in items:
            h = i.get("horizon_days") or i.get("signal", {}).get("horizon_days_primary")
            try: h = int(h) if h is not None else None
            except Exception: h = None
            if h is None:
                continue
            by_horizon[h]["total"] += 1
            if i.get("correct") is True:
                by_horizon[h]["correct"] += 1
        for h in by_horizon:
            t_ = by_horizon[h]["total"]
            by_horizon[h]["hit_rate"] = (by_horizon[h]["correct"] / t_) if t_ else 0.0

        # Trend over time windows (using scored_at)
        def window_hit_rate(days):
            cutoff = now - timedelta(days=days)
            in_window = [i for i in items
                         if (parse_iso(i.get("scored_at") or i.get("logged_at"))
                             or datetime(1970, 1, 1, tzinfo=timezone.utc)) >= cutoff]
            if not in_window: return None
            c = sum(1 for i in in_window if i.get("correct") is True)
            return c / len(in_window) if in_window else None

        scorecard.append({
            "signal_type": st,
            "total": total,
            "correct": correct,
            "hit_rate": round(hit_rate, 4),
            "avg_magnitude_error_pct": round(avg_mag_err, 3) if avg_mag_err is not None else None,
            "by_horizon": dict(by_horizon),
            "trend_30d": window_hit_rate(30),
            "trend_60d": window_hit_rate(60),
            "trend_90d": window_hit_rate(90),
        })

    # Sort by sample size desc
    scorecard.sort(key=lambda x: -x["total"])
    return scorecard


def compute_khalid_timeline(signals):
    """Extract Khalid Index timeline from logged signals.

    Two strategies (in priority order):
      1. Use signals where signal_type == 'khalid_index' — these are
         the dedicated Khalid Index logs (signal_value is the score,
         metadata.regime is the regime label).
      2. Fall back to khalid_score_at_log on signals with that field
         populated (Week 2A schema v2).
    """
    points = []

    # Strategy 1: dedicated khalid_index signals
    for s in signals:
        if s.get("signal_type") != "khalid_index":
            continue
        ts = s.get("logged_at")
        sv = s.get("signal_value")
        if sv is None or not ts:
            continue
        try:
            score = float(sv) if not isinstance(sv, str) else float(str(sv).replace("%", "").strip())
        except Exception:
            continue
        regime = (s.get("metadata") or {}).get("regime") or s.get("regime")
        dt = parse_iso(ts)
        if not dt:
            continue
        points.append({
            "date": dt.date().isoformat(),
            "ts": dt.isoformat(),
            "score": score,
            "regime": regime,
        })

    # Strategy 2: fallback to khalid_score_at_log on any signal
    if len(points) < 5:
        for s in signals:
            score = s.get("khalid_score_at_log")
            ts = s.get("logged_at")
            if score is None or not ts:
                continue
            try:
                score_f = float(score)
            except Exception:
                continue
            regime = s.get("regime_at_log")
            dt = parse_iso(ts)
            if not dt:
                continue
            points.append({
                "date": dt.date().isoformat(),
                "ts": dt.isoformat(),
                "score": score_f,
                "regime": regime,
            })

    # Group by date, take first reading of each day
    by_date = OrderedDict()
    for p in sorted(points, key=lambda x: x["ts"]):
        if p["date"] not in by_date:
            by_date[p["date"]] = p

    timeline = list(by_date.values())
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
    return [p for p in timeline if p["date"] >= cutoff]


def lambda_handler(event, context):
    """Build scorecard.json from SSM + DynamoDB and write to S3."""
    # 1. SSM calibration data
    weights = get_ssm_json("/justhodl/calibration/weights") or {}
    accuracy = get_ssm_json("/justhodl/calibration/accuracy") or {}
    calib_report = get_ssm_json("/justhodl/calibration/report") or {}

    # 2. DDB scans
    signals = scan_table("justhodl-signals", max_items=15000)
    outcomes = scan_table("justhodl-outcomes", max_items=15000)
    print(f"signals={len(signals)} outcomes={len(outcomes)}")

    # 3. Compute scorecard
    scorecard = compute_scorecard(signals, outcomes)
    # Merge calibrator weight + accuracy if available
    for row in scorecard:
        st = row["signal_type"]
        if isinstance(weights, dict) and st in weights:
            row["calibrator_weight"] = weights[st]
        if isinstance(accuracy, dict) and st in accuracy:
            row["calibrator_accuracy"] = accuracy[st]

    # 4. Khalid timeline
    timeline = compute_khalid_timeline(signals)

    # 5. Build output
    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "signals_total": len(signals),
            "outcomes_total": len(outcomes),
            "scored_outcomes": sum(1 for o in outcomes if o.get("correct") is not None),
            "has_calibration": bool(weights and accuracy),
            "calibration_summary": {
                "weights_count": len(weights) if isinstance(weights, dict) else 0,
                "accuracy_count": len(accuracy) if isinstance(accuracy, dict) else 0,
                "report_keys": list(calib_report.keys()) if isinstance(calib_report, dict) else [],
            },
        },
        "signal_scorecard": scorecard,
        "khalid_timeline": timeline,
        "calibration_weights": weights if isinstance(weights, dict) else {},
        "calibration_accuracy": accuracy if isinstance(accuracy, dict) else {},
    }

    # 6. Write to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=SCORECARD_KEY,
        Body=json.dumps(out, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "scorecard_rows": len(scorecard),
            "timeline_points": len(timeline),
            "signals_seen": len(signals),
            "outcomes_seen": len(outcomes),
        }),
    }
