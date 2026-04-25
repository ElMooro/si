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
    """Group outcomes by signal_type and compute metrics.

    Critical: hit_rate is computed over SCORED outcomes only (those
    where correct is True or False). Unscored outcomes (correct=None)
    are not yet eligible for scoring and shouldn't drag the rate down.
    """
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
        # Filter to scored outcomes (correct is explicitly True or False).
        # correct=None means not yet scored or unscoreable.
        scored_items = [i for i in items if i.get("correct") in (True, False)]
        scored = len(scored_items)
        correct = sum(1 for i in scored_items if i.get("correct") is True)
        hit_rate = (correct / scored) if scored else None

        # Magnitude error — only over scored items, requires both
        # predicted_magnitude and actual_change.
        mag_errors = []
        for i in scored_items:
            pred = i.get("signal", {}).get("predicted_magnitude_pct")
            actual = i.get("actual_change_pct") or i.get("actual_pct")
            if pred is not None and actual is not None:
                try:
                    mag_errors.append(abs(float(pred) - float(actual)))
                except Exception:
                    pass
        avg_mag_err = (sum(mag_errors) / len(mag_errors)) if mag_errors else None

        # By horizon — also filter to scored only
        by_horizon = defaultdict(lambda: {"total": 0, "correct": 0, "scored": 0})
        for i in items:
            h = i.get("horizon_days") or i.get("signal", {}).get("horizon_days_primary")
            try: h = int(h) if h is not None else None
            except Exception: h = None
            if h is None:
                continue
            by_horizon[h]["total"] += 1
            if i.get("correct") in (True, False):
                by_horizon[h]["scored"] += 1
                if i.get("correct") is True:
                    by_horizon[h]["correct"] += 1
        for h in by_horizon:
            sc = by_horizon[h]["scored"]
            by_horizon[h]["hit_rate"] = (by_horizon[h]["correct"] / sc) if sc else None

        # Trend over time windows (using scored_at) — scored items only
        def window_hit_rate(days):
            cutoff = now - timedelta(days=days)
            in_window = [i for i in scored_items
                         if (parse_iso(i.get("scored_at") or i.get("checked_at") or i.get("logged_at"))
                             or datetime(1970, 1, 1, tzinfo=timezone.utc)) >= cutoff]
            if not in_window: return None
            c = sum(1 for i in in_window if i.get("correct") is True)
            return c / len(in_window) if in_window else None

        scorecard.append({
            "signal_type": st,
            "total": total,           # all outcomes
            "scored": scored,         # only correct in {True, False}
            "correct": correct,       # only correct=True
            "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
            "avg_magnitude_error_pct": round(avg_mag_err, 3) if avg_mag_err is not None else None,
            "by_horizon": dict(by_horizon),
            "trend_30d": window_hit_rate(30),
            "trend_60d": window_hit_rate(60),
            "trend_90d": window_hit_rate(90),
        })

    # Sort by total desc (most data first)
    scorecard.sort(key=lambda x: -x["total"])
    return scorecard


def compute_khalid_timeline(signals):
    """Extract Khalid Index timeline from logged signals.

    Strategy:
      - Filter to signals where signal_type == 'khalid_index'
      - Use signal_value (the score) and metadata.regime
      - Return ALL points sorted by timestamp (no date-grouping —
        intra-day movement matters when our history is recent)
      - Trim to last 90 days (currently won't trim much; we have
        only ~1 day of khalid_index data so far)
    """
    points = []
    for s in signals:
        if s.get("signal_type") != "khalid_index":
            continue
        ts = s.get("logged_at")
        sv = s.get("signal_value")
        if sv is None or not ts:
            continue
        # signal_value can be a string like "43" or a Decimal/float — coerce
        try:
            if isinstance(sv, str):
                score = float(sv.replace("%", "").strip())
            else:
                score = float(sv)
        except Exception:
            continue
        meta = s.get("metadata") or {}
        regime = meta.get("regime") or s.get("regime_at_log") or s.get("regime")
        dt = parse_iso(ts)
        if not dt:
            continue
        points.append({
            "ts": dt.isoformat(),
            "date": dt.date().isoformat(),
            "score": score,
            "regime": regime,
        })

    # Fallback if no khalid_index signals: try khalid_score_at_log
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
                "ts": dt.isoformat(),
                "date": dt.date().isoformat(),
                "score": score_f,
                "regime": regime,
            })

    # Sort by timestamp; keep ALL points (no date-grouping)
    points.sort(key=lambda x: x["ts"])

    # Trim to last 90 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    return [p for p in points if p["ts"] >= cutoff]


def compute_morning_archive(s3_client, days=30):
    """Build a daily morning brief archive from archive/intelligence/.

    For each day in the past `days` days, picks the snapshot closest
    to 12:05 UTC (= 8:05 ET, when morning-intelligence runs). Extracts
    canonical fields suitable for display in reports.html Section 1.
    """
    from datetime import datetime, timezone, timedelta, date
    bucket = "justhodl-dashboard-live"

    # Collect intelligence/ keys for the last `days` days
    today = datetime.now(timezone.utc).date()
    cutoff_date = today - timedelta(days=days)
    keys_by_date = {}  # date -> list of (key, hhmm_distance_from_1205)

    paginator = s3_client.get_paginator("list_objects_v2")
    # Iterate the days backwards (today back to cutoff)
    for offset in range(days):
        d = today - timedelta(days=offset)
        prefix = f"archive/intelligence/{d.year}/{d.month:02d}/{d.day:02d}/"
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    # Filename like 1205.json — strip the .json
                    fn = k.rsplit("/", 1)[-1]
                    m = re.match(r"(\d{4})\.json", fn)
                    if not m:
                        continue
                    hhmm = int(m.group(1))
                    # Distance from 12:05 UTC (= 1205) in minutes
                    h = hhmm // 100
                    minute = hhmm % 100
                    minutes = h * 60 + minute
                    target = 12 * 60 + 5  # 12:05 UTC
                    distance = abs(minutes - target)
                    keys_by_date.setdefault(d, []).append((k, distance))
        except Exception as e:
            print(f"morning_archive: list error for {d}: {e}")

    # For each day, pick the closest-to-1205 key
    chosen = {}
    for d, items in keys_by_date.items():
        items.sort(key=lambda x: x[1])
        chosen[d] = items[0][0]

    # Fetch each chosen snapshot and build the archive entry
    archive = []
    for d in sorted(chosen.keys(), reverse=True):  # newest first
        k = chosen[d]
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=k)
            data = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as e:
            print(f"morning_archive: fetch error for {k}: {e}")
            continue

        # Extract canonical fields. Use .get with defaults so missing
        # fields don't break.
        scores = data.get("scores") or {}
        forecast = data.get("forecast") or {}
        archive.append({
            "date": d.isoformat(),
            "key": k,
            "generated_at": data.get("generated_at") or data.get("timestamp"),
            "regime": data.get("regime"),
            "phase": data.get("phase"),
            "phase_color": data.get("phase_color"),
            "headline": data.get("headline"),
            "headline_detail": data.get("headline_detail"),
            "action_required": data.get("action_required"),
            "khalid_score": scores.get("khalid_index") or scores.get("khalid"),
            "carry_risk": scores.get("carry_risk"),
            "ml_risk": scores.get("ml_risk") or scores.get("ml_intelligence"),
            "plumbing": scores.get("plumbing_stress") or scores.get("plumbing"),
            "vix": data.get("vix") or scores.get("vix"),
            "forecast_summary": forecast.get("summary") if isinstance(forecast, dict) else None,
            "risks_count": len(data.get("risks") or []),
            "signal_count": len(data.get("signals") or []),
        })

    return archive


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
    morning_archive = []
    try:
        morning_archive = compute_morning_archive(s3, days=30)
    except Exception as e:
        print(f"morning_archive failed: {e}")

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
        "morning_archive": morning_archive,
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
            "morning_archive_days": len(morning_archive),
            "signals_seen": len(signals),
            "outcomes_seen": len(outcomes),
        }),
    }
