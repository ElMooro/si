"""justhodl-calibration-snapshotter

Snapshots the live calibration state every Sunday 00:00 UTC into a versioned ledger
so we can chart how each signal's weight and accuracy evolves week-over-week.

Reads:
  - SSM /justhodl/calibration/weights      → {signal: weight}
  - SSM /justhodl/calibration/accuracy     → {signal: accuracy}
  - DynamoDB justhodl-outcomes (last 60d)  → outcome counts per signal type

Writes:
  - calibration/history/{ISO_WEEK}.json    → one snapshot per week (append-only)
  - calibration/history-index.json         → manifest of all snapshots
  - calibration/latest.json                → pointer to the most recent snapshot

Schedule: cron(5 0 ? * SUN *) — Sundays 00:05 UTC, after the calibrator runs at 09:00.
Actually we want this AFTER the calibrator updates SSM (calibrator runs Sundays 09:00 UTC),
so we schedule for Sundays 12:00 UTC: cron(0 12 ? * SUN *).

This is bootstrapped with today's snapshot as week 1 even on Mon-Sat first deploy.

Output schema:
{
  "as_of": "2026-05-04T20:30:00Z",
  "iso_week": "2026-W18",
  "iso_year": 2026, "iso_week_num": 18,
  "week_start": "2026-04-27", "week_end": "2026-05-03",
  "weights": {"signal_name": float, ...},
  "accuracy": {"signal_name": float, ...},
  "outcome_counts_60d": {"signal_name": int, ...},
  "summary": {
    "n_weights_total": 32,
    "n_signals_calibrated_n30": 12,
    "highest_weight": {"signal": "crisis_hy_oas_vs_hyg", "weight": 1.42},
    "lowest_weight": {"signal": "...", "weight": 0.1},
    "median_weight": 0.5,
    "weighted_mean_accuracy": 0.62
  },
  "v": "1.0"
}
"""
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)


def safe_get_ssm(name):
    try:
        v = SSM.get_parameter(Name=name)["Parameter"]["Value"]
        return json.loads(v) if v.lstrip().startswith(("{", "[")) else None
    except Exception as e:
        print(f"[ssm] {name}: {e}")
        return None


def count_outcomes_60d():
    """Count non-legacy outcomes per signal_type over last 60 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    tbl = DDB.Table("justhodl-outcomes")
    counts = Counter()
    last_key = None
    pages = 0
    while True:
        kw = {
            "Limit": 1000,
            "FilterExpression": Attr("checked_at").gte(cutoff) & Attr("is_legacy").ne(True),
            "ProjectionExpression": "signal_type",
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = tbl.scan(**kw)
        for it in resp.get("Items", []):
            counts[it.get("signal_type", "?")] += 1
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 12:
            break
    return dict(counts), pages


def iso_week_label(dt):
    """Return (label, iso_year, iso_week_num, week_start, week_end)."""
    iso_year, iso_week, iso_weekday = dt.isocalendar()
    label = f"{iso_year}-W{iso_week:02d}"
    # Compute Monday of that ISO week
    monday = dt - timedelta(days=iso_weekday - 1)
    sunday = monday + timedelta(days=6)
    return label, iso_year, iso_week, monday.date().isoformat(), sunday.date().isoformat()


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)
    print(f"[snapshotter] starting at {now.isoformat()}")

    # 1. Pull SSM
    weights = safe_get_ssm("/justhodl/calibration/weights") or {}
    accuracy = safe_get_ssm("/justhodl/calibration/accuracy") or {}
    print(f"[snapshotter] weights: {len(weights)}, accuracy: {len(accuracy)}")

    if not weights:
        return {"statusCode": 500, "body": json.dumps({"error": "no weights in SSM"})}

    # 2. Count outcomes per signal_type
    outcome_counts, pages = count_outcomes_60d()
    print(f"[snapshotter] outcome counts: {len(outcome_counts)} types ({pages} pages)")

    # 3. Build summary
    n_weights = len(weights)
    n_calibrated_n30 = sum(1 for k, v in outcome_counts.items() if v >= 30)
    weight_pairs = sorted(weights.items(), key=lambda x: -float(x[1]))
    median_weight = sorted([float(v) for v in weights.values()])[n_weights // 2] if n_weights else 0

    # Weighted-mean accuracy: sum(weight * acc) / sum(weights for signals with both)
    num = denom = 0.0
    for sig, w in weights.items():
        a = accuracy.get(sig)
        if a is not None:
            num += float(w) * float(a)
            denom += float(w)
    weighted_mean_acc = (num / denom) if denom > 0 else None

    label, iso_year, iso_week, week_start, week_end = iso_week_label(now)

    snapshot = {
        "v": "1.0",
        "as_of": now.isoformat(),
        "iso_week": label,
        "iso_year": iso_year,
        "iso_week_num": iso_week,
        "week_start": week_start,
        "week_end": week_end,
        "weights": {k: float(v) for k, v in weights.items()},
        "accuracy": {k: float(v) for k, v in accuracy.items()},
        "outcome_counts_60d": outcome_counts,
        "summary": {
            "n_weights_total": n_weights,
            "n_accuracy_keys": len(accuracy),
            "n_signals_calibrated_n30": n_calibrated_n30,
            "highest_weight": {"signal": weight_pairs[0][0], "weight": float(weight_pairs[0][1])} if weight_pairs else None,
            "lowest_weight": {"signal": weight_pairs[-1][0], "weight": float(weight_pairs[-1][1])} if weight_pairs else None,
            "median_weight": float(median_weight),
            "weighted_mean_accuracy": round(weighted_mean_acc, 4) if weighted_mean_acc is not None else None,
        },
        "duration_s": round(time.time() - started, 2),
    }

    # 4. Write versioned snapshot (idempotent — overwrites if same week ran twice)
    snapshot_key = f"calibration/history/{label}.json"
    body = json.dumps(snapshot, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=snapshot_key, Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=3600",
    )

    # Latest pointer
    S3.put_object(
        Bucket=BUCKET, Key="calibration/latest.json", Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    # 5. Update history-index manifest
    try:
        existing = json.loads(S3.get_object(Bucket=BUCKET, Key="calibration/history-index.json")["Body"].read())
    except Exception:
        existing = {"v": "1.0", "snapshots": []}

    snapshots = existing.get("snapshots", [])
    # Remove any existing entry for this week, then append
    snapshots = [s for s in snapshots if s.get("iso_week") != label]
    snapshots.append({
        "iso_week": label,
        "iso_year": iso_year,
        "iso_week_num": iso_week,
        "as_of": now.isoformat(),
        "week_start": week_start,
        "week_end": week_end,
        "key": snapshot_key,
        "size_bytes": len(body),
        "n_weights": n_weights,
        "n_calibrated_n30": n_calibrated_n30,
    })
    snapshots.sort(key=lambda x: (x["iso_year"], x["iso_week_num"]))

    index = {
        "v": "1.0",
        "last_updated": now.isoformat(),
        "n_snapshots": len(snapshots),
        "snapshots": snapshots,
    }
    S3.put_object(
        Bucket=BUCKET, Key="calibration/history-index.json",
        Body=json.dumps(index, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    print(f"[snapshotter] wrote {snapshot_key} ({len(body):,}b), index has {len(snapshots)} snapshots")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "iso_week": label,
            "n_weights": n_weights,
            "n_calibrated_n30": n_calibrated_n30,
            "n_snapshots_total": len(snapshots),
            "duration_s": snapshot["duration_s"],
        }),
    }
