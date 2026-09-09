"""justhodl-calibration-snapshotter

Snapshots the live calibration state every Sunday 00:00 UTC into a versioned ledger
so we can chart how each signal's weight and accuracy evolves week-over-week.

Reads:
  - SSM /justhodl/calibration/weights      → {signal: weight}
  - SSM /justhodl/calibration/accuracy     → {signal: accuracy}
  - DynamoDB justhodl-outcomes (last 60d)  → outcome counts per signal type

Writes:
  - calibration/versions/{ID}.json         → immutable model versions
  - calibration/index.json                → recoverable version index (conditional merge)
  - calibration/history/{ISO_WEEK}.json    → latest model per week (legacy view)
  - calibration/history-index.json         → conditional merged weekly manifest
  - calibration/model-latest.json          → newest model, separate from calibrator report

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
import uuid
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


def _rank(snapshot):
    stamp = snapshot.get("available_at") or snapshot.get("as_of")
    value = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("calibration timestamp must have timezone")
    return value.astimezone(timezone.utc), snapshot.get("snapshot_id", "")


def _cas_json(key, default, merge, attempts=8):
    """A concurrent writer must be merged, never silently overwritten."""
    for _ in range(attempts):
        try:
            found = S3.get_object(Bucket=BUCKET, Key=key)
            current = json.loads(found["Body"].read())
            etag = found.get("ETag")
            if not etag:
                raise ValueError("conditional calibration update requires ETag")
            condition = {"IfMatch": etag}
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("NoSuchKey", "NotFound", "404"):
                raise
            current, condition = default, {"IfNoneMatch": "*"}
        result = merge(current)
        try:
            S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(result, default=str).encode(),
                          ContentType="application/json", CacheControl="public, max-age=300", **condition)
            return result
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("PreconditionFailed", "ConditionalRequestConflict", "412", "409"):
                raise
    raise RuntimeError("calibration conditional publication exhausted retries; immutable versions remain recoverable")


def _version_rows():
    """Rebuild secondary indexes from the authoritative immutable objects.

    If a prior invocation stopped after creating its version, the next run
    recovers it. The backtest independently lists this same immutable prefix.
    """
    rows = []
    for page in S3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="calibration/versions/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            doc = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            if not doc.get("snapshot_id") or not isinstance(doc.get("weights"), dict):
                raise ValueError("invalid immutable calibration version")
            _rank(doc)
            rows.append((key, doc))
    return rows


def _merge_index(existing, recovered, field, identity):
    current = existing.get(field, [])
    if not isinstance(current, list):
        raise ValueError("invalid calibration index rows")
    by_id = {row[identity]: row for row in current}
    for row in recovered:
        prior = by_id.get(row[identity])
        if prior is None or _rank(row) >= _rank(prior):
            by_id[row[identity]] = row
    rows = sorted(by_id.values(), key=_rank)
    latest = (rows[-1].get("available_at") or rows[-1].get("as_of")) if rows else None
    result = {"v": "2.0", field: rows, "updated_at": latest,
              "recovery_source": "calibration/versions/"}
    if field == "snapshots":
        result.update(last_updated=latest, n_snapshots=len(rows))
    return result


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)
    print(f"[snapshotter] starting at {now.isoformat()}")

    # 1. Pull SSM
    weights = safe_get_ssm("/justhodl/calibration/weights") or {}
    accuracy_raw = safe_get_ssm("/justhodl/calibration/accuracy") or {}
    print(f"[snapshotter] weights: {len(weights)}, accuracy: {len(accuracy_raw)}")

    if not weights:
        return {"statusCode": 500, "body": json.dumps({"error": "no weights in SSM"})}

    # accuracy SSM is nested: {signal: {accuracy, n, avg_return}}. Flatten the
    # primary scalar (accuracy) and keep the rich fields as a sibling structure.
    accuracy = {}
    accuracy_meta = {}
    for sig, v in accuracy_raw.items():
        if isinstance(v, dict):
            acc = v.get("accuracy")
            if acc is not None:
                accuracy[sig] = float(acc)
            accuracy_meta[sig] = {
                "accuracy": float(v["accuracy"]) if v.get("accuracy") is not None else None,
                "n": int(v["n"]) if v.get("n") is not None else None,
                "avg_return": float(v["avg_return"]) if v.get("avg_return") is not None else None,
            }
        elif isinstance(v, (int, float)):
            accuracy[sig] = float(v)
            accuracy_meta[sig] = {"accuracy": float(v), "n": None, "avg_return": None}

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

    # audit 2026-09-08 INST-09: a snapshot is an IMMUTABLE, availability-stamped model version.
    # It becomes usable for decisions only from available_at (= now) onward; it is never labelled with
    # the Monday of the week it was computed in (that backdated Sunday weights onto the week's trades).
    snapshot_id = "cal-%s-%s-%s" % (now.strftime("%Y%m%dT%H%M%S%fZ"), label, uuid.uuid4().hex)
    snapshot = {
        "v": "2.0",
        "audit_version": "2026-09-09.1",
        "snapshot_id": snapshot_id,
        "as_of": now.isoformat(),
        "calibrated_at": now.isoformat(),
        "available_at": now.isoformat(),
        "training_end_at": now.isoformat(),
        "model_version": "calibrator-ssm-weights",
        "code_version": "snapshotter-2.0",
        "point_in_time_rule": "usable for a decision only when available_at <= decision timestamp",
        "iso_week": label,
        "iso_year": iso_year,
        "iso_week_num": iso_week,
        "week_start": week_start,
        "week_end": week_end,
        "weights": {k: float(v) for k, v in weights.items()},
        "accuracy": {k: float(v) for k, v in accuracy.items()},
        "accuracy_meta": accuracy_meta,
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

    # 4. Write the IMMUTABLE version (unique id) + the legacy weekly key (kept for old readers) + an index
    body = json.dumps(snapshot, default=str).encode("utf-8")
    if isinstance(event, dict) and event.get("mode") == "validate_only":
        return {"ok": True, "validation_only": True, "schema_version": "audit-accounting-1.0",
                "status": "READY", "artifact_size_bytes": len(body)}
    version_key = f"calibration/versions/{snapshot_id}.json"
    S3.put_object(Bucket=BUCKET, Key=version_key, Body=body, IfNoneMatch="*", ContentType="application/json", CacheControl="public, max-age=31536000, immutable")
    snapshot_key = f"calibration/history/{label}.json"
    _cas_json(snapshot_key, {}, lambda old: snapshot if not old or _rank(snapshot) >= _rank(old) else old)

    recovered = _version_rows()
    version_rows = []
    weekly_rows = []
    for key, doc in recovered:
        version_rows.append({"snapshot_id": doc["snapshot_id"], "key": key,
                             "available_at": doc["available_at"], "calibrated_at": doc["calibrated_at"],
                             "iso_week": doc["iso_week"], "n_weights": len(doc["weights"])})
        weekly_rows.append({"iso_week": doc["iso_week"], "iso_year": doc["iso_year"],
                            "iso_week_num": doc["iso_week_num"], "snapshot_id": doc["snapshot_id"],
                            "as_of": doc["as_of"], "available_at": doc["available_at"],
                            "week_start": doc["week_start"], "week_end": doc["week_end"],
                            "key": key, "size_bytes": len(json.dumps(doc, default=str).encode()),
                            "n_weights": len(doc["weights"]),
                            "n_calibrated_n30": doc["summary"]["n_signals_calibrated_n30"]})
    _cas_json("calibration/index.json", {},
              lambda old: _merge_index(old, version_rows, "versions", "snapshot_id"))
    index = _cas_json("calibration/history-index.json", {},
                      lambda old: _merge_index(old, weekly_rows, "snapshots", "iso_week"))
    snapshots = index["snapshots"]
    # Never collide with calibration/latest.json, owned by the calibrator's
    # horizon/report schema. A slower earlier invocation cannot regress latest.
    latest = max((doc for _, doc in recovered), key=_rank)
    _cas_json("calibration/model-latest.json", {},
              lambda old: latest if not old or _rank(latest) >= _rank(old) else old)

    print(f"[snapshotter] wrote {version_key} ({len(body):,}b), index has {len(snapshots)} weeks")

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
