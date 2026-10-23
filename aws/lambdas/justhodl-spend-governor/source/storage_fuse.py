"""storage_fuse.py - v1.5.0: brakes for the bucket itself (Khalid, 2026-10-20).

Lambda storms are stopped within ~2 hours by the fuses and the $50/month cap; the count-based lifecycle rule (ops 6571)
caps old copies at 10 per file.  What is left is a writer creating millions of NEW objects, or any other growth of
justhodl-dashboard-live that no version rule touches.  Once a day, from the free daily S3 metrics (BucketSizeBytes
per storage type, NumberOfObjects):

  warn   growth over the last complete day >= storage_warn_gb or >= storage_warn_pct, or objects >= storage_objects_warn
         -> event storage-growth (an issue for Khalid), nothing held
  trip   growth >= storage_trip_gb / storage_trip_pct / storage_objects_trip in one day, or two warn days in a row
         -> the hottest writers of that day (by Lambda cost from the meter's daily record) are held under CAP
            (manual release), event storage-fuse
  second a trip the day after a trip -> bucket versioning SUSPENDED (no new old copies; every existing object and
         version untouched; reversible with {"action":"storage_restore"}), the top writers held, event storage-fuse-versioning
  spend  yesterday's S3 service cost (Cost Explorer, by SERVICE) >= s3_usd_day_alert -> event s3-spend

Pure functions here (tested in tests/run_tests.py); the AWS calls and the ledger wiring live in lambda_function.py.
"""
from datetime import datetime, timedelta, timezone

GB = 1024.0 ** 3
S3_SERVICE = "Amazon Simple Storage Service"


def storage_days(points):
    """points: iterable of (day 'YYYY-MM-DD', storage_type, bytes) and ('objects' rows as type 'NumberOfObjects')
    -> {day: {"gb": float, "objects": int}} summed over storage types."""
    out = {}
    for day, stype, value in points:
        row = out.setdefault(day, {"gb": 0.0, "objects": 0})
        if stype == "NumberOfObjects":
            row["objects"] = int(value)
        else:
            row["gb"] = round(row["gb"] + float(value) / GB, 3)
    return out


def coverage(points):
    """points as for storage_days -> {day: {"types": storage types that reported, "objects": NumberOfObjects reported}}."""
    out = {}
    for day, stype, _value in points:
        row = out.setdefault(day, {"types": 0, "objects": False})
        if stype == "NumberOfObjects":
            row["objects"] = True
        else:
            row["types"] += 1
    return out


def partial_reason(cov, day):
    """v1.5.2: the daily S3 metrics for one day land type by type over several hours, so the first read of a day can hold
    one storage type and no object count (seen 2026-10-21/22: 173.7 GB and 0 objects against a 1,040 GB bucket, which read
    as -864 GB and would hide any real growth that day).  A day is complete when its object count is in and it carries as
    many storage types as the fullest other day in the window.  -> None when complete, else the reason to wait."""
    row = (cov or {}).get(day)
    if not row:
        return "no point for %s yet" % day
    want = max((v["types"] for d, v in cov.items() if d != day), default=row["types"])
    missing = []
    if not row["objects"]:
        missing.append("object count")
    if row["types"] < want:
        missing.append("%d of %d storage types" % (row["types"], want))
    return "partial point for %s (%s missing) - waiting for the complete day" % (day, ", ".join(missing)) if missing else None


def assess(days, cfg, history):
    """days: {day: {gb, objects}} (at least the last two complete days); history: the state's storage record
    ({"warn_days": [...], "trip_days": [...]}).  -> dict with day, prev, deltas and level in (None, warn, trip, second)."""
    ordered = sorted(d for d in days if days[d].get("gb"))
    if len(ordered) < 2:
        return {"level": None, "reason": "fewer than two daily points", "day": ordered[-1] if ordered else None}
    day, prev = ordered[-1], ordered[-2]
    cur, old = days[day], days[prev]
    d_gb = round(cur["gb"] - old["gb"], 2)
    d_pct = round(100.0 * d_gb / old["gb"], 2) if old["gb"] else 0.0
    d_obj = int(cur.get("objects", 0) - old.get("objects", 0))
    warn = d_gb >= cfg["storage_warn_gb"] or d_pct >= cfg["storage_warn_pct"] or d_obj >= cfg["storage_objects_warn"]
    trip = d_gb >= cfg["storage_trip_gb"] or d_pct >= cfg["storage_trip_pct"] or d_obj >= cfg["storage_objects_trip"]
    prev_day = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    warn_days = set(history.get("warn_days") or [])
    trip_days = set(history.get("trip_days") or [])
    level = None
    if trip or (warn and prev_day in warn_days):
        level = "second" if prev_day in trip_days else "trip"
    elif warn:
        level = "warn"
    return {"day": day, "prev": prev, "gb": cur["gb"], "prev_gb": old["gb"], "objects": cur.get("objects", 0),
            "delta_gb": d_gb, "delta_pct": d_pct, "delta_objects": d_obj, "level": level,
            "reason": "growth %+.1f GB (%+.1f%%), %+d objects over %s" % (d_gb, d_pct, d_obj, day)}


def pick_writers(daily, day, exempt, held, n):
    """The hottest functions of `day` from the meter's daily record {day: {fn: [invocations, usd]}}: by cost, then
    invocations; never an exempt one, never one already held."""
    rows = (daily or {}).get(day) or {}
    if not rows:                       # the window that ends on `day` may be booked a day later
        prior = sorted(k for k in (daily or {}) if k <= day)
        rows = (daily or {}).get(prior[-1]) if prior else {}
    ranked = sorted(((v[1], v[0], fn) for fn, v in (rows or {}).items() if fn not in exempt and fn not in held), reverse=True)
    return [fn for usd, inv, fn in ranked[:n] if usd > 0 or inv > 0]


def s3_usd_by_day(groups_by_day):
    """{day: [(service, usd), ...]} -> {day: usd of the S3 service}."""
    out = {}
    for day, groups in groups_by_day.items():
        v = sum(float(usd) for svc, usd in groups if svc == S3_SERVICE)
        if v:
            out[day] = round(v, 4)
    return out


def due(state_storage, now, after_hour):
    """Once per UTC day, at the first tick at or after `after_hour` UTC (the daily S3 metrics land in the morning)."""
    return now.hour >= after_hour and (state_storage or {}).get("day") != now.date().isoformat()


def utc_day(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


SERVICE_KEYS = {
    "Amazon Simple Storage Service": "s3", "Amazon SageMaker": "sagemaker", "AmazonCloudWatch": "cloudwatch",
    "Amazon DynamoDB": "dynamodb", "Amazon Elastic Compute Cloud - Compute": "ec2", "AWS Lambda": "lambda",
    "AWS Data Transfer": "transfer",
}


def service_usd_by_day(groups_by_day):
    """{day: [(service, usd), ...]} -> {day: {service: usd}} (services with a non-zero line)."""
    out = {}
    for day, groups in groups_by_day.items():
        row = {}
        for svc, usd in groups:
            v = float(usd)
            if v:
                row[svc] = round(row.get(svc, 0.0) + v, 4)
        if row:
            out[day] = row
    return out


def service_breaches(row, cfg):
    """[(service, usd, line)] for every service of one day over its line (s3 uses s3_usd_day_alert; unknown services
    use svc_alert_other; Tax and credits are skipped)."""
    out = []
    for svc, usd in sorted((row or {}).items(), key=lambda kv: -kv[1]):
        if svc.lower() in ("tax", "refund", "credit") or usd <= 0:
            continue
        key = SERVICE_KEYS.get(svc)
        line = cfg["s3_usd_day_alert"] if key == "s3" else cfg.get("svc_alert_" + key, cfg["svc_alert_other"]) if key else cfg["svc_alert_other"]
        if usd >= line:
            out.append((svc, usd, line))
    return out
