"""
justhodl-feed-heartbeat — Institutional-grade data freshness monitor.

WHY THIS EXISTS
───────────────
In finance, stale data is dangerous data. If a feed silently stops updating,
engines make decisions on dead metrics. This Lambda is the system's
"visual heartbeat": every 15 minutes it checks every critical data feed,
records exact freshness timestamps, and flags STALE/MISSING feeds.

This is the foundation of institutional trust — no silent failures.

OUTPUT
──────
  s3://justhodl-dashboard-live/data/feed-heartbeat.json
  {
    "generated_at": ISO-8601,
    "system_status": "HEALTHY" | "DEGRADED" | "CRITICAL",
    "n_feeds": 30,
    "n_fresh": 28,
    "n_stale": 1,
    "n_missing": 1,
    "feeds": {
      "ticker-360": {
        "artifact": "data/ticker-360.json",
        "last_modified": "2026-10-01T18:20:00Z",
        "age_minutes": 12.5,
        "expected_interval_minutes": 720,
        "status": "FRESH",
        "size_bytes": 1234567
      },
      ...
    },
    "alerts": [
      {"feed": "xbrl-fundamentals", "status": "STALE",
       "age_minutes": 1500, "expected": 10080,
       "message": "No update in 25h (expected weekly)"}
    ]
  }

SCHEDULE
────────
  rate(15 minutes)
"""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/feed-heartbeat.json"

# (logical_name, s3_key_or_prefix, expected_interval_minutes, is_prefix)
FEEDS = [
    # Ticker-360 domains (23)
    ("ticker-360", "data/ticker-360.json", 720, False),
    ("short-interest", "data/short-book.json", 1440, False),
    ("sec-8k", "data/sec-filings-intel.json", 60, False),
    ("xbrl-fundamentals", "data/xbrl-fundamentals/", 10080, True),
    ("corporate-actions", "data/corporate-actions-index.json", 1440, False),
    ("etf-holdings", "data/etf-issuer-holdings.json", 1440, False),
    ("13f-holdings", "data/13f-by-ticker.json", 1440 * 90, False),  # quarterly
    ("insider-trading", "data/insider-trades.json", 1440, False),
    ("earnings", "data/earnings-tracker.json", 1440, False),
    ("dark-pool", "data/dark-pool.json", 1440, False),
    ("cboe-options", "data/cboe-options.json", 60, False),
    ("macro-regime", "data/macro-regime.json", 1440, False),
    # Engine outputs
    ("flow-confluence", "data/flow-confluence.json", 1440, False),
    ("best-ideas", "data/best-ideas.json", 1440, False),
    ("master-ranker", "data/master-ranker.json", 60, False),
    ("conviction-engine", "data/conviction.json", 60, False),
    ("options-confluence", "data/options-confluence.json", 1440, False),
    # Schedules meta
    ("schedules", None, 60, False),  # special: checks EventBridge
]

s3 = boto3.client("s3", region_name=REGION)


def check_artifact(key, is_prefix):
    """Check S3 artifact freshness. Returns dict."""
    try:
        if is_prefix:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1)
            objs = r.get("Contents", [])
            if not objs:
                return {"exists": False}
            o = objs[0]
            lm = o["LastModified"]
            return {"exists": True,
                    "last_modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "size_bytes": o["Size"],
                    "_lm": lm}
        else:
            r = s3.head_object(Bucket=BUCKET, Key=key)
            lm = r["LastModified"]
            return {"exists": True,
                    "last_modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "size_bytes": r.get("ContentLength", 0),
                    "_lm": lm}
    except Exception:
        return {"exists": False}


def check_schedules():
    """Verify critical EventBridge schedules exist and are enabled."""
    try:
        sched = boto3.client("scheduler", region_name=REGION)
        critical = [
            "justhodl-ticker-360-schedule",
            "justhodl-sec-8k-enrich-schedule",
            "justhodl-short-interest-schedule",
            "justhodl-xbrl-fundamentals-schedule",
            "justhodl-corporate-actions-schedule",
            "justhodl-etf-issuer-holdings-daily",
        ]
        missing = []
        disabled = []
        for name in critical:
            try:
                r = sched.get_schedule(Name=name)
                if r.get("State") != "ENABLED":
                    disabled.append(name)
            except Exception:
                missing.append(name)
        return {"missing": missing, "disabled": disabled,
                "healthy": len(missing) == 0 and len(disabled) == 0}
    except Exception as e:
        return {"error": str(e)[:80]}


def lambda_handler(event, context):
    """Check all feeds, write heartbeat."""
    now = datetime.now(timezone.utc)
    feeds = {}
    alerts = []

    for name, key, expected_min, is_prefix in FEEDS:
        if name == "schedules":
            sched_check = check_schedules()
            status = "FRESH" if sched_check.get("healthy") else "CRITICAL"
            feeds[name] = {
                "artifact": "eventbridge-schedules",
                "status": status,
                "detail": sched_check,
                "expected_interval_minutes": expected_min,
            }
            if not sched_check.get("healthy"):
                alerts.append({
                    "feed": name, "status": "CRITICAL",
                    "message": f"Missing: {sched_check.get('missing', [])}, "
                               f"Disabled: {sched_check.get('disabled', [])}"})
            continue

        chk = check_artifact(key, is_prefix)
        if not chk.get("exists"):
            feeds[name] = {
                "artifact": key,
                "status": "MISSING",
                "last_modified": None,
                "age_minutes": None,
                "expected_interval_minutes": expected_min,
            }
            alerts.append({"feed": name, "status": "MISSING",
                           "message": f"Artifact not found: {key}"})
        else:
            age_min = (now - chk["_lm"]).total_seconds() / 60
            # Stale if age > 2x expected interval (grace period)
            # or > 48h for any feed (hard ceiling)
            is_stale = (age_min > expected_min * 2) or (age_min > 2880)
            status = "STALE" if is_stale else "FRESH"
            feeds[name] = {
                "artifact": key,
                "status": status,
                "last_modified": chk["last_modified"],
                "age_minutes": round(age_min, 1),
                "expected_interval_minutes": expected_min,
                "size_bytes": chk.get("size_bytes", 0),
            }
            if is_stale:
                alerts.append({
                    "feed": name, "status": "STALE",
                    "age_minutes": round(age_min, 1),
                    "expected_minutes": expected_min,
                    "message": f"No update in {age_min/60:.1f}h "
                               f"(expected every {expected_min/60:.1f}h)"})

    n_fresh = sum(1 for f in feeds.values() if f["status"] == "FRESH")
    n_stale = sum(1 for f in feeds.values() if f["status"] == "STALE")
    n_missing = sum(1 for f in feeds.values() if f["status"] == "MISSING")
    n_critical = sum(1 for f in feeds.values() if f["status"] == "CRITICAL")

    if n_critical > 0 or n_missing > 2:
        system_status = "CRITICAL"
    elif n_stale > 0 or n_missing > 0:
        system_status = "DEGRADED"
    else:
        system_status = "HEALTHY"

    out = {
        "generated_at": now.isoformat(),
        "system_status": system_status,
        "n_feeds": len(feeds),
        "n_fresh": n_fresh,
        "n_stale": n_stale,
        "n_missing": n_missing,
        "n_critical": n_critical,
        "feeds": feeds,
        "alerts": alerts,
    }

    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(out, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=300",
    )

    print(f"[heartbeat] {system_status}: {n_fresh} fresh, {n_stale} stale, "
          f"{n_missing} missing, {n_critical} critical")
    return {"statusCode": 200, "body": json.dumps({
        "system_status": system_status, "n_feeds": len(feeds),
        "alerts": len(alerts)})}
