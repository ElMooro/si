"""
justhodl-calibration-snapshot — Materializes Loop 1 calibration state into a
single S3 JSON file the frontend can read without DDB/SSM access.

Reads:
  - SSM /justhodl/calibration/weights        (current model weights per signal_type)
  - SSM /justhodl/calibration/accuracy       (overall accuracy + n + avg_return per signal_type)
  - DDB justhodl-outcomes                    (scored outcomes, last 60 days)
  - DDB justhodl-signals                     (raw signals logged, last 60 days)
  - S3 portfolio/signal-portfolio-state.json (paper portfolio NAV)
  - S3 data/ab-test-results.json             (challenger leaderboard)

Computes (per signal_type):
  - rolling 7d / 30d / 60d accuracy
  - rolling 7d / 30d / 60d avg return
  - direction hit rate (binary up/down predictions)
  - latest signal value + age

Writes: data/calibration-snapshot.json
Schedule: every 30 minutes
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from statistics import mean

import boto3
from boto3.dynamodb.conditions import Attr

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/calibration-snapshot.json"

OUTCOMES_TBL = DDB.Table("justhodl-outcomes")
SIGNALS_TBL = DDB.Table("justhodl-signals")


def get_ssm_json(path):
    try:
        v = SSM.get_parameter(Name=path)["Parameter"]["Value"]
        return json.loads(v)
    except Exception as e:
        print(f"[ssm] {path} failed: {e}")
        return {}


def scan_outcomes_recent(days=60):
    """Scan outcomes scored in last N days. Filters legacy=true items."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    items = []
    last_key = None
    pages = 0
    while True:
        kw = {"Limit": 1000, "FilterExpression": Attr("scored_at").gte(cutoff) & Attr("is_legacy").ne(True)}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        try:
            resp = OUTCOMES_TBL.scan(**kw)
        except Exception as e:
            # fallback: filter without is_legacy clause if attr is missing
            kw = {"Limit": 1000, "FilterExpression": Attr("scored_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = OUTCOMES_TBL.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 8:
            break
    print(f"[outcomes] scanned {pages} pages, {len(items)} non-legacy items")
    return items


def to_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except (ValueError, TypeError):
        return default


def compute_per_signal_stats(outcomes):
    """Per-signal-type rolling stats."""
    now = datetime.now(timezone.utc)
    by_type = defaultdict(list)
    for it in outcomes:
        st = it.get("signal_type")
        if not st:
            continue
        scored = it.get("scored_at")
        if not scored:
            continue
        try:
            ts = datetime.fromisoformat(scored.replace("Z", "+00:00"))
        except Exception:
            continue
        age_days = (now - ts).total_seconds() / 86400
        ret = to_float(it.get("return_pct"))
        hit = it.get("hit", None)
        if hit in ("true", True):
            hit = 1
        elif hit in ("false", False):
            hit = 0
        else:
            hit = None
        by_type[st].append({"age_days": age_days, "return_pct": ret, "hit": hit})

    stats = {}
    for st, recs in by_type.items():
        d7 = [r for r in recs if r["age_days"] <= 7]
        d30 = [r for r in recs if r["age_days"] <= 30]
        d60 = [r for r in recs if r["age_days"] <= 60]
        stats[st] = {
            "n_total": len(recs),
            "n_7d": len(d7),
            "n_30d": len(d30),
            "n_60d": len(d60),
            "accuracy_7d": _avg([r["hit"] for r in d7 if r["hit"] is not None]),
            "accuracy_30d": _avg([r["hit"] for r in d30 if r["hit"] is not None]),
            "accuracy_60d": _avg([r["hit"] for r in d60 if r["hit"] is not None]),
            "avg_return_7d": _avg([r["return_pct"] for r in d7 if r["return_pct"] is not None]),
            "avg_return_30d": _avg([r["return_pct"] for r in d30 if r["return_pct"] is not None]),
            "avg_return_60d": _avg([r["return_pct"] for r in d60 if r["return_pct"] is not None]),
        }
    return stats


def _avg(vals):
    if not vals:
        return None
    return round(sum(vals) / len(vals), 4)


def get_latest_signals(days=2, max_per_type=1):
    """Get latest signal per signal_type within last N days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    items = []
    last_key = None
    pages = 0
    while True:
        kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = SIGNALS_TBL.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 5:
            break
    # group by type, keep latest
    latest = {}
    for it in items:
        st = it.get("signal_type")
        if not st:
            continue
        ts = it.get("logged_at", "")
        if st not in latest or ts > latest[st].get("logged_at", ""):
            latest[st] = it
    return latest


def lambda_handler(event=None, context=None):
    started = time.time()

    # SSM state
    weights = get_ssm_json("/justhodl/calibration/weights")
    accuracy = get_ssm_json("/justhodl/calibration/accuracy")

    # Outcome stats (rolling 7/30/60d)
    outcomes = scan_outcomes_recent(days=60)
    rolling_stats = compute_per_signal_stats(outcomes)

    # Latest signal values
    latest = get_latest_signals(days=2)
    latest_by_type = {}
    for st, it in latest.items():
        latest_by_type[st] = {
            "logged_at": it.get("logged_at"),
            "signal_value": it.get("signal_value") if isinstance(it.get("signal_value"), (str, int, float)) else str(it.get("signal_value")),
            "direction": it.get("direction"),
            "confidence": to_float(it.get("confidence")),
            "asset": it.get("asset"),
        }

    # Paper portfolio
    portfolio = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="portfolio/signal-portfolio-state.json")
        portfolio = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[portfolio] {e}")

    # A/B test
    ab = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/ab-test-results.json")
        ab = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[ab] {e}")

    # Build per-signal unified view (weights + accuracy + rolling + latest)
    all_types = set(weights.keys()) | set(accuracy.keys()) | set(rolling_stats.keys()) | set(latest_by_type.keys())
    signals = []
    for st in sorted(all_types):
        acc_blob = accuracy.get(st, {}) if isinstance(accuracy, dict) else {}
        roll = rolling_stats.get(st, {})
        latest_blob = latest_by_type.get(st, {})
        signals.append({
            "signal_type": st,
            "weight": weights.get(st),
            "overall_accuracy": acc_blob.get("accuracy") if isinstance(acc_blob, dict) else None,
            "overall_n": acc_blob.get("n") if isinstance(acc_blob, dict) else None,
            "overall_avg_return": acc_blob.get("avg_return") if isinstance(acc_blob, dict) else None,
            "rolling": roll,
            "latest": latest_blob,
        })
    # Sort by weight desc (highest-trusted signals first)
    signals.sort(key=lambda x: -(x.get("weight") or 0))

    # Top performers + worst performers
    rated = [s for s in signals if s.get("overall_accuracy") is not None and s.get("overall_n", 0) >= 20]
    top_accuracy = sorted(rated, key=lambda x: -x["overall_accuracy"])[:10]
    worst_accuracy = sorted(rated, key=lambda x: x["overall_accuracy"])[:10]

    # Aggregate calibration stats
    weighted_accuracy = None
    if rated:
        total_w = sum(s.get("weight") or 0 for s in rated)
        if total_w > 0:
            weighted_accuracy = round(sum((s.get("weight") or 0) * s["overall_accuracy"] for s in rated) / total_w, 4)

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "summary": {
            "n_signal_types_tracked": len(all_types),
            "n_signal_types_with_accuracy": len(rated),
            "n_outcomes_60d": len(outcomes),
            "weighted_avg_accuracy": weighted_accuracy,
            "best_signal": top_accuracy[0]["signal_type"] if top_accuracy else None,
            "worst_signal": worst_accuracy[0]["signal_type"] if worst_accuracy else None,
        },
        "signals": signals,
        "top_accuracy": top_accuracy,
        "worst_accuracy": worst_accuracy,
        "paper_portfolio": {
            "current_nav": portfolio.get("current_nav") if portfolio else None,
            "initial_nav": portfolio.get("initial_nav") if portfolio else None,
            "current_nav_pct_chg": portfolio.get("current_nav_pct_chg") if portfolio else None,
            "unrealized_pnl_dollars": portfolio.get("unrealized_pnl_dollars") if portfolio else None,
            "open_positions": portfolio.get("open_positions", []) if portfolio else [],
            "first_seen": portfolio.get("first_seen") if portfolio else None,
            "last_run_date": portfolio.get("last_run_date") if portfolio else None,
        } if portfolio else None,
        "ab_test": ab,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=KEY, Body=body,
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    print(f"[calib-snap] wrote s3://{BUCKET}/{KEY}  {len(body):,}b  in {out['duration_s']}s")
    print(f"[calib-snap] tracked={len(all_types)}  rated={len(rated)}  weighted_acc={weighted_accuracy}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_signal_types": len(all_types),
            "n_outcomes_60d": len(outcomes),
            "weighted_avg_accuracy": weighted_accuracy,
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
