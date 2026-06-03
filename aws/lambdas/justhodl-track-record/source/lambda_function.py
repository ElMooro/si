"""justhodl-track-record — empirical proof of signal performance

Aggregates the self-improvement loop's daily scored predictions into the
numbers that matter for trust (and for converting a paying subscriber):
  • Hit rate by TIER (does POLITICIAN_COMMITTEE actually beat OPTIONS_EXTREME?)
  • Hit rate by HORIZON (1d → 30d)
  • Overall hit rate + sample size + avg/median return
  • EQUITY CURVE — cumulative return of "following the signals" (equal-weight
    every scored buy signal, max-favorable-move proxy)
  • Calibration maturity

Reads:  data/predictions-scored/{date}.json   (last ~90 days)
Output: data/track-record.json
Consumed by: track-record.html (the public proof page) + pricing.

SCHEDULE: daily 9:00 ET (after self-improvement scores yesterday).
"""
import json
import time
import statistics
from datetime import datetime, timezone
from collections import defaultdict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/track-record.json"
s3 = boto3.client("s3", region_name="us-east-1")

HIT_OUTCOMES = {"HIT", "HIT_BIG"}
ALL_OUTCOMES = {"HIT", "HIT_BIG", "SLOW", "FLAT", "MISS"}
HORIZONS = [1, 3, 5, 7, 14, 21, 30]


def _read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _list_scored(limit=120):
    keys = []
    token = None
    while True:
        kw = {"Bucket": S3_BUCKET, "Prefix": "data/predictions-scored/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return sorted(keys)[-limit:]


def tier_of(alerts):
    a = set(alerts or [])
    order = ["POLITICIAN_COMMITTEE", "EXECUTIVE_BUY", "POLITICIAN_BUY", "INSIDER_CLUSTER",
             "OPTIONS_EXTREME_CALL", "OPTIONS_BULLISH_CALL", "CASCADE_ALERT", "CASCADE_LAGGARD",
             "RETAIL_HOT", "RETAIL_VELOCITY", "EARNINGS_FRESH", "EARLY_MOVER_ALERT"]
    for o in order:
        if o in a:
            return o
    for x in a:
        if x.startswith("CONVERGENCE_"):
            return "CONVERGENCE"
        if x.startswith("VELOCITY_FIRED"):
            return "VELOCITY_FIRED"
    return "OTHER"


def lambda_handler(event, context):
    t0 = time.time()
    keys = _list_scored()
    print(f"[track-record] {len(keys)} scored files")

    by_tier = defaultdict(lambda: {"n": 0, "hits": 0, "returns": []})
    by_horizon = defaultdict(lambda: {"n": 0, "hits": 0})
    overall = {"n": 0, "hits": 0, "big": 0, "returns": []}
    daily = defaultdict(lambda: {"n": 0, "ret_sum": 0.0})   # for equity curve
    n_days = 0

    for key in keys:
        doc = _read_json(key)
        if not doc:
            continue
        scored = doc.get("scored") or []
        date = doc.get("snapshot_date_scored") or key.split("/")[-1].replace(".json", "")
        day_has = False
        for p in scored:
            outcome = p.get("outcome")
            if outcome not in ALL_OUTCOMES:
                continue
            day_has = True
            mr = p.get("max_return_pct")
            tier = tier_of(p.get("alerts"))
            is_hit = outcome in HIT_OUTCOMES
            overall["n"] += 1
            overall["hits"] += 1 if is_hit else 0
            overall["big"] += 1 if outcome == "HIT_BIG" else 0
            if mr is not None:
                overall["returns"].append(mr)
                daily[date]["n"] += 1
                daily[date]["ret_sum"] += mr
            bt = by_tier[tier]
            bt["n"] += 1
            bt["hits"] += 1 if is_hit else 0
            if mr is not None:
                bt["returns"].append(mr)
            hbh = p.get("hit_by_horizon") or {}
            for h in HORIZONS:
                v = hbh.get(f"{h}d")
                if v is not None:
                    by_horizon[f"{h}d"]["n"] += 1
                    by_horizon[f"{h}d"]["hits"] += 1 if v else 0
        if day_has:
            n_days += 1

    def pct(h, n):
        return round(h / n * 100, 1) if n else None

    tier_perf = []
    for tier, d in by_tier.items():
        if d["n"] < 1:
            continue
        rets = d["returns"]
        tier_perf.append({
            "tier": tier, "n": d["n"], "hit_rate": pct(d["hits"], d["n"]),
            "avg_return": round(statistics.mean(rets), 2) if rets else None,
            "median_return": round(statistics.median(rets), 2) if rets else None,
        })
    tier_perf.sort(key=lambda x: (x["hit_rate"] or 0), reverse=True)

    horizon_perf = [{"horizon": f"{h}d", "n": by_horizon[f"{h}d"]["n"],
                     "hit_rate": pct(by_horizon[f"{h}d"]["hits"], by_horizon[f"{h}d"]["n"])}
                    for h in HORIZONS if by_horizon[f"{h}d"]["n"] > 0]

    # Equity curve — equal-weight avg daily return of all scored signals, cumulative
    equity = []
    cum = 100.0
    for date in sorted(daily.keys()):
        d = daily[date]
        avg = d["ret_sum"] / d["n"] if d["n"] else 0.0
        # scale daily contribution (max-favorable-move proxy, conservatively /3)
        cum *= (1 + (avg / 100.0) / 3.0)
        equity.append({"date": date, "value": round(cum, 2), "n": d["n"], "avg_return": round(avg, 2)})

    rets = overall["returns"]
    calib = _read_json("data/cascade-calibration.json") or {}
    blend_conf = ((calib.get("blend") or {}).get("confidence")
                  or (((_read_json("data/cascade-recalibration-audit.json") or {}).get("blend")) or {}).get("confidence")
                  or "NONE")

    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "window": {"scored_files": len(keys), "trading_days": n_days},
        "overall": {
            "n_predictions": overall["n"],
            "hit_rate": pct(overall["hits"], overall["n"]),
            "big_hit_rate": pct(overall["big"], overall["n"]),
            "avg_return": round(statistics.mean(rets), 2) if rets else None,
            "median_return": round(statistics.median(rets), 2) if rets else None,
            "best": round(max(rets), 1) if rets else None,
            "worst": round(min(rets), 1) if rets else None,
        },
        "by_tier": tier_perf,
        "by_horizon": horizon_perf,
        "equity_curve": equity,
        "calibration_confidence": blend_conf,
        "maturity": ("MATURE" if overall["n"] >= 500 else "BUILDING" if overall["n"] >= 50
                     else "BOOTSTRAPPING"),
        "note": ("Hit = signal reached ≥+5% within its window; HIT_BIG = ≥+10%. "
                 "Returns are max-favorable-move (the opportunity the signal flagged). "
                 "Equity curve is an equal-weight, conservatively-scaled proxy, not a "
                 "live traded P&L. Performance is being measured continuously by the "
                 "self-improvement loop; sample grows daily."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[track-record] n={overall['n']} hit_rate={output['overall']['hit_rate']} "
          f"tiers={len(tier_perf)} days={n_days} maturity={output['maturity']}")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "n": overall["n"], "hit_rate": output["overall"]["hit_rate"],
                                 "tiers": len(tier_perf), "maturity": output["maturity"]})}
