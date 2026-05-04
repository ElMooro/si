"""
justhodl-ab-test — Compare competing prompt strategies and signal models head-to-head.

For every signal_type in justhodl-signals, accuracy is tracked over time. This Lambda
splits signals by their `variant` tag (default "production" if untagged) and computes
per-variant accuracy. It also fires a daily challenger run that scores morning-intel
data with an alternative Anthropic prompt strategy (the "challenger") and logs those
predictions with variant=challenger so the system accumulates head-to-head data.

Variants tested:
  - production      = current prompt (Khalid's hand-tuned, decisive, regime-aware)
  - challenger_a    = "consensus seeking" — emphasizes confluence across 5+ sources
  - challenger_b    = "contrarian"        — overweights extremes (z>2σ or |%ile|>90)

Output:
  data/ab-test-results.json  — per-variant accuracy, win-rate, sample size, statistical confidence
  DynamoDB justhodl-signals  — challenger predictions logged with variant tag

Schedule: daily 16 UTC.
"""
import json
import os
import time
import uuid
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import math
import boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/ab-test-results.json"
SIGNALS_TABLE = "justhodl-signals"
OUTCOMES_TABLE = "justhodl-outcomes"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def _decimal_default(o):
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def scan_all(table_name, filter_expr=None):
    table = ddb.Table(table_name)
    kwargs = {"FilterExpression": filter_expr} if filter_expr else {}
    out = []
    res = table.scan(**kwargs)
    out.extend(res.get("Items", []))
    while "LastEvaluatedKey" in res:
        kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]
        res = table.scan(**kwargs)
        out.extend(res.get("Items", []))
    return out


def fetch_morning_intel():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/morning-intel.json")
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"morning-intel fetch fail: {e}")
        return None


def fetch_active_signals_summary():
    """Read a compact view of today's signals for the challenger prompts."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        rep = json.loads(obj["Body"].read())
    except Exception:
        rep = {}
    summary = {
        "khalid_index": rep.get("khalid_index", {}).get("score"),
        "regime": rep.get("regime"),
        "vix": rep.get("vix"),
        "spy_close": rep.get("spy", {}).get("close") if isinstance(rep.get("spy"), dict) else None,
    }
    # Pull a few ancillary scores
    for src_key, label in [
        ("data/edge-data.json", "edge_composite"),
        ("data/macro-surprise.json", "macro_surprise_composite"),
        ("data/yield-curve.json", "yield_curve_regime"),
        ("data/historical-analogs.json", "analog_call"),
        ("data/event-study.json", "active_event_themes"),
    ]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=src_key)
            d = json.loads(obj["Body"].read())
            if label == "edge_composite":
                summary[label] = d.get("composite_score")
            elif label == "macro_surprise_composite":
                summary[label] = d.get("composite_z")
                summary["macro_regime"] = d.get("regime")
            elif label == "yield_curve_regime":
                summary[label] = d.get("regime")
                summary["spread_2s10s"] = d.get("spreads", {}).get("2s10s")
            elif label == "analog_call":
                summary[label] = d.get("call")
                summary["analog_21d_hit"] = d.get("forward_returns", {}).get("21d", {}).get("hit_rate_pct")
            elif label == "active_event_themes":
                summary[label] = d.get("active_themes")
        except Exception as e:
            print(f"summary fetch {src_key} fail: {e}")
    return summary


CHALLENGER_PROMPTS = {
    "challenger_a": (
        "You are a CONSENSUS-SEEKING analyst. Emphasize confluence across many signals. "
        "Only call BULLISH or BEARISH if at least 5 of the provided signals lean the same way; "
        "otherwise call NEUTRAL. Do not overweight any single source. "
        "Output JSON only: {\"call\":\"BULLISH|BEARISH|NEUTRAL\",\"horizon_days\":21,\"confidence\":0..1}."
    ),
    "challenger_b": (
        "You are a CONTRARIAN analyst. Overweight extreme readings (|z|>2 or percentile>90 or <10). "
        "When the crowd is positioned heavily one way, call the opposite. "
        "Output JSON only: {\"call\":\"BULLISH|BEARISH|NEUTRAL\",\"horizon_days\":21,\"confidence\":0..1}."
    ),
}


def call_anthropic(system_prompt, user_prompt):
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": 256,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
            # Extract first JSON object
            for start in range(len(text)):
                if text[start] == "{":
                    depth = 0
                    for end in range(start, len(text)):
                        if text[end] == "{":
                            depth += 1
                        elif text[end] == "}":
                            depth -= 1
                            if depth == 0:
                                try:
                                    return json.loads(text[start:end + 1])
                                except Exception:
                                    break
                    break
            return None
    except Exception as e:
        print(f"anthropic call fail: {e}")
        return None


def log_challenger_signal(variant, call, conf, horizon_days, summary):
    """Write challenger prediction to justhodl-signals with variant tag."""
    table = ddb.Table(SIGNALS_TABLE)
    sig_id = f"abtest_{variant}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    direction = {"BULLISH": "UP", "BEARISH": "DOWN", "NEUTRAL": "FLAT"}.get(call, "FLAT")
    ttl = int((now + timedelta(days=180)).timestamp())
    item = {
        "signal_id": sig_id,
        "signal_type": f"abtest_{variant}",
        "variant": variant,
        "signal_value": str(call),
        "direction": direction,
        "confidence": Decimal(str(round(float(conf or 0.6), 4))),
        "asset": "SPY",
        "horizons_days": [int(horizon_days or 21)],
        "logged_at": now.isoformat(),
        "ttl": ttl,
        "meta": {"abtest": True, "summary": json.loads(json.dumps(summary, default=_decimal_default))},
        "is_legacy": False,
    }
    try:
        table.put_item(Item=item)
        return True
    except Exception as e:
        print(f"log_challenger_signal fail: {e}")
        return False


def compute_variant_accuracy():
    """Aggregate accuracy by variant from outcomes table joined with signals."""
    # Get all scored outcomes
    outcomes = scan_all(OUTCOMES_TABLE, filter_expr=Attr("status").eq("scored"))
    # Build signal_id -> variant map from signals table for any abtest_* signal_types
    table = ddb.Table(SIGNALS_TABLE)
    # We pull variant tags directly off the signals when available
    variants = defaultdict(lambda: {"n_correct": 0, "n_wrong": 0, "n_total": 0,
                                    "horizons": defaultdict(lambda: {"correct": 0, "wrong": 0})})

    sigid_to_variant = {}
    if outcomes:
        # Need to look up signals by id; fall back to scanning ab-test signals
        ab_signals = scan_all(SIGNALS_TABLE, filter_expr=Attr("variant").exists())
        for s in ab_signals:
            sigid_to_variant[s.get("signal_id")] = s.get("variant", "production")

    for o in outcomes:
        sid = o.get("signal_id")
        # Default any unlabeled signal to production
        variant = sigid_to_variant.get(sid, "production")
        # Also tolerate variant directly on outcome
        if "variant" in o:
            variant = o.get("variant", variant)
        # is_legacy filter
        if o.get("is_legacy"):
            continue
        v = variants[variant]
        v["n_total"] += 1
        horizon = str(o.get("horizon_days", "?"))
        if o.get("predicted_correct") is True or o.get("hit") is True:
            v["n_correct"] += 1
            v["horizons"][horizon]["correct"] += 1
        elif o.get("predicted_correct") is False or o.get("hit") is False:
            v["n_wrong"] += 1
            v["horizons"][horizon]["wrong"] += 1

    return variants


def wilson_ci(n_correct, n_total, z=1.96):
    """Wilson score 95% CI for a proportion."""
    if n_total == 0:
        return (0.0, 0.0)
    p = n_correct / n_total
    denom = 1 + z**2 / n_total
    center = (p + z**2 / (2 * n_total)) / denom
    half = z * math.sqrt((p * (1 - p) + z**2 / (4 * n_total)) / n_total) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def lambda_handler(event, context):
    t0 = time.time()

    # 1) Fire challenger predictions for today (only if we have ANTHROPIC_KEY)
    summary = fetch_active_signals_summary()
    challenger_results = {}
    if ANTHROPIC_KEY:
        user_prompt = (
            "Today's signal summary (real production data):\n"
            + json.dumps(summary, default=_decimal_default, indent=2)
            + "\n\nGiven this summary, output your JSON call only."
        )
        for variant, sys_prompt in CHALLENGER_PROMPTS.items():
            resp = call_anthropic(sys_prompt, user_prompt)
            if resp:
                call = str(resp.get("call", "NEUTRAL")).upper()
                if call not in ("BULLISH", "BEARISH", "NEUTRAL"):
                    call = "NEUTRAL"
                conf = resp.get("confidence", 0.6)
                horizon = resp.get("horizon_days", 21)
                logged = log_challenger_signal(variant, call, conf, horizon, summary)
                challenger_results[variant] = {
                    "call": call, "confidence": float(conf), "horizon_days": int(horizon),
                    "logged": logged,
                }
            else:
                challenger_results[variant] = {"error": "no_response"}

    # 2) Compute per-variant accuracy from history
    variants = compute_variant_accuracy()

    # 3) Build leaderboard with confidence intervals
    leaderboard = []
    for variant, agg in variants.items():
        n_scored = agg["n_correct"] + agg["n_wrong"]
        accuracy = (agg["n_correct"] / n_scored) if n_scored else None
        ci_low, ci_high = wilson_ci(agg["n_correct"], n_scored)
        leaderboard.append({
            "variant": variant,
            "n_scored": n_scored,
            "n_total": agg["n_total"],
            "n_correct": agg["n_correct"],
            "n_wrong": agg["n_wrong"],
            "accuracy_pct": round(accuracy * 100, 2) if accuracy is not None else None,
            "ci_95_low_pct": round(ci_low * 100, 2),
            "ci_95_high_pct": round(ci_high * 100, 2),
            "sufficient_data": n_scored >= 30,
            "by_horizon": {h: dict(v) for h, v in agg["horizons"].items()},
        })
    # Sort by accuracy desc, undefined last
    leaderboard.sort(key=lambda x: (x["accuracy_pct"] or -1), reverse=True)

    # 4) Determine winner if any variant has sufficient data + statistically separates
    winner = None
    if leaderboard:
        top = leaderboard[0]
        if top["sufficient_data"] and top["accuracy_pct"] is not None:
            # Winner if its lower CI exceeds 50% AND beats #2 by ≥ 5 ppts (or #2 has insufficient data)
            challengers = [x for x in leaderboard[1:] if x["sufficient_data"]]
            if top["ci_95_low_pct"] > 50.0:
                if not challengers or (top["accuracy_pct"] - challengers[0]["accuracy_pct"]) >= 5.0:
                    winner = top["variant"]

    out = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "challenger_signals_today": challenger_results,
        "leaderboard": leaderboard,
        "winner": winner,
        "n_variants_tracked": len(variants),
        "summary_used_for_challengers": summary,
        "duration_s": round(time.time() - t0, 2),
    }

    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=json.dumps(out, default=_decimal_default, indent=2).encode(),
        ContentType="application/json",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_variants": len(variants),
            "winner": winner,
            "challenger_signals_today": list(challenger_results.keys()),
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
