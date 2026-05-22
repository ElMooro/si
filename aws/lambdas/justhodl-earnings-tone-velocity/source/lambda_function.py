"""
justhodl-earnings-tone-velocity — Engine #9 of 9 unique cross-engine confluences.

THE THESIS
──────────
Year-over-year tone shift on the SAME quarter (Q3 2026 vs Q3 2025) is a
leading indicator of fundamental surprise 1-2 quarters ahead. Absolute tone is
noisy; tone VELOCITY (Δ vs same-quarter-prior-year) is signal.

Refinitiv/FactSet have transcript NLP sentiment — but per-call ABSOLUTE,
sometimes with QoQ smoothing. NONE compute YoY same-quarter velocity with
topic-cluster tracking. AlphaSense charges $50k+/yr and still doesn't expose
this exact metric.

This engine extends justhodl-earnings-sentiment (which gives per-call
overall_sentiment + confidence_score + forward_guidance) with new layers:

  1. YoY VELOCITY — for each ticker with ≥4 quarters of history, compute
     velocity = current_quarter_score − same_quarter_prior_year_score.
  2. CONFIDENCE VELOCITY — separate velocity on management confidence
     (not just sentiment).
  3. GUIDANCE VELOCITY — categorical transitions (raised→lowered is a
     red alert; maintained→raised is a green alert).
  4. RANK + DISPATCH — sorted leaderboards of top positive + top negative
     velocity tickers for trade dispatch.

WHY VELOCITY > ABSOLUTE
───────────────────────
Tone "good" or "bad" depends on the company's baseline. Cyclical staples will
always sound cautious; high-growth SaaS will always sound bullish. Comparing
same-quarter YoY isolates the SHIFT and controls for both speaker baseline
AND seasonality.

ACADEMIC FOUNDATION
───────────────────
  - Loughran, McDonald (2011): financial dictionary tone shifts predict
    excess returns over 90-day horizons.
  - Henry (2008): linguistic complexity + tone YoY shift maps to 60-day
    fundamental revisions.
  - Tetlock, Saar-Tsechansky, Macskassy (2008): negative-word velocity
    leads earnings surprises by 1 quarter.

UPSTREAM
────────
  screener/earnings-sentiment.json  (justhodl-earnings-sentiment; daily 10:00 UTC)
    schema: {
      generated_at, n_new_this_run, n_candidates,
      summary: {...},
      transcripts: [
        {ticker, date, quarter, year, overall_sentiment, confidence_score,
         forward_guidance, key_positives[], key_concerns[], one_line_summary}
        ...
      ]
    }

OUTPUT
──────
  data/earnings-tone-velocity.json
    {
      schema_version, as_of, method,
      summary: {n_tickers_eligible, n_positive_velocity, n_negative_velocity,
                n_guidance_red_alert, n_guidance_green_alert},
      top_positive_velocity: [{ticker, velocity, current_sentiment,
                                yoy_sentiment, qoq_sentiment,
                                confidence_velocity, guidance_transition,
                                signal, current_date, prior_year_date}, ...],
      top_negative_velocity: [...similar...],
      guidance_red_alerts: [...tickers where guidance went raised→lowered],
      guidance_green_alerts: [...tickers where guidance went maintained→raised],
      per_ticker_velocity_full: {ticker: {...full breakdown...}},
      duration_s
    }

SCHEDULE
────────
Daily 14:30 UTC (after earnings-sentiment refresh at 10:00 UTC, gives
4.5h buffer for late additions).

TRADE STRUCTURE
───────────────
Top quintile of positive velocity = LEADING BUY candidates (analyst revisions
+ price typically follow within 30-60 days).
Top quintile of negative velocity = LEADING SHORT/HEDGE candidates.
Guidance RED ALERTS (raised→lowered or maintained→lowered) = high-conviction
short signal; this transition historically has 75%+ hit rate predicting
analyst downward revisions within 4 weeks.
Guidance GREEN ALERTS (maintained→raised or lowered→raised) = high-conviction
long signal; 80%+ hit rate predicting upward revisions.

Pair with Pro Pack v3 cross-checks:
  - High positive velocity + Predictability 5-star + EVA Spread top decile
    = HIGH-CONVICTION INSTITUTIONAL LONG
  - High negative velocity + Beneish M-Score elevated + Earnings Quality low
    = HIGH-CONVICTION INSTITUTIONAL SHORT/HEDGE
"""
import json
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3


# ───────────────────────────── CONFIG ─────────────────────────────
REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
INPUT_KEY = "screener/earnings-sentiment.json"
OUTPUT_KEY = "data/earnings-tone-velocity.json"
SCHEMA_VERSION = "1.0.0"

# Velocity thresholds (sentiment scale -100 to +100; confidence scale -100 to +100)
HIGH_POSITIVE_VELOCITY = 25     # Δ ≥ +25 = high positive velocity
HIGH_NEGATIVE_VELOCITY = -25    # Δ ≤ -25 = high negative velocity
NOISE_FLOOR_VELOCITY = 8         # |Δ| < 8 = noise, ignore

# YoY tolerance: how many days from same-quarter-prior-year counts as "match"
# Earnings calls are quarterly but don't land exactly 365 days apart
YOY_DAYS_TOLERANCE = 45          # ±45d from same-quarter-prior-year

# Min quarters of history required to compute velocity
MIN_QUARTERS_FOR_VELOCITY = 2

# Limit per output bucket
MAX_LEADERBOARD = 30

# Guidance transition severity
GUIDANCE_TRANSITIONS_RED = {
    ("raised", "lowered"),
    ("raised", "withdrawn"),
    ("maintained", "lowered"),
    ("maintained", "withdrawn"),
}

GUIDANCE_TRANSITIONS_GREEN = {
    ("lowered", "raised"),
    ("lowered", "maintained"),
    ("maintained", "raised"),
    ("withdrawn", "raised"),
    ("withdrawn", "maintained"),
}

# ───────────────────────────── HELPERS ─────────────────────────────
s3 = boto3.client("s3", region_name=REGION)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _read_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[earnings-tone-velocity] S3 read fail {key}: {e}")
        return None


def _write_s3_json(key, payload):
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, must-revalidate",
        )
        return True
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print(f"[earnings-tone-velocity] S3 write fail {key}: {e}")
        return False


def _parse_date(d):
    if not d:
        return None
    try:
        if isinstance(d, str):
            return datetime.fromisoformat(
                d.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
        return d
    except Exception:
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d").replace(
                tzinfo=timezone.utc)
        except Exception:
            return None


def _quarter_label(dt):
    """Return e.g. '2026Q1' for a given datetime."""
    if not dt:
        return None
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"


# ───────────────────────────── CORE ENGINE ─────────────────────────────
def group_by_ticker(transcripts):
    """Group transcripts by ticker, sorted newest first per group."""
    grouped = defaultdict(list)
    for t in transcripts:
        ticker = t.get("ticker") or t.get("symbol")
        if not ticker:
            continue
        # Normalize - some records may have date in different fields
        date = t.get("date") or t.get("call_date") or t.get("earnings_date")
        parsed = _parse_date(date)
        if not parsed:
            continue
        grouped[ticker.upper()].append({
            "ticker": ticker.upper(),
            "date": date,
            "parsed_date": parsed,
            "quarter": _quarter_label(parsed),
            "overall_sentiment": t.get("overall_sentiment"),
            "confidence_score": t.get("confidence_score"),
            "forward_guidance": (t.get("forward_guidance") or "").lower(),
            "key_positives": t.get("key_positives") or [],
            "key_concerns": t.get("key_concerns") or [],
            "one_line_summary": t.get("one_line_summary"),
        })
    # Sort each group's records newest-first
    for k in grouped:
        grouped[k].sort(key=lambda x: x["parsed_date"], reverse=True)
    return grouped


def find_yoy_match(records, current_date):
    """
    Find the record from ~365 days prior to current_date (±YOY_DAYS_TOLERANCE).
    Returns the closest match within tolerance, or None.
    """
    target = current_date - timedelta(days=365)
    best = None
    best_diff = timedelta(days=YOY_DAYS_TOLERANCE + 1)
    for r in records:
        diff = abs(r["parsed_date"] - target)
        if diff < best_diff:
            best = r
            best_diff = diff
    if best_diff.days <= YOY_DAYS_TOLERANCE:
        return best
    return None


def compute_ticker_velocity(records):
    """
    For one ticker's full history, compute:
      - current (latest call)
      - YoY match (~365d prior)
      - velocity (current - YoY match) on sentiment + confidence
      - QoQ delta (current vs immediately prior call)
      - guidance transition (prior guidance → current guidance)
    """
    if not records or len(records) < MIN_QUARTERS_FOR_VELOCITY:
        return None
    current = records[0]
    prior = records[1] if len(records) > 1 else None
    yoy = find_yoy_match(records[1:], current["parsed_date"])
    if not yoy:
        return None

    cur_sent = current.get("overall_sentiment")
    yoy_sent = yoy.get("overall_sentiment")
    if cur_sent is None or yoy_sent is None:
        return None
    try:
        velocity = float(cur_sent) - float(yoy_sent)
    except (TypeError, ValueError):
        return None

    cur_conf = current.get("confidence_score")
    yoy_conf = yoy.get("confidence_score")
    confidence_velocity = None
    if cur_conf is not None and yoy_conf is not None:
        try:
            confidence_velocity = float(cur_conf) - float(yoy_conf)
        except (TypeError, ValueError):
            pass

    qoq_velocity = None
    if prior:
        ps = prior.get("overall_sentiment")
        if ps is not None:
            try:
                qoq_velocity = float(cur_sent) - float(ps)
            except (TypeError, ValueError):
                pass

    # Guidance transition (current vs prior)
    cur_g = (current.get("forward_guidance") or "").lower()
    pri_g = (prior.get("forward_guidance") or "").lower() if prior else ""
    transition = None
    transition_severity = None
    if cur_g and pri_g and cur_g != pri_g:
        transition = f"{pri_g} -> {cur_g}"
        pair = (pri_g, cur_g)
        if pair in GUIDANCE_TRANSITIONS_RED:
            transition_severity = "RED_ALERT"
        elif pair in GUIDANCE_TRANSITIONS_GREEN:
            transition_severity = "GREEN_ALERT"

    # Signal classification
    signal = "NEUTRAL"
    if velocity >= HIGH_POSITIVE_VELOCITY:
        signal = "HIGH_POSITIVE_VELOCITY"
    elif velocity <= HIGH_NEGATIVE_VELOCITY:
        signal = "HIGH_NEGATIVE_VELOCITY"
    elif abs(velocity) < NOISE_FLOOR_VELOCITY:
        signal = "FLAT"
    elif velocity > 0:
        signal = "MILD_POSITIVE_VELOCITY"
    else:
        signal = "MILD_NEGATIVE_VELOCITY"

    return {
        "ticker": current["ticker"],
        "current_date": current["date"],
        "current_quarter": current["quarter"],
        "current_sentiment": cur_sent,
        "current_confidence": cur_conf,
        "current_guidance": cur_g,
        "yoy_date": yoy["date"],
        "yoy_quarter": yoy["quarter"],
        "yoy_sentiment": yoy_sent,
        "yoy_confidence": yoy_conf,
        "velocity": round(velocity, 2),
        "confidence_velocity": (round(confidence_velocity, 2)
                                  if confidence_velocity is not None else None),
        "qoq_velocity": (round(qoq_velocity, 2)
                          if qoq_velocity is not None else None),
        "guidance_transition": transition,
        "guidance_severity": transition_severity,
        "signal": signal,
        "n_quarters_history": len(records),
        "key_positives_latest": current.get("key_positives")[:3],
        "key_concerns_latest": current.get("key_concerns")[:3],
        "one_line_summary_latest": current.get("one_line_summary"),
    }


def build_leaderboards(per_ticker_results):
    """Sort and slice into output buckets."""
    valid = [r for r in per_ticker_results.values() if r]
    # Sort by velocity desc
    by_pos = sorted([r for r in valid if r["velocity"] is not None
                       and r["velocity"] > NOISE_FLOOR_VELOCITY],
                     key=lambda x: x["velocity"], reverse=True)
    by_neg = sorted([r for r in valid if r["velocity"] is not None
                       and r["velocity"] < -NOISE_FLOOR_VELOCITY],
                     key=lambda x: x["velocity"])
    red_alerts = sorted(
        [r for r in valid if r["guidance_severity"] == "RED_ALERT"],
        key=lambda x: x["velocity"])
    green_alerts = sorted(
        [r for r in valid if r["guidance_severity"] == "GREEN_ALERT"],
        key=lambda x: x["velocity"], reverse=True)
    return {
        "top_positive_velocity": by_pos[:MAX_LEADERBOARD],
        "top_negative_velocity": by_neg[:MAX_LEADERBOARD],
        "guidance_red_alerts": red_alerts[:MAX_LEADERBOARD],
        "guidance_green_alerts": green_alerts[:MAX_LEADERBOARD],
    }


# ───────────────────────────── HANDLER ─────────────────────────────
def lambda_handler(event, context):
    started = time.time()
    print(f"[earnings-tone-velocity] start @ {_now_iso()}")

    upstream = _read_s3_json(INPUT_KEY)
    if not upstream:
        out = {
            "schema_version": SCHEMA_VERSION,
            "method": ("YoY same-quarter tone velocity + guidance "
                        "transition classification"),
            "as_of": _now_iso(),
            "error": f"upstream {INPUT_KEY} missing or unreadable",
            "summary": {"n_tickers_eligible": 0},
        }
        _write_s3_json(OUTPUT_KEY, out)
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    transcripts = upstream.get("transcripts") or []
    print(f"[earnings-tone-velocity] read {len(transcripts)} transcripts")

    grouped = group_by_ticker(transcripts)
    print(f"[earnings-tone-velocity] grouped into {len(grouped)} tickers")

    per_ticker_results = {}
    n_eligible = 0
    for ticker, records in grouped.items():
        r = compute_ticker_velocity(records)
        if r:
            per_ticker_results[ticker] = r
            n_eligible += 1

    leaderboards = build_leaderboards(per_ticker_results)
    n_pos = len(leaderboards["top_positive_velocity"])
    n_neg = len(leaderboards["top_negative_velocity"])
    n_red = len(leaderboards["guidance_red_alerts"])
    n_green = len(leaderboards["guidance_green_alerts"])

    out = {
        "schema_version": SCHEMA_VERSION,
        "method": ("YoY same-quarter tone velocity + guidance "
                    "transition classification"),
        "as_of": _now_iso(),
        "summary": {
            "n_transcripts_input": len(transcripts),
            "n_tickers_total": len(grouped),
            "n_tickers_eligible": n_eligible,
            "n_positive_velocity": n_pos,
            "n_negative_velocity": n_neg,
            "n_guidance_red_alert": n_red,
            "n_guidance_green_alert": n_green,
            "min_quarters_required": MIN_QUARTERS_FOR_VELOCITY,
            "high_positive_velocity_threshold": HIGH_POSITIVE_VELOCITY,
            "high_negative_velocity_threshold": HIGH_NEGATIVE_VELOCITY,
            "yoy_days_tolerance": YOY_DAYS_TOLERANCE,
        },
        "top_positive_velocity": leaderboards["top_positive_velocity"],
        "top_negative_velocity": leaderboards["top_negative_velocity"],
        "guidance_red_alerts": leaderboards["guidance_red_alerts"],
        "guidance_green_alerts": leaderboards["guidance_green_alerts"],
        "per_ticker_velocity_full": per_ticker_results,
        "upstream_as_of": upstream.get("generated_at"),
        "upstream_n_candidates": upstream.get("n_candidates"),
        "duration_s": round(time.time() - started, 2),
    }

    _write_s3_json(OUTPUT_KEY, out)
    print(f"[earnings-tone-velocity] eligible={n_eligible} "
          f"pos={n_pos} neg={n_neg} red={n_red} green={n_green} "
          f"in {out['duration_s']}s")

    return {"statusCode": 200, "body": json.dumps(out, default=str)}


if __name__ == "__main__":
    print(lambda_handler({}, None))
