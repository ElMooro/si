"""justhodl-alpha-compass — single-screen landing payload.

THE CONSUMER QUESTION
─────────────────────
"What do I buy RIGHT NOW, how much, and where do I get out if I'm wrong?"

This Lambda is the joiner. It does not generate signals. It does not score
conviction. It does not learn anything. It assembles the platform's own
research output into one institutional-format card, the way a desk briefing
note is structured.

INPUT FEEDS  (all already published by other engines)
──────────────────────────────────────────────────────
  conviction.json                  → top-of-system setups, 0-100 score, evidence
  magnitude-distributions.json     → realised return distribution per signal-stack
  signal-scorecard.json            → per-signal hit rate (Wilson LB), avg return
  portfolio/sizer-v2.json          → horizon-aware Kelly position sizing
  data/regime-flag.json (or eq.)   → current macro regime, for context badge
  miss-summary.json                → 30d miss totals (shown as "coverage state")

OUTPUT
──────
  data/alpha-compass.json — consumed by alpha-compass.html:
    {
      generated_at, regime, regime_label, regime_color,
      top_calls:   [ { rank, ticker, label, direction, conviction,
                         confidence_band, n_engines, n_families,
                         thesis, invalidation,
                         hist: { n, median, p25, p75, win_rate, ... },
                         sizing: { kelly_pct, dollar_at_100k, ... },
                         stop_pct,  # from p25 of historical stack
                         target_pct # from p75 of historical stack
                     }, ... ],   # top 3
      watchlist:   [ ... ],     # next 5-10
      coverage:    { misses_30d_total, near_misses_30d, top_uncovered_sectors },
      links: { ... }
    }

SCHEDULE
────────
cron(0 */3 * * ? *) — every 3 hours, matching conviction-engine cadence.
"""

import json
import os
import statistics
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

KEY_CONVICTION  = "data/conviction.json"
KEY_MAGDIST     = "data/magnitude-distributions.json"
KEY_SCORECARD   = "data/signal-scorecard.json"
KEY_SIZER       = "portfolio/sizer-v2.json"
KEY_REGIME      = "data/regime-flag.json"
KEY_MISS_SUMMARY = "data/miss-summary.json"
OUTPUT_KEY      = "data/alpha-compass.json"

s3 = boto3.client("s3", region_name=REGION)


def _decimal_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"unencodeable {type(o)}")


def safe_load(key: str) -> dict:
    """Load a JSON object from S3. Returns {} on any failure (graceful degrade)."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError, KeyError) as e:
        print(f"[compass] could not load {key}: {e}")
        return {}


def find_matching_stack(stacks: list, contributing_engines: list,
                         horizon_hint: int = 30) -> dict:
    """Match a conviction setup's contributing engines to a published stack.

    Strategy:
      1. Build a candidate signal-name set from the contributing-engines list.
      2. Score each published stack by Jaccard overlap with that set.
      3. Return the best match at the requested horizon (or any horizon if
         no horizon match found and overlap is high enough).
    """
    if not stacks or not contributing_engines:
        return {}

    # Extract a set of signal names from the contributing engines (they may
    # store the signal type under different keys depending on origin).
    candidate = set()
    for e in contributing_engines:
        for k in ("signal", "engine", "name", "id"):
            v = e.get(k) if isinstance(e, dict) else None
            if v:
                candidate.add(str(v).strip().lower())
                break

    if not candidate:
        return {}

    best, best_score, best_horizon_score = None, 0.0, 0.0
    for s in stacks:
        members = set(str(x).strip().lower() for x in (s.get("signals") or []))
        if not members:
            continue
        inter = candidate & members
        if not inter:
            continue
        union = candidate | members
        jaccard = len(inter) / len(union)
        horizon_match = 1.0 if s.get("horizon_days") == horizon_hint else 0.5
        score = jaccard * horizon_match
        if score > best_score:
            best, best_score, best_horizon_score = s, score, horizon_match

    if best and best_score >= 0.30:  # require minimum overlap
        return best
    return {}


def stop_target_from_distribution(stack_match: dict) -> tuple:
    """Derive stop and target percentages from the realised return distribution.

    Institutional convention:
      • Stop  ≈ p25 of historical realised returns (you accept the bottom
                quartile of outcomes; below this you take the loss).
      • Target ≈ p75 of historical realised returns (you take profit at the
                  upper-quartile expectation; above this is gravy).
    """
    if not stack_match:
        return None, None
    p25 = stack_match.get("p25")
    p75 = stack_match.get("p75")
    return (
        round(p25, 1) if p25 is not None else None,
        round(p75, 1) if p75 is not None else None,
    )


def scorecard_lookup(scorecard: dict, engines: list) -> dict:
    """Aggregate scorecard metrics across the engines in a setup."""
    if not scorecard or not engines:
        return {}
    by_type = scorecard.get("by_signal_type") or scorecard.get("signals") or {}
    if isinstance(by_type, list):
        by_type = {r.get("signal_type") or r.get("name"): r for r in by_type if isinstance(r, dict)}
    hits, ns, avg_returns, grades = [], [], [], []
    for e in engines:
        sig = (e.get("signal") if isinstance(e, dict) else None) or ""
        rec = by_type.get(sig) or by_type.get(sig.lower())
        if not isinstance(rec, dict):
            continue
        n = rec.get("n_scored") or rec.get("n")
        wlb = rec.get("wilson_lb") or rec.get("hit_rate_lb")
        ar = rec.get("avg_return") or rec.get("mean_return")
        grade = rec.get("grade")
        if n is not None: ns.append(n)
        if wlb is not None: hits.append(float(wlb))
        if ar is not None: avg_returns.append(float(ar))
        if grade: grades.append(grade)
    if not (ns or hits or avg_returns):
        return {}
    return {
        "engines_with_record": len(ns) if ns else len(hits),
        "median_n":           int(statistics.median(ns)) if ns else None,
        "min_wilson_lb":      round(min(hits), 3) if hits else None,
        "mean_wilson_lb":     round(statistics.fmean(hits), 3) if hits else None,
        "mean_avg_return":    round(statistics.fmean(avg_returns), 3) if avg_returns else None,
        "best_grade":         max(grades) if grades else None,
    }


def sizer_lookup(sizer: dict, subject: str, ticker: str) -> dict:
    """Find sizing recommendation for this subject or ticker in sizer-v2 output."""
    if not sizer:
        return {}
    by_key = sizer.get("by_subject") or sizer.get("recommendations") or {}
    if isinstance(by_key, list):
        # Match by either subject or ticker
        for r in by_key:
            if not isinstance(r, dict):
                continue
            if (r.get("subject") == subject) or (r.get("ticker") == ticker):
                return r
        return {}
    if isinstance(by_key, dict):
        return by_key.get(subject) or by_key.get(ticker) or {}
    return {}


def regime_context(regime_json: dict) -> dict:
    """Normalise a regime payload into a small badge object."""
    if not regime_json:
        return {"label": "Unknown", "color": "#888", "regime": None}
    reg = (regime_json.get("regime")
           or regime_json.get("market_regime")
           or regime_json.get("dominant_regime")
           or "")
    reg_label = str(reg).replace("_", " ").title() if reg else "Unknown"
    # crude colour mapping — UI can override
    colour = {
        "RISK_ON":     "#00e68a", "EXPANSION":  "#00e68a",
        "NORMAL":      "#4dabf7", "STABLE":     "#4dabf7",
        "TRANSITION":  "#ffd43b", "CAUTION":    "#ffd43b",
        "RISK_OFF":    "#ff4757", "CRISIS":     "#ff4757",
        "CONTRACTION": "#ff922b",
    }.get(reg.upper() if isinstance(reg, str) else "", "#888")
    return {
        "label": reg_label,
        "color": colour,
        "regime": reg,
        "khalid_score": regime_json.get("khalid_score") or regime_json.get("score"),
    }


def build_card(setup: dict, magdist: dict, scorecard: dict, sizer: dict,
                rank: int) -> dict:
    """Assemble one institutional-format card."""
    engines = setup.get("contributing_engines") or []
    horizon_hint = 30
    stack_match = find_matching_stack(
        magdist.get("stacks") or [], engines, horizon_hint=horizon_hint
    )
    stop_pct, target_pct = stop_target_from_distribution(stack_match)
    scard = scorecard_lookup(scorecard, engines)

    # Try to find the top ticker for this setup (conviction-engine may have
    # surfaced one or more underneath the subject card).
    top_tickers = setup.get("top_tickers") or setup.get("tickers") or []
    primary_ticker = None
    if top_tickers and isinstance(top_tickers, list):
        first = top_tickers[0]
        if isinstance(first, dict):
            primary_ticker = first.get("ticker") or first.get("symbol")
        elif isinstance(first, str):
            primary_ticker = first

    sizing = sizer_lookup(sizer, setup.get("subject"), primary_ticker)

    card = {
        "rank":              rank,
        "subject":           setup.get("subject"),
        "ticker":            primary_ticker,
        "direction":         setup.get("direction"),
        "conviction":        setup.get("conviction"),
        "confidence_band":   setup.get("confidence"),
        "n_engines":         setup.get("n_engines"),
        "n_families":        setup.get("n_families"),
        "agreement_pct":     setup.get("agreement_pct"),
        "thesis":            setup.get("thesis"),
        "invalidation":      setup.get("invalidation"),
        "engines": [
            {
                "signal":   e.get("signal"),
                "label":    e.get("signal_label") or e.get("signal"),
                "read":     e.get("read"),
                "skill":    e.get("skill_weight"),
            } for e in engines[:8] if isinstance(e, dict)
        ],
        "distribution": {
            **stack_match,
        } if stack_match else None,
        "scorecard": scard or None,
        "sizing": {
            "kelly_pct":     sizing.get("kelly_pct") or sizing.get("fraction"),
            "dollar_at_100k": sizing.get("dollar_at_100k") or sizing.get("nominal"),
            "horizon":       sizing.get("horizon") or sizing.get("horizon_days"),
        } if sizing else None,
        "stop_pct":   stop_pct,
        "target_pct": target_pct,
        "tickers":    top_tickers[:5] if isinstance(top_tickers, list) else [],
    }
    return card


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)

    conviction   = safe_load(KEY_CONVICTION)
    magdist      = safe_load(KEY_MAGDIST)
    scorecard    = safe_load(KEY_SCORECARD)
    sizer        = safe_load(KEY_SIZER)
    regime_json  = safe_load(KEY_REGIME)
    miss_summary = safe_load(KEY_MISS_SUMMARY)

    setups = (conviction.get("setups")
              or conviction.get("conviction_sheet")
              or conviction.get("ranked") or [])
    # conviction-engine output may be a list of subject blocks; sort by conviction desc
    if setups and isinstance(setups, list):
        setups.sort(key=lambda r: -(r.get("conviction") or 0))

    top_calls = [build_card(s, magdist, scorecard, sizer, i + 1)
                 for i, s in enumerate(setups[:3])]
    watchlist = [build_card(s, magdist, scorecard, sizer, i + 4)
                 for i, s in enumerate(setups[3:13])]

    coverage = {
        "miss_summary_30d_totals": miss_summary.get("totals") or {},
        "top_recurring_misses":    list((miss_summary.get("top_recurring_tickers") or {}).items())[:10],
    }

    output = {
        "schema_version":  "1.0",
        "generated_at":    started.isoformat(),
        "regime":          regime_context(regime_json),
        "top_calls":       top_calls,
        "watchlist":       watchlist,
        "coverage":        coverage,
        "source_feeds": {
            "conviction":              {"present": bool(conviction), "as_of": conviction.get("generated_at")},
            "magnitude_distributions": {"present": bool(magdist),    "as_of": magdist.get("generated_at"),
                                         "stacks": magdist.get("totals", {}).get("published_stacks")},
            "scorecard":               {"present": bool(scorecard)},
            "sizer":                   {"present": bool(sizer)},
            "miss_summary":            {"present": bool(miss_summary)},
        },
    }

    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=_decimal_default, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    print(f"[compass] top_calls={len(top_calls)} watchlist={len(watchlist)} "
          f"regime={output['regime']['label']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "top_calls": len(top_calls),
            "watchlist": len(watchlist),
        }),
    }


lambda_handler = handler
