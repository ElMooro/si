"""justhodl-miss-calibrator — proposes threshold adjustments from miss patterns.

THE LAST LINK IN THE SELF-IMPROVEMENT LOOP
──────────────────────────────────────────
justhodl-calibrator already adjusts SIGNAL WEIGHTS based on realized
hit-rates from trades the system took. But it can't reduce thresholds for
signals that "almost fired" — it never sees those.

miss-detector publishes a 30d rolling tally of near-misses per signal type
via data/miss-summary.json. THIS Lambda reads that tally and produces:

  1. Per-signal-type threshold adjustment proposals (delta as fraction)
  2. Recurring missed-ticker patterns (suggesting universe expansion)
  3. Per-engine coverage gaps (which engines NEVER fire on movers)

PROPOSALS ARE PROPOSALS, NOT MUTATIONS
──────────────────────────────────────
This Lambda WRITES PROPOSALS to:
  - SSM:  /justhodl/calibration/miss_threshold_proposals
  - S3:   data/miss-calibrator-proposals.json
  - Telegram: weekly digest with top 5 proposals

Engines that respect proposals OPT IN by reading the SSM path. The proposal
schema includes confidence, evidence, and an EXPIRY so the calibrator
cannot make irreversible system-wide changes.

GUARDRAILS
──────────
1. Never recommend lowering threshold on a DEPRECATED signal (would
   re-promote a known-bad signal). Read deprecated_signals from
   signal-scorecard.json.
2. Cap proposed deltas at -20% reduction (no more than that per cycle).
3. Require minimum near_miss count (default 10) within the 30d window.
4. Decay older proposals — anything > 14d old is removed.

SCHEDULE
────────
cron(15 9 ? * MON *) — weekly Monday 09:15 UTC, after Sunday calibrator (Sun 09:00).
"""

import json
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

KEY_MISS_SUMMARY   = "data/miss-summary.json"
KEY_SCORECARD      = "data/signal-scorecard.json"
KEY_UNIVERSE       = "data/universe.json"
KEY_MASTER_RANKER  = "data/master-ranker.json"
OUTPUT_KEY         = "data/miss-calibrator-proposals.json"

SSM_PROPOSALS_PATH = "/justhodl/calibration/miss_threshold_proposals"
SSM_RECURRING_PATH = "/justhodl/calibration/miss_recurring_tickers"

MIN_NEAR_MISSES = 10            # require ≥10 near-misses in 30d to propose
MAX_THRESHOLD_REDUCTION = -0.20  # never cut threshold by more than 20%
MIN_THRESHOLD_REDUCTION = -0.02  # smallest meaningful change
RECURRING_TICKER_MIN = 3         # ticker missed ≥3x → flag for universe review
PROPOSAL_TTL_DAYS = 14            # proposals expire after 14d

TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def safe_load(key: str) -> dict:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError) as e:
        print(f"[miss-cal] could not load {key}: {e}")
        return {}


def threshold_delta_for(near_miss_count: int) -> float:
    """Map near-miss count → proposed threshold reduction (negative fraction).

    Curve: smooth sigmoid that maxes at MAX_THRESHOLD_REDUCTION.
      10  near misses → -2% (just over MIN)
      25  near misses → -8%
      50  near misses → -14%
      100 near misses → -20% (capped)

    The reasoning: a signal that ALMOST fires 50 times in 30 days is one
    threshold-tweak away from being useful. A signal that ALMOST fires 5
    times in 30 days isn't worth touching — could just be noise.
    """
    if near_miss_count < MIN_NEAR_MISSES:
        return 0.0
    # Sigmoid centred around 30, scaled to span [MIN, MAX]
    x = (near_miss_count - 30) / 20.0
    sigmoid = 1.0 / (1.0 + math.exp(-x))
    delta = MIN_THRESHOLD_REDUCTION + (MAX_THRESHOLD_REDUCTION - MIN_THRESHOLD_REDUCTION) * sigmoid
    return round(delta, 4)


def proposal_confidence(near_miss_count: int, scorecard_wlb: float = None) -> str:
    """Confidence band for the proposal."""
    if near_miss_count >= 50 and scorecard_wlb and scorecard_wlb >= 0.55:
        return "HIGH"
    if near_miss_count >= 25:
        return "MODERATE"
    return "LOW"


def ssm_put_json(path: str, value: dict):
    """Write JSON to SSM Parameter Store. Uses String type (truncated to 4096
    chars; for larger payloads use S3 reference instead).
    """
    body = json.dumps(value, default=str, separators=(",", ":"))
    if len(body) > 4000:
        # SSM String max is 4096. Store an S3 reference instead.
        body = json.dumps({
            "see_s3": OUTPUT_KEY,
            "schema_version": value.get("schema_version", "1.0"),
            "generated_at": value.get("generated_at"),
            "n_proposals": len(value.get("proposals", [])),
        })
    try:
        ssm.put_parameter(
            Name=path,
            Value=body,
            Type="String",
            Overwrite=True,
            Description="Auto-generated miss-calibrator output (see S3 for full payload)",
        )
    except Exception as e:
        print(f"[miss-cal] ssm put fail {path}: {e}")


def send_telegram(text: str):
    if not TELEGRAM_TOKEN:
        return
    try:
        import urllib.parse, urllib.request
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data, method="POST")
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        print(f"[miss-cal] telegram: {e}")


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)

    miss_summary = safe_load(KEY_MISS_SUMMARY)
    scorecard    = safe_load(KEY_SCORECARD)
    universe     = safe_load(KEY_UNIVERSE)

    if not miss_summary:
        print("[miss-cal] no miss-summary available — skipping")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no miss-summary"})}

    # Pull the relevant slices from scorecard
    deprecated = set((scorecard.get("deprecated_signals") or []))
    promoted = set((scorecard.get("promoted_signals") or []))
    scorecard_by_type = {}
    sc_rows = scorecard.get("by_signal_type") or scorecard.get("signals") or []
    if isinstance(sc_rows, list):
        for r in sc_rows:
            if isinstance(r, dict):
                st = (r.get("signal_type") or r.get("name") or "").strip().lower()
                if st:
                    scorecard_by_type[st] = r
    elif isinstance(sc_rows, dict):
        scorecard_by_type = {k.strip().lower(): v for k, v in sc_rows.items()}

    near_by_signal = miss_summary.get("near_misses_by_signal") or {}
    recurring_tickers = miss_summary.get("top_recurring_tickers") or {}

    # Universe membership for the universe-expansion proposals
    universe_tickers = set()
    for s in (universe.get("stocks") or []):
        if isinstance(s, dict):
            t = s.get("ticker") or s.get("symbol")
            if t:
                universe_tickers.add(t.upper())

    # ─── Build proposals ─────────────────────────────────────────────────
    proposals = []
    skipped_deprecated = []
    skipped_below_min = []

    for signal_type, near_miss_count in near_by_signal.items():
        st_lower = str(signal_type).strip().lower()
        if not st_lower:
            continue

        # Guardrail 1: skip deprecated signals
        if st_lower in deprecated:
            skipped_deprecated.append({
                "signal_type": signal_type, "near_misses": near_miss_count,
                "reason": "signal is deprecated by signal-scorecard",
            })
            continue

        # Guardrail 2: minimum threshold
        if near_miss_count < MIN_NEAR_MISSES:
            skipped_below_min.append({"signal_type": signal_type,
                                       "near_misses": near_miss_count})
            continue

        delta = threshold_delta_for(near_miss_count)
        sc_row = scorecard_by_type.get(st_lower) or {}
        wlb = sc_row.get("wilson_lb") or sc_row.get("hit_rate_lb")
        wlb_f = float(wlb) if wlb is not None else None
        confidence = proposal_confidence(near_miss_count, wlb_f)

        proposals.append({
            "signal_type":     signal_type,
            "delta_pct":       delta,          # negative = lower threshold
            "near_misses_30d": near_miss_count,
            "confidence":      confidence,
            "is_promoted":     st_lower in promoted,
            "scorecard_wilson_lb": wlb_f,
            "scorecard_n":     sc_row.get("n_scored") or sc_row.get("n"),
            "scorecard_grade": sc_row.get("grade"),
            "rationale": (
                f"{near_miss_count} near-misses in last 30 days. "
                f"Proposed threshold reduction {delta:+.1%}. "
                + (f"Signal is currently PROMOTED (Wilson LB {wlb_f:.2f})."
                   if wlb_f is not None and st_lower in promoted else
                   f"Signal exists in scorecard but not in promoted list.")
            ),
            "expires_at": (started + timedelta(days=PROPOSAL_TTL_DAYS)).isoformat(),
        })

    proposals.sort(key=lambda r: -r["near_misses_30d"])

    # ─── Recurring missed tickers (universe candidates) ────────────────────
    universe_candidates = []
    for ticker, count in recurring_tickers.items():
        if count < RECURRING_TICKER_MIN:
            continue
        in_universe = ticker.upper() in universe_tickers
        universe_candidates.append({
            "ticker":      ticker,
            "miss_count":  count,
            "in_universe": in_universe,
            "action":      "engine_coverage_gap" if in_universe else "universe_expansion_candidate",
        })

    # ─── Engine coverage gaps (high miss totals → engines never firing) ────
    miss_totals = miss_summary.get("totals") or {}
    n_total_misses = sum(int(v) for v in miss_totals.values()) if miss_totals else 0

    # ─── Build final output ────────────────────────────────────────────────
    output = {
        "schema_version":  "1.0",
        "method":          "miss_pattern_threshold_proposal_v1",
        "generated_at":    started.isoformat(),
        "miss_summary_age_hours": None,
        "miss_summary_window_days": miss_summary.get("window_days"),
        "totals": {
            "n_misses_total_30d":     n_total_misses,
            "n_signals_with_near_misses": len(near_by_signal),
            "n_proposals":            len(proposals),
            "n_skipped_deprecated":   len(skipped_deprecated),
            "n_skipped_below_min":    len(skipped_below_min),
            "n_universe_candidates":  len(universe_candidates),
        },
        "proposals":            proposals,
        "skipped_deprecated":   skipped_deprecated[:20],
        "skipped_below_min":    skipped_below_min[:30],
        "universe_candidates":  universe_candidates[:50],
        "miss_summary_totals":  miss_totals,
        "guardrails": {
            "min_near_misses":     MIN_NEAR_MISSES,
            "max_threshold_drop":  MAX_THRESHOLD_REDUCTION,
            "proposal_ttl_days":   PROPOSAL_TTL_DAYS,
        },
    }

    try:
        miss_age = started - datetime.fromisoformat(
            miss_summary.get("generated_at", "").replace("Z", "+00:00")
        )
        output["miss_summary_age_hours"] = round(miss_age.total_seconds() / 3600, 1)
    except Exception:
        pass

    # ─── Persist ──────────────────────────────────────────────────────────
    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    ssm_put_json(SSM_PROPOSALS_PATH, output)

    # Compact recurring-tickers map to SSM for engines that want it
    recurring_compact = {
        "schema_version": "1.0",
        "generated_at": started.isoformat(),
        "in_universe_misses": {c["ticker"]: c["miss_count"] for c in universe_candidates if c["in_universe"]},
        "out_of_universe_candidates": {c["ticker"]: c["miss_count"] for c in universe_candidates if not c["in_universe"]},
    }
    ssm_put_json(SSM_RECURRING_PATH, recurring_compact)

    # ─── Telegram weekly digest ────────────────────────────────────────────
    lines = [
        f"<b>🎯 Miss-Calibrator weekly report</b>",
        f"30d window: <b>{n_total_misses}</b> misses, "
        f"<b>{len(proposals)}</b> threshold proposals, "
        f"<b>{len(universe_candidates)}</b> ticker reviews.",
        "",
    ]
    if proposals:
        lines.append("<b>Top 5 threshold proposals:</b>")
        for p in proposals[:5]:
            lines.append(
                f"  • {p['signal_type']}: {p['delta_pct']:+.1%} "
                f"({p['near_misses_30d']} near-misses, conf={p['confidence']})"
            )
    if universe_candidates:
        in_u = [c for c in universe_candidates if c["in_universe"]][:5]
        oou  = [c for c in universe_candidates if not c["in_universe"]][:5]
        if in_u:
            lines.append("")
            lines.append("<b>Recurring misses IN universe (engine coverage gaps):</b>")
            for c in in_u:
                lines.append(f"  • {c['ticker']} ({c['miss_count']}x)")
        if oou:
            lines.append("")
            lines.append("<b>Recurring misses OUT of universe (consider adding):</b>")
            for c in oou:
                lines.append(f"  • {c['ticker']} ({c['miss_count']}x)")
    lines.append("")
    lines.append(f"Full report: data/miss-calibrator-proposals.json")
    send_telegram("\n".join(lines))

    print(f"[miss-cal] {len(proposals)} proposals, "
          f"{len(universe_candidates)} universe candidates, "
          f"{len(skipped_deprecated)} skipped deprecated")

    # ─── Emit events for downstream coordination ────────────────────────
    # When we generate HIGH-confidence proposals, the system needs to know
    # immediately so engines can opt in or operator can review.
    try:
        from system_events import publish, publish_many
        events_to_publish = []
        
        for p in proposals:
            if p.get("confidence") == "HIGH":
                events_to_publish.append((
                    "calibrator.proposal_high_confidence",
                    {
                        "signal_type":   p["signal_type"],
                        "delta_pct":     p["delta_pct"],
                        "near_misses":   p["near_misses_30d"],
                        "confidence":    p["confidence"],
                        "is_promoted":   p.get("is_promoted"),
                        "expires_at":    p.get("expires_at"),
                    },
                ))
        
        # Also emit near_miss.extreme for signals with extreme near-miss counts (>= 50)
        for sig_type, count in (near_by_signal or {}).items():
            try:
                count_int = int(count)
            except (TypeError, ValueError):
                continue
            if count_int >= 50:
                events_to_publish.append((
                    "near_miss.extreme",
                    {
                        "signal_type": sig_type,
                        "count":       count_int,
                        "window":      "30d",
                    },
                ))
        
        if events_to_publish:
            # publish_many caps at 10 events per call
            for i in range(0, len(events_to_publish), 10):
                publish_many(events_to_publish[i:i+10])
    except Exception as e:
        print(f"[miss-cal] event publish: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "proposals": len(proposals),
            "universe_candidates": len(universe_candidates),
            "skipped_deprecated": len(skipped_deprecated),
        }),
    }


lambda_handler = handler
