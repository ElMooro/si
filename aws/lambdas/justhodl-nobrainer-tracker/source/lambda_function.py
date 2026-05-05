"""
justhodl-nobrainer-tracker  (Layer 6 of nobrainer hunter)
=========================================================
Reads Layer 4 leaderboard and logs each top-tier nobrainer call into the
existing justhodl-signals DDB table so the horizon-aware calibrator measures
its performance over 30 / 60 / 90 / 180 day windows.

Why this matters:
  Without this layer, no-one is grading the system. We don't know if
  asymmetric_score=82 means anything until we measure how those tickers
  performed 30/60/90 days later. With it: every top candidate becomes a
  signal_type='nobrainer_<theme>' record that the outcome-checker scores
  against benchmark, and the calibrator computes per-horizon weights.

Logging policy (avoids spam):
  • Only log candidates with asymmetric_score >= MIN_TRACK_SCORE (default 60)
  • Dedupe per (ticker, theme) per day — don't re-log the same call within
    the same day, only when the score crosses a threshold or the call resets
  • signal_type takes the form: nobrainer_<theme_etf>  (e.g. nobrainer_SMH)
    so calibrator naturally groups by theme — we'll learn whether the system
    is better at memory plays vs uranium plays
  • predicted_direction = "UP" with confidence proportional to score/100
  • windows = [30, 60, 90, 180] for asymmetric trades (long-tail catalysts)
  • baseline_price auto-fetched by signal-logger pattern via Polygon
  • metadata captures the full 5-factor breakdown for forensics

Schedule: rate(1 hour) — runs every hour, but logs only when score crosses
                         threshold or new day begins. Heavy dedup ensures
                         most invocations are no-ops.

Reads:
  s3://justhodl-dashboard-live/data/nobrainers.json (Layer 4)

Writes (DDB):
  justhodl-signals table — one record per (ticker, theme) call, with TTL 365d

Reads (state, for dedup):
  s3://justhodl-dashboard-live/portfolio/nobrainer-tracker-state.json
"""
import json
import os
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"
STATE_KEY = "portfolio/nobrainer-tracker-state.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
POLYGON_BASE = "https://api.polygon.io"

S3 = boto3.client("s3", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)

MIN_TRACK_SCORE = float(os.environ.get("MIN_TRACK_SCORE", "60"))
MAX_LOGS_PER_RUN = int(os.environ.get("MAX_LOGS_PER_RUN", "20"))
# Re-log a (ticker, theme) signal only if score has changed by >= this much
SCORE_DELTA_TRIGGER = float(os.environ.get("SCORE_DELTA_TRIGGER", "5.0"))
# Re-log on schedule even with no score change (stay-in-market re-confirmation)
RECONFIRM_HOURS = int(os.environ.get("RECONFIRM_HOURS", "168"))  # weekly


# ─────────────────────────────────────────────────────────────────────────────
# DECIMAL helper for DDB
# ─────────────────────────────────────────────────────────────────────────────
def f2d(v):
    """Recursively coerce floats → Decimal for DDB."""
    if isinstance(v, float):
        return Decimal(str(round(v, 8)))
    if isinstance(v, dict):
        return {k: f2d(x) for k, x in v.items()}
    if isinstance(v, list):
        return [f2d(x) for x in v]
    return v


# ─────────────────────────────────────────────────────────────────────────────
# POLYGON BASELINE PRICE FETCH (mirrors signal-logger pattern)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_polygon_close(ticker, days_back=7):
    """Most recent daily close for ticker. None on failure."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start.isoformat()}/{end.isoformat()}?adjusted=true&sort=desc&limit=10"
           f"&apiKey={POLYGON_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-nobrainer-tracker/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            if r.status != 200:
                return None
            data = json.loads(r.read().decode())
            results = data.get("results") or []
            if not results:
                return None
            return float(results[0].get("c"))
    except Exception as e:
        print(f"[poly] {ticker} err: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# STATE (dedup)
# ─────────────────────────────────────────────────────────────────────────────
def load_state():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"last_logged": {}, "n_runs": 0, "n_logs_total": 0}


def save_state(state):
    try:
        S3.put_object(
            Bucket=BUCKET,
            Key=STATE_KEY,
            Body=json.dumps(state, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="max-age=60, public",
        )
    except Exception as e:
        print(f"[state] save err: {e}")


def should_log(state, ticker, theme, current_score, now):
    """Dedup logic — log only on (a) first sighting, (b) score change >= delta,
    or (c) reconfirm window elapsed."""
    key = f"{ticker}|{theme}"
    last = state.get("last_logged", {}).get(key)
    if not last:
        return True, "first_sighting"

    # Score change?
    last_score = last.get("score", 0)
    if abs(current_score - last_score) >= SCORE_DELTA_TRIGGER:
        return True, f"score_delta {last_score:.1f}→{current_score:.1f}"

    # Reconfirm window?
    last_ts = datetime.fromisoformat(last["logged_at"])
    if (now - last_ts).total_seconds() / 3600.0 >= RECONFIRM_HOURS:
        return True, f"reconfirm after {RECONFIRM_HOURS}h"

    return False, f"dedup (last logged {(now - last_ts).total_seconds()/3600.0:.1f}h ago, "\
                  f"score {last_score:.1f}→{current_score:.1f})"


def update_state(state, ticker, theme, score, signal_id, now):
    key = f"{ticker}|{theme}"
    state.setdefault("last_logged", {})[key] = {
        "score": score,
        "signal_id": signal_id,
        "logged_at": now.isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL LOGGER (mirror of justhodl-signal-logger schema v2)
# ─────────────────────────────────────────────────────────────────────────────
def log_nobrainer_signal(candidate, regime_snapshot=None):
    """Write the signal record to DDB. Returns signal_id or None on failure."""
    table = DDB.Table(SIGNALS_TABLE)
    now = datetime.now(timezone.utc)
    sid = str(uuid.uuid4())

    ticker = candidate["ticker"]
    theme = candidate["theme_etf"]
    score = candidate["asymmetric_score"]

    # Fetch baseline price for ticker
    price = fetch_polygon_close(ticker)
    if price is None:
        print(f"[track] {ticker} — baseline price unavailable, skipping")
        return None

    # Predict UP for TIER_A and TIER_B, NEUTRAL for TIER_C/D
    if score >= 70:
        pred = "UP"
    elif score >= 60:
        pred = "UP"  # still bullish, just less confident
    else:
        pred = "NEUTRAL"

    # Confidence ∝ score
    conf = min(1.0, score / 100.0)

    # Long-tail windows for asymmetric trades
    windows = [30, 60, 90, 180]
    ts = {f"day_{d}": (now + timedelta(days=d)).isoformat() for d in windows}

    signal_type = f"nobrainer_{theme}"

    fund = candidate.get("fundamentals") or {}
    factors = candidate.get("factors") or {}

    # Predicted magnitude — score/10 as a rough %-target heuristic. A score of
    # 80 implies +8% over the primary horizon as a baseline expectation.
    magnitude_pct = round(score / 10.0, 2) if pred == "UP" else 0.0
    target_price = price * (1.0 + magnitude_pct / 100.0)

    item = {
        "signal_id": sid,
        "signal_type": signal_type,
        "signal_value": ticker,
        "predicted_direction": pred,
        "confidence": f2d(conf),
        "measure_against": ticker,
        "baseline_price": f2d(price),
        "baseline_benchmark_price": None,
        "benchmark": None,
        "check_windows": [str(d) for d in windows],
        "check_timestamps": ts,
        "outcomes": {},
        "accuracy_scores": {},
        "logged_at": now.isoformat(),
        "logged_epoch": int(now.timestamp()),
        "status": "pending",
        "metadata": f2d({
            "asymmetric_score": score,
            "flag": candidate.get("flag"),
            "tier": candidate.get("tier"),
            "theme_phase": candidate.get("theme_phase"),
            "theme_name": candidate.get("theme_name"),
            "factors": factors,
            "fundamentals_summary": {
                "market_cap":    fund.get("market_cap"),
                "revenue_ttm":   fund.get("revenue_ttm"),
                "p_s":           fund.get("p_s"),
                "p_e":           fund.get("p_e"),
                "ev_ebitda":     fund.get("ev_ebitda"),
                "fcf_yield":     fund.get("fcf_yield"),
                "mcap_to_rev":   fund.get("mcap_to_rev"),
                "industry":      fund.get("industry"),
            },
            "next_earnings": candidate.get("next_earnings"),
        }),
        "ttl": int((now + timedelta(days=365)).timestamp()),
        "schema_version": "2",
        "predicted_magnitude_pct": f2d(magnitude_pct),
        "predicted_target_price": f2d(target_price),
        "horizon_days_primary": max(windows),
        "regime_at_log": (regime_snapshot or {}).get("regime"),
        "khalid_score_at_log": (regime_snapshot or {}).get("khalid_score"),
        "ka_score_at_log": (regime_snapshot or {}).get("khalid_score"),  # alias
        "rationale": (
            f"Layer 4 nobrainer hunter — {candidate.get('flag')} score {score}/100. "
            f"Theme {theme} ({candidate.get('theme_phase')}) supply_inflection="
            f"{factors.get('supply_inflection')}/100, valuation_asym="
            f"{factors.get('valuation_asym')}/100, tier-{candidate.get('tier')}. "
            f"mcap_to_rev={fund.get('mcap_to_rev')}."
        ),
        "supporting_signals": [
            f"{s.get('signal')}={s.get('score')} ({s.get('flag')})"
            for s in (candidate.get("supply_signals") or [])[:3]
        ],
    }

    try:
        table.put_item(Item=item)
        print(f"[track-LOG] {signal_type}={ticker} {pred} conf={conf:.2f} baseline=${price:.2f} score={score}")
        return sid
    except Exception as e:
        print(f"[track-LOG] DDB put_item err for {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# REGIME SNAPSHOT (best-effort for context tag)
# ─────────────────────────────────────────────────────────────────────────────
def capture_regime():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/report.json")
        d = json.loads(obj["Body"].read())
        ki = d.get("khalid_index")
        if isinstance(ki, dict):
            return {"khalid_score": int(ki.get("score", 0)) if ki.get("score") else None,
                    "regime": ki.get("regime") or d.get("regime")}
        if ki is not None:
            return {"khalid_score": int(float(ki)), "regime": d.get("regime")}
        return {"regime": d.get("regime"), "khalid_score": None}
    except Exception as e:
        print(f"[regime] snapshot failed (non-fatal): {e}")
        return {"regime": None, "khalid_score": None}


# ─────────────────────────────────────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[track] Layer 6 — nobrainer-tracker starting")
    now = datetime.now(timezone.utc)

    # 1. Load Layer 4
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
        layer4 = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[track] FATAL — Layer 4 missing: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    leaderboard = (layer4.get("summary") or {}).get("top_25_overall", [])
    candidates = [x for x in leaderboard if x.get("asymmetric_score", 0) >= MIN_TRACK_SCORE]
    print(f"[track] leaderboard: {len(leaderboard)}  candidates >= {MIN_TRACK_SCORE}: {len(candidates)}")

    # 2. Load state
    state = load_state()
    state["n_runs"] = state.get("n_runs", 0) + 1
    state.setdefault("history", [])

    # 3. Capture regime
    regime = capture_regime()
    print(f"[track] regime: {regime}")

    # 4. Log each candidate (with dedup)
    n_logged = 0
    n_skipped = 0
    n_errors = 0
    log_results = []

    for c in candidates:
        if n_logged >= MAX_LOGS_PER_RUN:
            print(f"[track] reached MAX_LOGS_PER_RUN ({MAX_LOGS_PER_RUN}) — stopping")
            break

        ticker = c["ticker"]
        theme = c["theme_etf"]
        score = c["asymmetric_score"]

        ok, reason = should_log(state, ticker, theme, score, now)
        if not ok:
            n_skipped += 1
            print(f"[track] SKIP {ticker}/{theme} — {reason}")
            log_results.append({"ticker": ticker, "theme": theme, "skipped": True, "reason": reason})
            continue

        sid = log_nobrainer_signal(c, regime_snapshot=regime)
        if sid:
            update_state(state, ticker, theme, score, sid, now)
            n_logged += 1
            log_results.append({"ticker": ticker, "theme": theme, "signal_id": sid,
                                "score": score, "reason": reason})
        else:
            n_errors += 1
            log_results.append({"ticker": ticker, "theme": theme, "error": True})

    state["n_logs_total"] = state.get("n_logs_total", 0) + n_logged
    # Keep last 100 history entries
    state["history"].append({
        "ts": now.isoformat(),
        "n_logged": n_logged,
        "n_skipped": n_skipped,
        "n_errors": n_errors,
    })
    state["history"] = state["history"][-100:]
    save_state(state)

    print(f"[track] done — logged={n_logged} skipped={n_skipped} err={n_errors} "
          f"(total ever: {state['n_logs_total']})")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_logged": n_logged,
            "n_skipped": n_skipped,
            "n_errors": n_errors,
            "n_total_ever": state["n_logs_total"],
            "duration_s": round(time.time() - started, 1),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
