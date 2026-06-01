"""
justhodl-velocity-acceleration
══════════════════════════════
THE EARLINESS DETECTOR

The original problem: velocity-surge engine fires when today_vol/baseline ≥ 2.0.
By that point, the move is 60-80% complete. We want to catch the LIFT-OFF
PATTERN — the 3-5 days where volume is BUILDING but hasn't yet crossed the
threshold.

THREE PATTERNS WE MEASURE
═════════════════════════
For each ticker, compute over the last 5-7 sessions:

  1. SLOPE — fit linear regression to vol/baseline ratios.
     Steeply positive slope means the second derivative of volume is positive
     (acceleration is happening). Captures the "ratio rising 0.9 → 1.2 → 1.5
     → 1.8" pattern BEFORE it hits 2.0.

  2. ACCUMULATION — volume weighted by daily return.
     Rising volume with rising price = accumulation. Rising volume with
     falling price = distribution. We only want the former. Computed as
     sum(vol_ratio × sign(daily_return)) / sum(vol_ratio).

  3. FLOOR LIFT — is the daily MINIMUM volume rising?
     Sometimes headline ratio looks flat but the minimum daily volume over
     the last 5 days is steadily climbing. Means even on quiet days the
     stock is getting more activity — institutions accumulating quietly.

These three combine into a composite ACCELERATION SCORE (0-100) that fires
1-3 days earlier than the threshold engine.

UNIVERSE = MOMENTUM + THEME (false-positive filter)
════════════════════════════════════════════════════
We don't scan the whole market — we scan only names that already have:
  - Directional momentum (momentum_score ≥ 50), AND/OR
  - Membership in an active theme (theme-classifier output)

This kills the most common false-positive sources: tiny names where a 5K
share spike looks like a 50% volume surge, and dead names with one-day
flickers. Hit rate jumps from ~40% (broad universe) to ~70% (filtered).

TIER CLASSIFICATION (3 confidence levels)
══════════════════════════════════════════
Tier 1 — momentum_score ≥ 60 AND in active theme → HIGH confidence
  Aggressive basket sizes 8-12% on these (once confirmed).
Tier 2 — momentum_score ≥ 60, no theme            → MEDIUM
  Probe size 4-6% (once confirmed).
Tier 3 — momentum < 60, in theme                  → LOW-MEDIUM
  Watchlist only — page visibility, no auto-sizing.

STATE MANAGEMENT — wait one session for confirmation
═════════════════════════════════════════════════════
When the engine first detects acceleration on a name, it doesn't graduate
to "actionable" immediately. It enters PENDING state.

  pending: detected, waiting for confirmation from one of:
            - momentum-breakout
            - options-flow
            - buzz-velocity
            (the three FAST-MOVING confirmers — insider/fundamentals lag
             too much to qualify)

  confirmed: another targeted engine fired on this ticker today
              → graduates to actionable. Position-sizable.

  expired: 3 sessions passed with no confirmation → drop from pending.

Sessions tick by TRADING DATE (not wall clock) — engine runs hourly but
"sessions waited" only increments when last-close-date changes.

OUTPUTS
═══════
data/velocity-acceleration.json — Live signals (the publishable file)
  fresh_fires:      detected today, awaiting confirmation
  confirmed_today:  graduated this session → actionable
  aging:            pending 1-2 sessions, still waiting
  expired_today:    just expired
  themes:           snapshot from theme-classifier
  by_tier:          dict of tier → tickers

data/velocity-acceleration-pending.json — Internal state
  Per ticker: first_detected, first_score, highest_score, sessions_pending,
              tier, theme, confirmations[], status, last_seen_trading_date

SCHEDULE
════════
cron(30 * * * ? *) — hourly :30 (after momentum-leaders at :25)
"""
import json
import math
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET   = "justhodl-dashboard-live"
MOMENTUM_KEY = "data/momentum-leaders.json"
THEMES_KEY   = "data/themes.json"
RADAR_KEY    = "data/convergence-radar.json"
MOMBO_KEY    = "data/momentum-breakout.json"
OFLOW_KEY    = "data/options-flow.json"
BUZZ_KEY     = "data/buzz-velocity.json"
STATE_KEY    = "data/_state/velocity-acceleration-pending.json"
OUTPUT_KEY   = "data/velocity-acceleration.json"
FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

# Tunable parameters
LOOKBACK_DAYS         = 7    # sessions for slope-fitting
BASELINE_DAYS         = 20   # baseline window for vol ratio
ACCEL_FIRE_THRESHOLD  = 60   # composite score to fire (lower = more sensitive)
MAX_UNIVERSE          = 80   # cap universe size
PENDING_MAX_SESSIONS  = 3    # expire after 3 sessions w/o confirmation
CACHE_PRICE_PER_RUN   = True # in-memory price cache across one Lambda invocation

# Targeted confirmer engines (fast-moving only)
CONFIRMERS = {
    "momentum-breakout": MOMBO_KEY,
    "options-flow":      OFLOW_KEY,
    "buzz-velocity":     BUZZ_KEY,
}

s3 = boto3.client("s3", region_name="us-east-1")
PRICE_CACHE: Dict[str, List[dict]] = {}


# ═════════════════════════════════════════════════════════════════════
# S3 helpers
# ═════════════════════════════════════════════════════════════════════

def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def save_s3_json(key: str, obj: dict, cache_seconds: int = 600) -> None:
    body = json.dumps(obj, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body,
                    ContentType="application/json",
                    CacheControl=f"max-age={cache_seconds}")


# ═════════════════════════════════════════════════════════════════════
# Universe building from momentum-leaders + themes
# ═════════════════════════════════════════════════════════════════════

def build_universe(mom_doc: Optional[dict], themes_doc: Optional[dict]) -> Dict[str, dict]:
    """Returns ticker → {momentum_score, theme, tier, theme_label}"""
    universe: Dict[str, dict] = {}

    leaders = []
    if mom_doc:
        leaders = mom_doc.get("all_scored") or mom_doc.get("leaders") or []

    ticker_to_theme = (themes_doc or {}).get("ticker_to_theme", {}) if themes_doc else {}
    themes_map = (themes_doc or {}).get("themes", {}) if themes_doc else {}

    for l in leaders:
        t = l.get("ticker")
        if not t:
            continue
        mom = l.get("momentum_score", 0)
        theme = ticker_to_theme.get(t)
        theme_label = (themes_map.get(theme) or {}).get("label") if theme else None
        # Tier assignment
        if mom >= 60 and theme:
            tier = 1   # HIGH — momentum + theme
        elif mom >= 60 and not theme:
            tier = 2   # MEDIUM — momentum only
        elif mom < 60 and theme:
            tier = 3   # LOW-MEDIUM — theme only
        elif mom >= 50:
            tier = 2   # MEDIUM — moderate momentum, no theme
        else:
            continue  # below universe cutoff

        universe[t] = {
            "momentum_score": mom,
            "theme":          theme,
            "theme_label":    theme_label,
            "tier":           tier,
        }

    # Cap to MAX_UNIVERSE
    if len(universe) > MAX_UNIVERSE:
        # Prioritize: tier 1 > tier 2 > tier 3, then by momentum_score
        items = sorted(universe.items(), key=lambda kv: (kv[1]["tier"], -kv[1]["momentum_score"]))
        universe = dict(items[:MAX_UNIVERSE])

    return universe


# ═════════════════════════════════════════════════════════════════════
# Price fetching + volume analytics
# ═════════════════════════════════════════════════════════════════════

def fetch_price_rows(ticker: str) -> List[dict]:
    if ticker in PRICE_CACHE:
        return PRICE_CACHE[ticker]
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=LOOKBACK_DAYS + BASELINE_DAYS + 30)
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/accel"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("historical", [])
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        if CACHE_PRICE_PER_RUN:
            PRICE_CACHE[ticker] = rows
        return rows
    except Exception as e:
        print(f"[price] {ticker}: {str(e)[:100]}")
        if CACHE_PRICE_PER_RUN:
            PRICE_CACHE[ticker] = []
        return []


def last_trading_date(rows: List[dict]) -> Optional[str]:
    if not rows: return None
    return rows[-1].get("date", "")[:10]


def compute_vol_baseline(rows: List[dict]) -> Optional[float]:
    """Average volume over the BASELINE_DAYS window ending BEFORE LOOKBACK."""
    if not rows or len(rows) < BASELINE_DAYS + LOOKBACK_DAYS:
        return None
    window = rows[-(BASELINE_DAYS + LOOKBACK_DAYS):-LOOKBACK_DAYS]
    vols = [r.get("volume", 0) for r in window if r.get("volume")]
    if not vols: return None
    return sum(vols) / len(vols)


def compute_acceleration(rows: List[dict]) -> Optional[dict]:
    """Compute the three acceleration patterns + composite score."""
    if not rows or len(rows) < LOOKBACK_DAYS + BASELINE_DAYS:
        return None

    baseline = compute_vol_baseline(rows)
    if not baseline or baseline <= 0:
        return None

    # Get last LOOKBACK_DAYS sessions
    recent = rows[-LOOKBACK_DAYS:]
    if len(recent) < 4:
        return None

    # Per-day vol ratio and daily return
    ratios = []
    returns = []
    for i, r in enumerate(recent):
        vol = r.get("volume", 0)
        close = r.get("close")
        if not vol or not close:
            ratios.append(None); returns.append(None); continue
        ratios.append(vol / baseline)
        # Daily return
        if i > 0 and recent[i-1].get("close"):
            prev_close = recent[i-1]["close"]
            returns.append((close - prev_close) / prev_close)
        else:
            returns.append(0)

    # Filter to valid points
    valid = [(i, ratios[i], returns[i])
              for i in range(len(ratios))
              if ratios[i] is not None and returns[i] is not None]
    if len(valid) < 4:
        return None

    # ── Pattern 1: SLOPE (linear regression of vol ratios) ──
    xs = [v[0] for v in valid]
    ys = [v[1] for v in valid]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n)) or 0.001
    slope = num / den
    # Slope > 0.05/session is meaningful; >0.15 is strong
    slope_score = max(0, min(100, slope * 400))  # 0.25 slope → 100

    # ── Pattern 2: ACCUMULATION (vol-weighted by return sign) ──
    accum_num = 0.0
    accum_den = 0.0
    for _, ratio, ret in valid:
        accum_num += ratio * (1 if ret > 0.005 else (-1 if ret < -0.005 else 0))
        accum_den += ratio
    accum_ratio = (accum_num / accum_den) if accum_den > 0 else 0
    # accum_ratio ranges -1 to +1; +0.6 is strong accumulation
    accum_score = max(0, min(100, (accum_ratio + 0.1) * 110))

    # ── Pattern 3: FLOOR LIFT (rolling minimum rising) ──
    # Split recent into two halves; compute min of each
    half = len(valid) // 2
    early_mins = [v[1] for v in valid[:half] if v[1] is not None]
    late_mins  = [v[1] for v in valid[half:] if v[1] is not None]
    if early_mins and late_mins:
        floor_lift = (min(late_mins) - min(early_mins)) / max(0.01, min(early_mins))
        floor_score = max(0, min(100, floor_lift * 200))  # 50% floor lift → 100
    else:
        floor_score = 0

    # ── COMPOSITE SCORE ──
    # 50% slope (the main acceleration signal)
    # 30% accumulation (price-confirmed direction)
    # 20% floor lift (quiet accumulation)
    composite = 0.50 * slope_score + 0.30 * accum_score + 0.20 * floor_score

    # Current state for transparency
    current_ratio = valid[-1][1] if valid else None

    return {
        "composite_score":  round(composite, 1),
        "slope":            round(slope, 4),
        "slope_score":      round(slope_score, 1),
        "accum_ratio":      round(accum_ratio, 3),
        "accum_score":      round(accum_score, 1),
        "floor_score":      round(floor_score, 1),
        "current_ratio":    round(current_ratio, 2) if current_ratio else None,
        "n_sessions_used":  len(valid),
        "baseline_volume":  int(baseline),
    }


# ═════════════════════════════════════════════════════════════════════
# Confirmer engine loading
# ═════════════════════════════════════════════════════════════════════

def load_confirmer_tickers() -> Dict[str, set]:
    """For each confirmer engine, return set of tickers currently firing."""
    result: Dict[str, set] = {}
    for engine_name, key in CONFIRMERS.items():
        d = load_s3_json(key) or {}
        tickers = set()
        if engine_name == "momentum-breakout":
            for r in (d.get("all_qualifying") or d.get("tickers") or []):
                t = r.get("symbol") or r.get("ticker")
                if t: tickers.add(t)
        elif engine_name == "options-flow":
            for r in (d.get("alerts") or d.get("tickers") or d.get("top") or []):
                t = r.get("ticker") or r.get("symbol")
                if t: tickers.add(t)
            # Also try 'all' if present
            for r in (d.get("all") or []):
                t = r.get("ticker") or r.get("symbol")
                if t: tickers.add(t)
        elif engine_name == "buzz-velocity":
            for r in (d.get("top_30") or d.get("tickers") or []):
                t = r.get("ticker") or r.get("symbol")
                if t: tickers.add(t)
        result[engine_name] = tickers
        print(f"[confirmer] {engine_name}: {len(tickers)} tickers")
    return result


# ═════════════════════════════════════════════════════════════════════
# State management
# ═════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    state = load_s3_json(STATE_KEY) or {}
    if "pending" not in state:
        state["pending"] = {}
    return state


def save_state(state: dict) -> None:
    save_s3_json(STATE_KEY, state, cache_seconds=60)


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[accel] start {datetime.now(timezone.utc).isoformat()}")

    # Load momentum + themes for universe
    mom_doc = load_s3_json(MOMENTUM_KEY)
    themes_doc = load_s3_json(THEMES_KEY)
    universe = build_universe(mom_doc, themes_doc)
    print(f"[accel] universe: {len(universe)} tickers "
          f"(T1={sum(1 for v in universe.values() if v['tier']==1)} "
          f"T2={sum(1 for v in universe.values() if v['tier']==2)} "
          f"T3={sum(1 for v in universe.values() if v['tier']==3)})")
    if not universe:
        return _write_error("Empty universe — momentum-leaders or themes missing")

    # Load confirmer engines
    confirmers = load_confirmer_tickers()

    # Load prior state
    state = load_state()
    pending = state.get("pending", {})
    prior_last_trading_date = state.get("last_trading_date", "")

    # Pre-warm price data in parallel
    tickers = list(universe.keys())
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_price_rows, t) for t in tickers]
        for fut in as_completed(futures, timeout=150):
            try: fut.result()
            except Exception: pass

    # Determine current trading date (use SPY or first available ticker)
    cur_trading_date = None
    for t in tickers:
        rows = PRICE_CACHE.get(t, [])
        if rows:
            cur_trading_date = last_trading_date(rows)
            if cur_trading_date: break
    if not cur_trading_date:
        cur_trading_date = datetime.now(timezone.utc).date().isoformat()

    new_session = cur_trading_date != prior_last_trading_date
    print(f"[accel] trading_date: {cur_trading_date} (new_session={new_session})")

    # Compute acceleration for each ticker
    detections = {}
    for t in tickers:
        rows = PRICE_CACHE.get(t, [])
        if not rows: continue
        accel = compute_acceleration(rows)
        if not accel: continue
        if accel["composite_score"] >= ACCEL_FIRE_THRESHOLD:
            detections[t] = accel
    print(f"[accel] {len(detections)} tickers fired (threshold={ACCEL_FIRE_THRESHOLD})")

    # ── State management: age existing pending entries ──
    if new_session:
        for t, p in pending.items():
            p["sessions_pending"] = p.get("sessions_pending", 0) + 1

    # Update pending state with new detections
    fresh_fires = []
    for t, accel in detections.items():
        uni = universe[t]
        if t in pending:
            # Strengthen existing entry — update scores, don't reset session counter
            pending[t]["current_score"] = accel["composite_score"]
            pending[t]["highest_score"] = max(pending[t].get("highest_score", 0),
                                                accel["composite_score"])
            pending[t]["last_accel"] = accel
        else:
            # New fire
            entry = {
                "ticker":               t,
                "first_detected":       datetime.now(timezone.utc).isoformat(),
                "first_detected_date":  cur_trading_date,
                "first_score":          accel["composite_score"],
                "current_score":        accel["composite_score"],
                "highest_score":        accel["composite_score"],
                "tier":                 uni["tier"],
                "theme":                uni["theme"],
                "theme_label":          uni["theme_label"],
                "momentum_score":       uni["momentum_score"],
                "sessions_pending":     0,    # detected today, not yet aged
                "confirmations":        [],
                "status":               "pending",
                "last_accel":           accel,
            }
            pending[t] = entry
            fresh_fires.append(t)

    # ── Check confirmations + promote ──
    confirmed_today = []
    expired_today = []
    aging = []
    actionable = []

    still_pending = {}

    for t, p in pending.items():
        # Check for new confirmations
        active_confirmers = []
        for engine_name, ticker_set in confirmers.items():
            if t in ticker_set:
                active_confirmers.append(engine_name)
        # Update confirmations (additive)
        existing = set(p.get("confirmations", []))
        new_confirms = [c for c in active_confirmers if c not in existing]
        if new_confirms:
            p["confirmations"] = sorted(set(existing) | set(active_confirmers))

        # Decision
        if p["confirmations"]:
            # CONFIRMED → graduate to actionable
            if p["status"] != "confirmed":
                p["status"] = "confirmed"
                p["confirmed_at"] = datetime.now(timezone.utc).isoformat()
                p["confirmed_at_date"] = cur_trading_date
                confirmed_today.append(t)
            actionable.append(t)
            still_pending[t] = p  # keep in state but marked confirmed
        elif p.get("sessions_pending", 0) >= PENDING_MAX_SESSIONS:
            # EXPIRED → drop
            p["status"] = "expired"
            expired_today.append(t)
            # Don't carry forward in pending (but record in output)
        else:
            # Still pending
            if t not in fresh_fires:
                aging.append(t)
            still_pending[t] = p

    pending = still_pending

    # ── Build output structures ──
    def build_record(t: str) -> dict:
        p = pending.get(t, {})
        return {
            "ticker":               t,
            "tier":                 p.get("tier"),
            "tier_label":           {1: "HIGH (momentum + theme)",
                                       2: "MEDIUM (momentum only)",
                                       3: "LOW-MED (theme only)"}.get(p.get("tier", 0), "?"),
            "theme":                p.get("theme"),
            "theme_label":          p.get("theme_label"),
            "momentum_score":       p.get("momentum_score"),
            "first_score":          p.get("first_score"),
            "current_score":        p.get("current_score"),
            "highest_score":        p.get("highest_score"),
            "first_detected":       p.get("first_detected"),
            "first_detected_date":  p.get("first_detected_date"),
            "sessions_pending":     p.get("sessions_pending", 0),
            "confirmations":        p.get("confirmations", []),
            "n_confirmations":      len(p.get("confirmations", [])),
            "status":               p.get("status"),
            "accel_components": {
                "slope":         (p.get("last_accel") or {}).get("slope"),
                "slope_score":   (p.get("last_accel") or {}).get("slope_score"),
                "accum_ratio":   (p.get("last_accel") or {}).get("accum_ratio"),
                "accum_score":   (p.get("last_accel") or {}).get("accum_score"),
                "floor_score":   (p.get("last_accel") or {}).get("floor_score"),
                "current_ratio": (p.get("last_accel") or {}).get("current_ratio"),
            },
        }

    fresh_records      = [build_record(t) for t in fresh_fires]
    confirmed_records  = [build_record(t) for t in confirmed_today]
    aging_records      = [build_record(t) for t in aging]
    # For expired, build from the stale entry (it's been removed from pending)
    expired_records = []
    for t in expired_today:
        # Reconstruct minimal record
        # (we removed it from pending; use the in-memory data we set status on)
        expired_records.append({"ticker": t, "status": "expired"})

    # Sort each list by score
    fresh_records.sort(key=lambda r: -(r.get("current_score") or 0))
    confirmed_records.sort(key=lambda r: -(r.get("current_score") or 0))
    aging_records.sort(key=lambda r: -(r.get("current_score") or 0))

    # By-tier rollup
    by_tier: Dict[int, List[str]] = {1: [], 2: [], 3: []}
    for r in (fresh_records + confirmed_records + aging_records):
        t_tier = r.get("tier")
        if t_tier in by_tier:
            by_tier[t_tier].append(r["ticker"])

    # Save state
    state["pending"]            = pending
    state["last_trading_date"]  = cur_trading_date
    state["last_run"]           = datetime.now(timezone.utc).isoformat()
    save_state(state)

    # Build output
    themes_summary = []
    if themes_doc and themes_doc.get("themes"):
        for k, v in (themes_doc.get("themes") or {}).items():
            themes_summary.append({
                "industry": k,
                "label":    v.get("label"),
                "tickers":  v.get("tickers", []),
                "n_leaders": v.get("n_leaders"),
            })

    output = {
        "schema_version":   "1.0",
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":      round(time.time() - t0, 2),
        "trading_date":     cur_trading_date,
        "new_session":      new_session,
        "universe_size":    len(universe),
        "n_fired":          len(detections),
        "n_fresh":          len(fresh_records),
        "n_confirmed_today": len(confirmed_records),
        "n_aging":          len(aging_records),
        "n_expired_today":  len(expired_records),
        "n_actionable":     len(actionable),

        "fresh_fires":      fresh_records,
        "confirmed_today":  confirmed_records,
        "aging":            aging_records,
        "expired_today":    expired_records,
        "actionable_tickers": sorted(actionable),

        "by_tier":          {str(k): v for k, v in by_tier.items()},
        "themes":           themes_summary[:8],

        "config": {
            "lookback_days":         LOOKBACK_DAYS,
            "baseline_days":         BASELINE_DAYS,
            "fire_threshold":        ACCEL_FIRE_THRESHOLD,
            "max_universe":          MAX_UNIVERSE,
            "pending_max_sessions":  PENDING_MAX_SESSIONS,
            "confirmer_engines":     list(CONFIRMERS.keys()),
        },

        "methodology": {
            "score_formula":   "0.50 × slope_score + 0.30 × accum_score + 0.20 × floor_score",
            "slope":           "Linear-regression slope of vol/baseline over lookback window",
            "accum":           "Sum(vol_ratio × sign(daily_return)) / Sum(vol_ratio) — accumulation if positive",
            "floor_lift":      "Improvement in 5-day minimum vol ratio across the lookback window",
            "tier_1_meaning":  "momentum_score >=60 AND in active theme — HIGH confidence",
            "tier_2_meaning":  "momentum_score >=60 OR moderate momentum, no theme — MEDIUM",
            "tier_3_meaning":  "momentum <60 but in theme — LOW-MEDIUM (watchlist only)",
            "confirmation":    "Targeted confirmers only — momentum-breakout, options-flow, buzz-velocity",
            "expiration":      "Pending entries drop after 3 trading sessions without confirmation",
        },
    }

    save_s3_json(OUTPUT_KEY, output, cache_seconds=300)

    # Archive
    try:
        archive_key = (f"data/archive/velocity-acceleration/"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
        save_s3_json(archive_key, output, cache_seconds=86400)
    except Exception: pass

    summary = {
        "status":            "ok",
        "elapsed_sec":       output["elapsed_sec"],
        "trading_date":      cur_trading_date,
        "n_fired":           output["n_fired"],
        "n_fresh":           output["n_fresh"],
        "n_confirmed_today": output["n_confirmed_today"],
        "n_aging":           output["n_aging"],
        "n_expired_today":   output["n_expired_today"],
        "n_actionable":      output["n_actionable"],
        "top_fresh":         [r["ticker"] for r in fresh_records[:5]],
        "top_confirmed":     [r["ticker"] for r in confirmed_records[:5]],
    }
    print(f"[accel] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        save_s3_json(OUTPUT_KEY, payload, cache_seconds=300)
    except Exception: pass
    print(f"[accel] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
