"""
justhodl-radar-backtest
═══════════════════════
Reads the convergence-radar S3 archive (data/archive/convergence-radar/)
for the last 30 days. For each archived snapshot, extracts which tickers
were at which tier (ULTRA/HIGH/MED/LOW). Looks up forward returns 1d/5d/20d
via FMP. Aggregates:
  - Hit rate (% positive return) per tier
  - Average return per tier
  - Sharpe ratio per tier
  - Best/worst single trades
  - Cumulative hypothetical equity curve

OUTPUT
══════
data/radar-backtest.json
{
  "schema_version":     "1.0",
  "generated_at":       "...",
  "lookback_days":      30,
  "n_snapshots":        45,
  "n_unique_signals":   324,  # ticker-tier-date combinations
  "per_tier_stats": {
    "ULTRA": {"n": 87, "hit_rate_5d": 0.62, "avg_return_5d": 4.2, "sharpe": 1.8, ...},
    "HIGH":  {"n": 124, "hit_rate_5d": 0.58, "avg_return_5d": 2.9, ...},
    "MED":   {"n": 153, ...},
    "LOW":   {"n": 89, ...}
  },
  "transition_signals": {
    "NEW_HIGH":     {"n": 42, "avg_return_5d": 5.8, ...},
    "ACCELERATING": {"n": 31, "avg_return_5d": 6.4, ...},
    "ULTRA_NEW":    {"n": 12, "avg_return_5d": 8.1, ...}
  },
  "best_signals":     [{"ticker": "X", "date": "...", "tier": "ULTRA", "return_5d": 28.4}],
  "worst_signals":    [...],
  "signal_decay":     {"return_1d": x, "return_5d": y, "return_20d": z}  # signal-to-noise over time
}

SCHEDULE
════════
cron(0 6 * * ? *) — daily at 06:00 UTC (post-market, before next session)
"""
import json
import math
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET    = "justhodl-dashboard-live"
ARCHIVE_PREFIX = "data/archive/convergence-radar/"
OUTPUT_KEY   = "data/radar-backtest.json"
FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

LOOKBACK_DAYS = 30
PRICE_CACHE: Dict[str, List[dict]] = {}  # ticker -> price rows

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Archive listing + reading
# ═════════════════════════════════════════════════════════════════════

def list_archive_snapshots(lookback_days: int) -> List[dict]:
    """Get the list of archived radar snapshot keys."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    paginator = s3.get_paginator("list_objects_v2")
    out = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=ARCHIVE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                continue
            out.append({"key": obj["Key"], "last_modified": obj["LastModified"]})
    out.sort(key=lambda x: x["last_modified"])
    return out


def load_snapshot(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[archive] {key}: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════
# Extract signals from a snapshot — only one per (ticker, day, tier)
# ═════════════════════════════════════════════════════════════════════

def extract_signals(snap: dict, snap_date: str) -> List[dict]:
    """Extract (ticker, tier, transition_type, snap_date) tuples from one snapshot."""
    out = []
    tickers = snap.get("tickers", []) or []
    for r in tickers:
        out.append({
            "ticker":          r["ticker"],
            "tier":            r.get("tier", "MED"),
            "n_engines":       r.get("n_engines"),
            "convergence":     r.get("convergence_score"),
            "directional":     r.get("directional_score"),
            "pump_likelihood": r.get("pump_likelihood"),
            "pump_category":   r.get("pump_category"),
            "is_new_high":     r.get("is_new_high"),
            "is_accelerating": r.get("is_accelerating"),
            "is_ultra_new":    r.get("is_ultra_new"),
            "snap_date":       snap_date,
        })
    return out


# ═════════════════════════════════════════════════════════════════════
# Price history fetcher (cached per Lambda run)
# ═════════════════════════════════════════════════════════════════════

def fetch_price_history(ticker: str) -> List[dict]:
    if ticker in PRICE_CACHE:
        return PRICE_CACHE[ticker]
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=LOOKBACK_DAYS + 45)  # cushion for 20d forward + recent
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/backtest"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("historical", [])
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        PRICE_CACHE[ticker] = rows
        return rows
    except Exception as e:
        print(f"[price] {ticker}: {str(e)[:120]}")
        PRICE_CACHE[ticker] = []
        return []


def get_close(rows: List[dict], date_iso: str) -> Optional[float]:
    """Find the first close on or after date_iso."""
    for r in rows:
        if r.get("date", "")[:10] >= date_iso:
            return r.get("close")
    return None


def get_close_offset(rows: List[dict], base_date_iso: str, offset_days: int) -> Optional[float]:
    """Find close N trading days after base_date_iso (approximate calendar offset)."""
    # Find index of base
    base_idx = None
    for i, r in enumerate(rows):
        if r.get("date", "")[:10] >= base_date_iso:
            base_idx = i
            break
    if base_idx is None:
        return None
    target = base_idx + offset_days
    if target >= len(rows):
        return None
    return rows[target].get("close")


# ═════════════════════════════════════════════════════════════════════
# Forward return calculation
# ═════════════════════════════════════════════════════════════════════

def compute_forward_returns(ticker: str, snap_date_iso: str) -> dict:
    """For a ticker signaled on snap_date, compute 1d/5d/20d forward returns."""
    rows = fetch_price_history(ticker)
    if not rows:
        return {}

    # Entry: close on day after snap_date (we'd buy at next open ideally)
    entry = get_close_offset(rows, snap_date_iso, 1)
    if entry is None:
        return {}
    fwd_1d  = get_close_offset(rows, snap_date_iso, 2)
    fwd_5d  = get_close_offset(rows, snap_date_iso, 6)
    fwd_20d = get_close_offset(rows, snap_date_iso, 21)

    out = {"entry_price": round(entry, 2)}
    if fwd_1d is not None and entry > 0:
        out["return_1d_pct"] = round((fwd_1d / entry - 1) * 100, 2)
    if fwd_5d is not None and entry > 0:
        out["return_5d_pct"] = round((fwd_5d / entry - 1) * 100, 2)
    if fwd_20d is not None and entry > 0:
        out["return_20d_pct"] = round((fwd_20d / entry - 1) * 100, 2)
    return out


# ═════════════════════════════════════════════════════════════════════
# Aggregation: per-tier and per-transition stats
# ═════════════════════════════════════════════════════════════════════

def compute_tier_stats(records: List[dict], horizon_key: str = "return_5d_pct") -> dict:
    """Aggregate stats per tier."""
    by_tier: Dict[str, List[float]] = {}
    for r in records:
        tier = r.get("tier", "?")
        ret = r.get(horizon_key)
        if ret is None:
            continue
        by_tier.setdefault(tier, []).append(ret)

    stats = {}
    for tier, returns in by_tier.items():
        if not returns:
            continue
        n = len(returns)
        wins = sum(1 for r in returns if r > 0)
        avg = sum(returns) / n
        std = math.sqrt(sum((r - avg) ** 2 for r in returns) / max(1, n - 1)) if n > 1 else 0
        sharpe = (avg / std) * math.sqrt(252 / 5) if std > 0 else 0  # 5-day Sharpe, annualized
        stats[tier] = {
            "n":            n,
            "hit_rate":     round(wins / n, 3),
            "avg_return":   round(avg, 2),
            "median":       round(sorted(returns)[n // 2], 2),
            "std":          round(std, 2),
            "sharpe":       round(sharpe, 2),
            "best":         round(max(returns), 2),
            "worst":        round(min(returns), 2),
        }
    return stats


def compute_transition_stats(records: List[dict]) -> dict:
    """Stats specifically for transition signals (NEW_HIGH, ACCELERATING, ULTRA_NEW)."""
    transitions = {
        "NEW_HIGH":     [r for r in records if r.get("is_new_high")],
        "ACCELERATING": [r for r in records if r.get("is_accelerating")],
        "ULTRA_NEW":    [r for r in records if r.get("is_ultra_new")],
    }
    out = {}
    for trans_name, recs in transitions.items():
        for horizon in ("return_1d_pct", "return_5d_pct", "return_20d_pct"):
            returns = [r.get(horizon) for r in recs if r.get(horizon) is not None]
            if not returns:
                continue
            n = len(returns)
            wins = sum(1 for r in returns if r > 0)
            avg = sum(returns) / n
            out.setdefault(trans_name, {})[horizon] = {
                "n":          n,
                "hit_rate":   round(wins / n, 3),
                "avg_return": round(avg, 2),
                "best":       round(max(returns), 2),
            }
    return out


def compute_pump_category_stats(records: List[dict]) -> dict:
    """Stats per pump_category (PUMP_PRIMED, PUMP_LIKELY, etc.)."""
    by_cat: Dict[str, List[float]] = {}
    for r in records:
        cat = r.get("pump_category")
        ret = r.get("return_5d_pct")
        if not cat or ret is None:
            continue
        by_cat.setdefault(cat, []).append(ret)

    out = {}
    for cat, returns in by_cat.items():
        if not returns:
            continue
        n = len(returns)
        wins = sum(1 for r in returns if r > 0)
        avg = sum(returns) / n
        out[cat] = {
            "n":          n,
            "hit_rate":   round(wins / n, 3),
            "avg_return": round(avg, 2),
            "best":       round(max(returns), 2),
            "worst":      round(min(returns), 2),
        }
    return out


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[backtest] start {datetime.now(timezone.utc).isoformat()}")

    # 1. List archive snapshots
    snapshots = list_archive_snapshots(LOOKBACK_DAYS)
    print(f"[backtest] {len(snapshots)} snapshots in last {LOOKBACK_DAYS}d")
    if not snapshots:
        return _write_error("No archived snapshots")

    # 2. Sample snapshots — use daily snapshot at noon UTC (to avoid intraday noise)
    # Group by date, take first snapshot of each day
    by_date: Dict[str, dict] = {}
    for snap in snapshots:
        d_iso = snap["last_modified"].date().isoformat()
        if d_iso not in by_date:
            by_date[d_iso] = snap
    daily_snapshots = list(by_date.values())
    print(f"[backtest] using {len(daily_snapshots)} daily snapshots")

    # 3. Load + extract signals
    all_signals: List[dict] = []
    for snap_meta in daily_snapshots:
        snap = load_snapshot(snap_meta["key"])
        if not snap:
            continue
        snap_date = snap_meta["last_modified"].date().isoformat()
        all_signals.extend(extract_signals(snap, snap_date))

    print(f"[backtest] {len(all_signals)} raw signal records")

    # Dedupe — keep first occurrence of (ticker, snap_date)
    seen = set()
    unique: List[dict] = []
    for s in all_signals:
        key = (s["ticker"], s["snap_date"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)

    print(f"[backtest] {len(unique)} unique (ticker, date) signals")

    # 4. For each signal, fetch forward returns (parallel by ticker batches)
    # Cap to top-tier signals to keep API calls reasonable
    high_value = [s for s in unique if s["tier"] in ("ULTRA", "HIGH", "MED")]
    print(f"[backtest] computing forward returns for {len(high_value)} signals")

    # Group by ticker so we only fetch each ticker's history once
    by_ticker: Dict[str, List[dict]] = {}
    for s in high_value:
        by_ticker.setdefault(s["ticker"], []).append(s)

    enriched: List[dict] = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        # Pre-warm price cache in parallel
        futures = {ex.submit(fetch_price_history, t): t for t in by_ticker}
        for fut in as_completed(futures, timeout=60):
            t = futures[fut]
            try:
                fut.result()
            except Exception:
                pass

    # Now compute forward returns for each signal (cache populated)
    for t, recs in by_ticker.items():
        for s in recs:
            fwd = compute_forward_returns(t, s["snap_date"])
            if fwd:
                enriched.append({**s, **fwd})

    print(f"[backtest] {len(enriched)} signals with forward returns")

    # 5. Compute aggregate stats
    tier_stats_5d = compute_tier_stats(enriched, "return_5d_pct")
    tier_stats_1d = compute_tier_stats(enriched, "return_1d_pct")
    tier_stats_20d = compute_tier_stats(enriched, "return_20d_pct")
    transition_stats = compute_transition_stats(enriched)
    pump_cat_stats = compute_pump_category_stats(enriched)

    # Best/worst individual trades
    valid_5d = [r for r in enriched if r.get("return_5d_pct") is not None]
    valid_5d.sort(key=lambda r: -r["return_5d_pct"])
    best_signals = [
        {"ticker": r["ticker"], "snap_date": r["snap_date"], "tier": r["tier"],
         "n_engines": r["n_engines"], "pump_category": r.get("pump_category"),
         "return_5d_pct": r["return_5d_pct"], "return_1d_pct": r.get("return_1d_pct"),
         "return_20d_pct": r.get("return_20d_pct")}
        for r in valid_5d[:15]
    ]
    worst_signals = [
        {"ticker": r["ticker"], "snap_date": r["snap_date"], "tier": r["tier"],
         "n_engines": r["n_engines"], "pump_category": r.get("pump_category"),
         "return_5d_pct": r["return_5d_pct"]}
        for r in valid_5d[-10:]
    ]

    # Overall stats
    all_5d_rets = [r["return_5d_pct"] for r in enriched if r.get("return_5d_pct") is not None]
    overall_hit_rate = sum(1 for r in all_5d_rets if r > 0) / len(all_5d_rets) if all_5d_rets else 0
    overall_avg = sum(all_5d_rets) / len(all_5d_rets) if all_5d_rets else 0

    # Build output
    output = {
        "schema_version":   "1.0",
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":      round(time.time() - t0, 2),
        "lookback_days":    LOOKBACK_DAYS,
        "n_snapshots":      len(daily_snapshots),
        "n_unique_signals": len(unique),
        "n_with_returns":   len(enriched),
        "n_unique_tickers": len(by_ticker),
        "overall_5d": {
            "hit_rate":   round(overall_hit_rate, 3),
            "avg_return": round(overall_avg, 2),
        },
        "per_tier_5d":      tier_stats_5d,
        "per_tier_1d":      tier_stats_1d,
        "per_tier_20d":     tier_stats_20d,
        "transitions":      transition_stats,
        "pump_categories":  pump_cat_stats,
        "best_signals":     best_signals,
        "worst_signals":    worst_signals,
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=3600",
    )

    summary = {
        "status":         "ok",
        "elapsed_sec":    output["elapsed_sec"],
        "n_snapshots":    output["n_snapshots"],
        "n_signals":      output["n_with_returns"],
        "overall_hit_5d": output["overall_5d"]["hit_rate"],
        "overall_avg_5d": output["overall_5d"]["avg_return"],
    }
    print(f"[backtest] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "status":         "error",
        "error":          message,
        **extras,
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception:
        pass
    print(f"[backtest] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
