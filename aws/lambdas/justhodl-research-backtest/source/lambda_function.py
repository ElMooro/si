"""justhodl-research-backtest

Honest track record of equity-research verdicts. NOT a self-congratulatory
performance dashboard — an institutional attribution report that:

  1. Reads every research file in S3 with its generated_at timestamp
  2. Treats generated_at as the "as-of" date and the research's quote.price
     as the entry price (no look-ahead — only data the analyst could see)
  3. Fetches current spot prices via FMP /stable/quote
  4. Computes per-call: absolute return, SPY-relative alpha, days_held
  5. Aggregates by rating: win rate, mean return, mean alpha, std dev, N
  6. THE KEY DIFFERENTIATOR: ALSO computes whether the critique signal
     predicted anything. Did contested tickers (high disagreement_score)
     systematically underperform consensus picks? That validates the
     whole ensemble premise.

OUTPUT: analytics/backtest_results.json with:
  - per_call: array of every research outcome
  - rating_summary: aggregates by analyst rating
  - critique_summary: aggregates by critic alternative_rating
  - ensemble_attribution: high-conviction (concur) vs contested (diverge)
    outcomes — does the ensemble signal predict anything?
  - benchmark: SPY return over the same windows
  - caveats: small N, short timeframe, etc.

Scheduled daily 11:00 UTC (06:00 ET) — after market overnight stabilization.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
RESEARCH_PREFIX = "equity-research/"
HISTORY_PREFIX = "equity-research-history/"
CRITIQUE_PREFIX = "equity-critique/"
OUTPUT_KEY = "analytics/backtest_results.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════
def http_get_json(url: str, timeout: int = 15) -> Optional[list]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Backtest/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[fmp] {url[:80]}...: {e}")
        return None


def get_current_price(ticker: str) -> Optional[float]:
    """FMP /stable/quote/{TICKER} — current spot price."""
    if not FMP_KEY:
        return None
    data = http_get_json(f"{FMP_BASE}/quote?symbol={ticker}&apikey={FMP_KEY}")
    if isinstance(data, list) and data:
        return float(data[0].get("price", 0)) or None
    return None


def days_between(iso_a: str, iso_b: str) -> int:
    """Days between two ISO datetimes."""
    try:
        a = datetime.fromisoformat(iso_a.replace("Z", "+00:00"))
        b = datetime.fromisoformat(iso_b.replace("Z", "+00:00"))
        return abs((b - a).days)
    except Exception:
        return 0


def pct_change(start: float, end: float) -> Optional[float]:
    """% change from start to end."""
    if not start or start <= 0 or not end:
        return None
    return round((end / start - 1) * 100, 2)


# ═════════════════════════════════════════════════════════════════════
# Read all research + critique files
# ═════════════════════════════════════════════════════════════════════
def list_keys_under(prefix: str) -> list:
    """List all .json keys under prefix."""
    keys = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            k = obj["Key"]
            if k.endswith(".json") and not k.endswith("manifest.json"):
                keys.append(k)
    return keys


def read_s3_json(key: str) -> Optional[dict]:
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════
# Core backtest
# ═════════════════════════════════════════════════════════════════════
def list_history_for_ticker(ticker: str) -> list:
    """List all historical snapshots for a ticker, sorted by date ascending.

    Returns: [(date_str, key), ...] from oldest to newest. Empty if no history.
    """
    snapshots = []
    pag = s3.get_paginator("list_objects_v2")
    # equity-research-history/YYYY-MM-DD/{TICKER}.json
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=HISTORY_PREFIX):
        for obj in (page.get("Contents") or []):
            key = obj["Key"]
            # Parse date and ticker from path
            parts = key[len(HISTORY_PREFIX):].split("/")
            if len(parts) == 2 and parts[1] == f"{ticker}.json":
                snapshots.append((parts[0], key))
    snapshots.sort(key=lambda x: x[0])
    return snapshots


def build_per_call_attribution(now_prices: dict, spy_now: Optional[float],
                                spy_then_cache: dict) -> list:
    """For each research file, compute return + alpha attribution.

    Returns list of dicts, one per (ticker, generated_at) pair.

    Strategy: use the OLDEST historical snapshot as the "entry" point so
    days_held > 0 and we can measure real performance. If no history exists,
    fall back to the latest research (days_held=0, return=0 — sample
    needs to mature).
    """
    per_call = []
    research_keys = list_keys_under(RESEARCH_PREFIX)
    print(f"[backtest] found {len(research_keys)} current research files")

    # Build critique lookup
    critique_lookup = {}
    for ck in list_keys_under(CRITIQUE_PREFIX):
        cd = read_s3_json(ck)
        if cd and cd.get("ticker"):
            critique_lookup[cd["ticker"]] = cd

    for key in research_keys:
        latest_doc = read_s3_json(key)
        if not latest_doc:
            continue
        ticker = latest_doc.get("ticker")
        if not ticker:
            continue

        # Try to find oldest historical snapshot (true entry point)
        history = list_history_for_ticker(ticker)
        if history:
            oldest_date, oldest_key = history[0]
            entry_doc = read_s3_json(oldest_key)
            if entry_doc:
                gen_at = entry_doc.get("generated_at") or f"{oldest_date}T00:00:00+00:00"
                entry_price = (entry_doc.get("quote") or {}).get("price")
                # Verdict from oldest (the original call to evaluate)
                verdict = entry_doc.get("verdict") or {}
            else:
                gen_at = latest_doc.get("generated_at")
                entry_price = (latest_doc.get("quote") or {}).get("price")
                verdict = latest_doc.get("verdict") or {}
        else:
            # No history — use latest (will show 0% return)
            gen_at = latest_doc.get("generated_at")
            entry_price = (latest_doc.get("quote") or {}).get("price")
            verdict = latest_doc.get("verdict") or {}

        if not entry_price or not gen_at:
            continue

        rating = verdict.get("rating")
        pt = verdict.get("price_target_12m")

        current_price = now_prices.get(ticker)
        if not current_price:
            per_call.append({
                "ticker": ticker,
                "generated_at": gen_at,
                "rating": rating,
                "entry_price": entry_price,
                "current_price": None,
                "ticker_return_pct": None,
                "spy_return_pct": None,
                "alpha_pct": None,
                "days_held": days_between(gen_at, datetime.now(timezone.utc).isoformat()),
                "status": "no_current_price",
                "n_history_snapshots": len(history),
            })
            continue

        ticker_ret = pct_change(entry_price, current_price)
        days = days_between(gen_at, datetime.now(timezone.utc).isoformat())

        spy_then = spy_then_cache.get(gen_at[:10])
        spy_ret = pct_change(spy_then, spy_now) if (spy_then and spy_now) else None
        alpha = round(ticker_ret - spy_ret, 2) if (ticker_ret is not None and spy_ret is not None) else None

        # Capture regime stamp from entry snapshot (the regime active when
        # the call was made). Falls back to latest if no entry doc.
        regime_stamp = None
        if history:
            regime_stamp = (entry_doc or {}).get("regime_at_generation") if 'entry_doc' in dir() else None
        if not regime_stamp:
            regime_stamp = latest_doc.get("regime_at_generation") or {}
        regime_at_gen = (regime_stamp or {}).get("regime")

        critique = critique_lookup.get(ticker, {})
        c_obj = critique.get("critique", {})
        per_call.append({
            "ticker": ticker,
            "generated_at": gen_at,
            "days_held": days,
            "rating": rating,
            "conviction_grade": verdict.get("conviction_grade"),
            "price_target_12m": pt,
            "entry_price": entry_price,
            "current_price": current_price,
            "ticker_return_pct": ticker_ret,
            "spy_return_pct": spy_ret,
            "alpha_pct": alpha,
            "pt_progress_pct": pct_change(entry_price, pt) if pt else None,
            "pt_capture_pct": (
                round(((current_price - entry_price) / (pt - entry_price)) * 100, 1)
                if pt and pt != entry_price else None
            ),
            "critic_rating":       c_obj.get("alternative_rating"),
            "disagreement_score":  c_obj.get("disagreement_score"),
            "rating_diverges":     bool(c_obj.get("alternative_rating") and rating
                                         and c_obj.get("alternative_rating") != rating),
            "regime_at_generation": regime_at_gen,
            "n_history_snapshots": len(history),
        })

    return per_call


def aggregate_by_field(calls: list, field: str, group_label: str = "value") -> list:
    """Group calls by a field value, compute summary stats."""
    groups = {}
    for c in calls:
        if c.get("ticker_return_pct") is None:
            continue
        v = c.get(field) or "(none)"
        groups.setdefault(v, []).append(c)

    out = []
    for v, gcalls in groups.items():
        returns = [c["ticker_return_pct"] for c in gcalls if c.get("ticker_return_pct") is not None]
        alphas = [c["alpha_pct"] for c in gcalls if c.get("alpha_pct") is not None]
        wins = sum(1 for r in returns if r > 0)
        win_rate = round(100 * wins / len(returns), 1) if returns else None
        alpha_wins = sum(1 for a in alphas if a > 0)
        alpha_win_rate = round(100 * alpha_wins / len(alphas), 1) if alphas else None

        # Median
        sr = sorted(returns)
        median_ret = sr[len(sr)//2] if sr else None

        out.append({
            group_label:       v,
            "n":               len(gcalls),
            "n_with_alpha":    len(alphas),
            "mean_return_pct": round(sum(returns)/len(returns), 2) if returns else None,
            "median_return_pct": median_ret,
            "mean_alpha_pct":  round(sum(alphas)/len(alphas), 2) if alphas else None,
            "win_rate_pct":    win_rate,
            "alpha_win_rate_pct": alpha_win_rate,
            "best_call":       max(gcalls, key=lambda c: c.get("ticker_return_pct") or -999, default={}).get("ticker"),
            "worst_call":      min(gcalls, key=lambda c: c.get("ticker_return_pct") or 999, default={}).get("ticker"),
        })

    # Sort by N desc
    out.sort(key=lambda r: r.get("n", 0), reverse=True)
    return out


def build_ensemble_attribution(calls: list) -> dict:
    """The killer view: does the critique signal predict anything?

    Compare outcomes for:
      - Consensus calls (research + critic agree on rating): high-conviction
      - Contested calls (rating_diverges = True): the AIs disagree

    If contested tickers underperform consensus systematically, the ensemble
    signal IS alpha. That validates the whole Devil's Advocate premise.
    """
    with_critique = [c for c in calls if c.get("disagreement_score") is not None
                       and c.get("ticker_return_pct") is not None]

    consensus = [c for c in with_critique if not c.get("rating_diverges")]
    contested = [c for c in with_critique if c.get("rating_diverges")]

    def stats(group):
        if not group:
            return {"n": 0}
        returns = [c["ticker_return_pct"] for c in group]
        alphas  = [c.get("alpha_pct") for c in group if c.get("alpha_pct") is not None]
        return {
            "n": len(group),
            "mean_return_pct": round(sum(returns)/len(returns), 2) if returns else None,
            "mean_alpha_pct":  round(sum(alphas)/len(alphas), 2) if alphas else None,
            "win_rate_pct":    round(100 * sum(1 for r in returns if r > 0) / len(returns), 1),
        }

    consensus_stats = stats(consensus)
    contested_stats = stats(contested)
    # Spread = consensus_alpha - contested_alpha. If positive, ensemble works.
    spread = None
    if consensus_stats.get("mean_alpha_pct") is not None and contested_stats.get("mean_alpha_pct") is not None:
        spread = round(consensus_stats["mean_alpha_pct"] - contested_stats["mean_alpha_pct"], 2)

    return {
        "n_with_critique":    len(with_critique),
        "consensus":          consensus_stats,
        "contested":          contested_stats,
        "alpha_spread_pct":   spread,
        "interpretation":     (
            "Consensus picks outperformed contested ones — ensemble signal is alpha"
            if spread is not None and spread > 0
            else "Contested picks outperformed consensus — counterintuitive, possibly noise"
            if spread is not None and spread < 0
            else "Sample too small for meaningful inference"
        ),
    }


# ═════════════════════════════════════════════════════════════════════
# SPY benchmark
# ═════════════════════════════════════════════════════════════════════
def build_spy_history(start_date: str, end_date: str) -> dict:
    """Fetch SPY EOD prices in range. Returns {date_iso: close_price} dict."""
    if not FMP_KEY:
        return {}
    url = f"{FMP_BASE}/historical-price-eod/full?symbol=SPY&from={start_date}&to={end_date}&apikey={FMP_KEY}"
    data = http_get_json(url, timeout=30)
    if not isinstance(data, list):
        return {}
    out = {}
    for row in data:
        d = row.get("date")
        p = row.get("close") or row.get("adjClose")
        if d and p:
            out[d[:10]] = float(p)
    return out


def get_spy_then(spy_history: dict, target_date: str) -> Optional[float]:
    """Find SPY price closest to target_date (going backward to nearest trading day)."""
    d = target_date[:10]
    # Try exact
    if d in spy_history:
        return spy_history[d]
    # Walk backward up to 5 days (weekends, holidays)
    from datetime import datetime as _dt, timedelta as _td
    try:
        cur = _dt.fromisoformat(d)
        for _ in range(7):
            cur = cur - _td(days=1)
            k = cur.strftime("%Y-%m-%d")
            if k in spy_history:
                return spy_history[k]
    except Exception:
        pass
    return None


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def build_regime_attribution(calls: list) -> dict:
    """Aggregate alpha BY regime AND BY rating × regime — the institutional
    attribution table.

    Returns:
      {
        by_regime: [{regime, n, avg_alpha, win_rate, n_strong_buy, n_buy, ...}],
        by_rating_regime: [{rating, regime, n, avg_alpha, win_rate}],
        regime_coverage: {n_tagged, n_total, pct_coverage},
      }

    A call qualifies as a 'win' if alpha_pct > 0. STRONG_BUY in REFLATION
    regime delivering +X% alpha vs same rating in CREDIT_STRESS regime
    delivering -Y% is the kind of read every PM wants.
    """
    tagged = [c for c in calls if c.get("regime_at_generation") and c.get("alpha_pct") is not None]
    total_with_alpha = sum(1 for c in calls if c.get("alpha_pct") is not None)

    # by regime (collapsed across all ratings)
    by_regime_groups = {}
    for c in tagged:
        r = c["regime_at_generation"]
        by_regime_groups.setdefault(r, []).append(c)

    by_regime = []
    for regime, group in sorted(by_regime_groups.items(), key=lambda x: -len(x[1])):
        alphas = [c["alpha_pct"] for c in group if c.get("alpha_pct") is not None]
        if not alphas:
            continue
        n = len(alphas)
        avg = round(sum(alphas) / n, 2)
        wins = sum(1 for a in alphas if a > 0)
        # Median + dispersion
        sorted_a = sorted(alphas)
        med = round(sorted_a[n // 2], 2)
        max_a = round(max(alphas), 2)
        min_a = round(min(alphas), 2)
        # Best rating in this regime
        by_rating_in_regime = {}
        for c in group:
            rt = c.get("rating") or "UNKNOWN"
            by_rating_in_regime.setdefault(rt, []).append(c.get("alpha_pct") or 0)
        rating_breakdown = {
            rt: {"n": len(vals), "avg_alpha": round(sum(vals) / len(vals), 2)}
            for rt, vals in by_rating_in_regime.items() if vals
        }
        by_regime.append({
            "regime": regime,
            "n": n,
            "avg_alpha_pct": avg,
            "median_alpha_pct": med,
            "max_alpha_pct": max_a,
            "min_alpha_pct": min_a,
            "win_rate_pct": round(100 * wins / n, 1),
            "by_rating": rating_breakdown,
        })

    # cross-tab: rating × regime
    by_rating_regime = []
    cells = {}
    for c in tagged:
        key = (c.get("rating") or "UNKNOWN", c["regime_at_generation"])
        cells.setdefault(key, []).append(c)
    for (rating, regime), group in sorted(cells.items()):
        alphas = [c["alpha_pct"] for c in group if c.get("alpha_pct") is not None]
        if not alphas:
            continue
        n = len(alphas)
        by_rating_regime.append({
            "rating": rating,
            "regime": regime,
            "n": n,
            "avg_alpha_pct": round(sum(alphas) / n, 2),
            "win_rate_pct": round(100 * sum(1 for a in alphas if a > 0) / n, 1),
        })

    return {
        "by_regime": by_regime,
        "by_rating_regime": by_rating_regime,
        "regime_coverage": {
            "n_calls_with_regime_tag": len(tagged),
            "n_calls_with_alpha": total_with_alpha,
            "pct_coverage": round(100 * len(tagged) / max(total_with_alpha, 1), 1),
            "n_distinct_regimes": len(by_regime_groups),
            "regimes_observed": list(by_regime_groups.keys()),
        },
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[backtest] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Find every unique ticker in the research universe
    research_keys = list_keys_under(RESEARCH_PREFIX)
    universe = set()
    earliest_gen = None
    for k in research_keys:
        doc = read_s3_json(k)
        if doc and doc.get("ticker"):
            universe.add(doc["ticker"])
            ga = doc.get("generated_at")
            if ga and (earliest_gen is None or ga < earliest_gen):
                earliest_gen = ga
    print(f"[backtest] universe: {len(universe)} unique tickers; earliest research: {earliest_gen}")

    # 2. Fetch current prices for all tickers + SPY
    now_prices = {}
    for t in universe:
        p = get_current_price(t)
        if p:
            now_prices[t] = p
    print(f"[backtest] fetched current prices for {len(now_prices)}/{len(universe)} tickers")

    spy_now = get_current_price("SPY")
    print(f"[backtest] SPY current: {spy_now}")

    # 3. Build SPY history covering the research date range
    start_date = (earliest_gen or "2025-01-01")[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spy_history = build_spy_history(start_date, end_date)
    print(f"[backtest] SPY history: {len(spy_history)} days from {start_date} to {end_date}")

    # 4. Build per-call attribution
    spy_then_cache = {}  # date -> SPY price
    for k in research_keys:
        doc = read_s3_json(k)
        if doc and doc.get("generated_at"):
            d = doc["generated_at"][:10]
            if d not in spy_then_cache:
                spy_then_cache[d] = get_spy_then(spy_history, d)

    per_call = build_per_call_attribution(now_prices, spy_now, spy_then_cache)
    # Sort by alpha desc for the report
    per_call.sort(key=lambda c: c.get("alpha_pct") if c.get("alpha_pct") is not None else -999, reverse=True)

    # 5. Aggregations
    rating_summary = aggregate_by_field(per_call, "rating", "rating")
    critique_summary = aggregate_by_field(per_call, "critic_rating", "critic_rating")
    ensemble_attr = build_ensemble_attribution(per_call)
    regime_attr = build_regime_attribution(per_call)

    # 6. Sample size honesty
    n_with_returns = sum(1 for c in per_call if c.get("ticker_return_pct") is not None)
    n_with_alpha = sum(1 for c in per_call if c.get("alpha_pct") is not None)
    avg_days_held = (
        round(sum(c["days_held"] for c in per_call if c.get("days_held")) / max(len(per_call), 1), 1)
        if per_call else 0
    )

    caveats = []
    if n_with_returns < 30:
        caveats.append(f"SMALL SAMPLE: only {n_with_returns} calls with return data — not statistically significant")
    if avg_days_held < 60:
        caveats.append(f"SHORT TIMEFRAME: avg holding period only {avg_days_held} days — ratings target 12 months")
    if not spy_now:
        caveats.append("SPY benchmark unavailable — alpha calculations missing")
    if len(now_prices) < len(universe):
        caveats.append(f"PRICE MISSING: {len(universe) - len(now_prices)} tickers couldn't be priced")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "universe_size": len(universe),
        "n_research_files": len(research_keys),
        "n_calls_with_returns": n_with_returns,
        "n_calls_with_alpha": n_with_alpha,
        "avg_days_held": avg_days_held,
        "earliest_research": earliest_gen,
        "spy_current_price": spy_now,
        "caveats": caveats,
        "per_call": per_call,
        "rating_summary": rating_summary,
        "critique_summary": critique_summary,
        "ensemble_attribution": ensemble_attr,
        "regime_attribution":   regime_attr,
    }

    # Write to S3
    body = json.dumps(out, default=str).encode()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    print(f"[backtest] DONE in {out['elapsed_s']}s — wrote {OUTPUT_KEY} ({round(len(body)/1024,1)}KB)")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "ok": True,
            "n_calls": n_with_returns,
            "elapsed_s": out["elapsed_s"],
            "key": OUTPUT_KEY,
        }),
    }
