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
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = "justhodl-dashboard-live"
RESEARCH_PREFIX = "equity-research/"
HISTORY_PREFIX = "equity-research-history/"
CRITIQUE_PREFIX = "equity-critique/"
OUTPUT_KEY = "analytics/backtest_results.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

s3 = boto3.client("s3", region_name="us-east-1")
_document_cache = {}
_key_cache = {}
_deadline = None

def require_time():
    if _deadline is not None and time.monotonic() >= _deadline:
        raise RuntimeError("RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION")


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════
def http_get_json(url: str, timeout: int = 15) -> Optional[list]:
    require_time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Backtest/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print("[fmp] PROVIDER_UNAVAILABLE")
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
    require_time()
    if prefix in _key_cache: return _key_cache[prefix]
    keys = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            k = obj["Key"]
            if k.endswith(".json") and not k.endswith("manifest.json"):
                keys.append(k)
    _key_cache[prefix] = keys
    return keys


def read_s3_json(key: str) -> Optional[dict]:
    require_time()
    if key in _document_cache: return _document_cache[key]
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        document=json.loads(body)
        _document_cache[key]=document
        return document
    except Exception as e:
        print("[read] SOURCE_UNAVAILABLE")
        return None


# ═════════════════════════════════════════════════════════════════════
# Core backtest
# ═════════════════════════════════════════════════════════════════════
_history_by_ticker = None


def list_history_for_ticker(ticker: str) -> list:
    """Enumerate history once per invocation, retaining every dated decision."""
    global _history_by_ticker
    if _history_by_ticker is None:
        catalog={}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET,Prefix=HISTORY_PREFIX):
            for obj in page.get("Contents",[]):
                key=obj["Key"]
                parts=key[len(HISTORY_PREFIX):].split("/")
                if len(parts)==2 and parts[1].endswith(".json"):
                    catalog.setdefault(parts[1][:-5],[]).append((parts[0],key))
        _history_by_ticker={sym:sorted(entries) for sym,entries in catalog.items()}
    return _history_by_ticker.get(ticker,[])


def _utc_timestamp(value):
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            return None
        return ts.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def critique_available_at(critique, decision_iso):
    """Only data known by the decision instant may enter attribution."""
    if not isinstance(critique, dict):
        return None
    available = _utc_timestamp(critique.get("available_at") or critique.get("generated_at"))
    decision = _utc_timestamp(decision_iso)
    return critique if available is not None and decision is not None and available <= decision else None


def build_per_call_attribution(now_prices: dict, spy_now: Optional[float],
                                spy_then_cache: dict) -> list:
    """One attribution per immutable research decision; never substitute today's context."""
    research_keys=list_keys_under(RESEARCH_PREFIX)
    critique_lookup={}
    for key in list_keys_under(CRITIQUE_PREFIX):
        doc=read_s3_json(key)
        if isinstance(doc,dict) and doc.get("ticker"):
            critique_lookup.setdefault(doc["ticker"],[]).append(doc)
    rows=[]
    seen=set()
    for key in research_keys:
        latest=read_s3_json(key)
        if not isinstance(latest,dict) or not latest.get("ticker"):
            continue
        ticker=latest["ticker"]
        history=list_history_for_ticker(ticker)
        entries=[(key,read_s3_json(key)) for _date,key in history]
        entries.append((key,latest))
        for source_key,entry in entries:
            if not isinstance(entry,dict):
                continue
            generated=entry.get("generated_at")
            decision=_utc_timestamp(entry.get("available_at") or generated)
            if decision is None or decision > datetime.now(timezone.utc):
                continue
            research_id=entry.get("research_id") or entry.get("id")
            identity=str(research_id or (ticker+"@"+decision.isoformat()))
            if identity in seen:
                continue
            seen.add(identity)
            entry_price=(entry.get("quote") or {}).get("price")
            current=now_prices.get(ticker)
            if not entry_price or not generated:
                continue
            verdict=entry.get("verdict") or {}
            rating=verdict.get("rating")
            target=verdict.get("price_target_12m")
            ticker_ret=pct_change(entry_price,current) if current else None
            spy_then=spy_then_cache.get(generated[:10])
            spy_ret=pct_change(spy_then,spy_now) if spy_then and spy_now else None
            alpha=round(ticker_ret-spy_ret,2) if ticker_ret is not None and spy_ret is not None else None
            candidates=[]
            for cd in critique_lookup.get(ticker,[]):
                # A critique of another call is not evidence for this decision, even if older.
                same_call=(research_id is not None and cd.get("research_id")==research_id) or cd.get("research_generated_at")==generated
                if same_call and critique_available_at(cd,decision.isoformat()) is not None:
                    candidates.append(cd)
            critique=max(candidates,key=lambda cd:_utc_timestamp(cd.get("available_at") or cd.get("generated_at"))) if candidates else {}
            c=critique.get("critique") or {}
            stamp=entry.get("regime_at_generation") or {}
            regime=stamp.get("regime") if isinstance(stamp,dict) else None
            rows.append({"research_id":identity,"source_key":source_key,"ticker":ticker,"generated_at":generated,
                         "available_at":decision.isoformat(),"days_held":days_between(generated,datetime.now(timezone.utc).isoformat()),
                         "rating":rating,"conviction_grade":verdict.get("conviction_grade"),"price_target_12m":target,
                         "entry_price":entry_price,"current_price":current,"ticker_return_pct":ticker_ret,"spy_return_pct":spy_ret,
                         "alpha_pct":alpha,"status":"priced_attribution" if alpha is not None else "incomplete_marks",
                         "pt_progress_pct":pct_change(entry_price,target) if target else None,
                         "pt_capture_pct":round(((current-entry_price)/(target-entry_price))*100,1) if current and target and target!=entry_price else None,
                         "critic_rating":c.get("alternative_rating"),"disagreement_score":c.get("disagreement_score"),
                         "rating_diverges":bool(c.get("alternative_rating") and rating and c["alternative_rating"]!=rating),
                         "regime_at_generation":regime,"regime_source":"entry_snapshot" if regime else "unknown",
                         "critique_source":("critique@"+str(critique.get("available_at") or critique.get("generated_at"))) if critique else "unknown_call_identity_or_availability",
                         "n_history_snapshots":len(history),"headline_eligible":False})
    return rows


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
    # Spread = consensus_alpha - contested_alpha, with a Welch t-test and coverage requirement.
    # audit 2026-09-08 INST-11: a positive mean spread is NOT "alpha" without significance and coverage.
    a, b = [], []
    spread = None
    t_stat = None
    ci95 = None
    degrees_of_freedom = None
    critical_t = None
    n_total = len(calls)
    coverage = (sum(c.get("alpha_pct") is not None for c in with_critique) / n_total) if n_total else 0.0
    if consensus_stats.get("mean_alpha_pct") is not None and contested_stats.get("mean_alpha_pct") is not None:
        spread = round(consensus_stats["mean_alpha_pct"] - contested_stats["mean_alpha_pct"], 2)
        a = [c["alpha_pct"] for c in consensus if c.get("alpha_pct") is not None]
        b = [c["alpha_pct"] for c in contested if c.get("alpha_pct") is not None]
        if len(a) >= 2 and len(b) >= 2:
            ma, mb = sum(a) / len(a), sum(b) / len(b)
            va = sum((x - ma) ** 2 for x in a) / (len(a) - 1)
            vb = sum((x - mb) ** 2 for x in b) / (len(b) - 1)
            se = (va / len(a) + vb / len(b)) ** 0.5
            if se > 0:
                t_stat = round((ma - mb) / se, 3)
                denominator = (va/len(a))**2/(len(a)-1) + (vb/len(b))**2/(len(b)-1)
                degrees_of_freedom = se**4/denominator if denominator else None
                # Conservative lower-df Student-t critical value (two-sided 95%).
                critical_t = 12.706
                for df,critical in ((1,12.706),(2,4.303),(5,2.571),(10,2.228),(15,2.131),(20,2.086),(30,2.042),(40,2.021),(60,2.000),(120,1.980)):
                    if degrees_of_freedom is not None and degrees_of_freedom >= df:
                        critical_t=critical
                ci95 = [round(ma-mb-critical_t*se,2), round(ma-mb+critical_t*se,2)]
    min_n = 20
    significant = t_stat is not None and critical_t is not None and abs(t_stat) >= critical_t and len(a) >= min_n and len(b) >= min_n and coverage >= 0.5
    if spread is None:
        interp = "Sample too small for meaningful inference"
    elif significant and spread > 0:
        interp = "Positive consensus/contested association in this paired sample (Welch-t threshold met, paired n>=%d each, coverage>=50%%); overlapping calls and market factors prevent an investable alpha claim" % min_n
    elif significant and spread < 0:
        interp = "Contested picks outperformed consensus with statistical significance — counter to the ensemble premise"
    else:
        interp = "Spread %s%% does not meet the evidence threshold (t=%s, paired-alpha n=%d/%d, coverage %.0f%%) — no alpha claim" % (spread, t_stat, len(a), len(b), coverage * 100)

    return {
        "n_calls_total":      n_total,
        "n_with_critique":    len(with_critique),
        "n_alpha_consensus": len(a), "n_alpha_contested": len(b),
        "critique_coverage_pct": round(coverage * 100, 1),
        "consensus":          consensus_stats,
        "contested":          contested_stats,
        "alpha_spread_pct":   spread,
        "t_stat":             t_stat,
        "welch_degrees_of_freedom": degrees_of_freedom, "critical_t_95": critical_t,
        "ci95_spread_pct":    ci95,
        "significance_rule":  "two-sided conservative Welch-t 95%% threshold, paired alpha n >= %d per group, paired coverage >= 50%%; critique matched to call identity and availability" % min_n,
        "significant":        bool(significant),
        "interpretation":     interp,
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
    global _history_by_ticker, _document_cache, _key_cache, _deadline
    _history_by_ticker = None
    _document_cache = {}
    _key_cache = {}
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,"get_remaining_time_in_millis") else 900
    _deadline=time.monotonic()+max(1,remaining-45)
    t0 = time.time()
    print(f"[backtest] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Find every unique ticker in the research universe
    research_keys = list_keys_under(RESEARCH_PREFIX)
    # Cache each full source document once; eight reads at a time retain all rows.
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(read_s3_json,research_keys))
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
    with ThreadPoolExecutor(max_workers=4) as pool:
        tickers=sorted(universe | {"SPY"})
        prices=dict(zip(tickers,pool.map(get_current_price,tickers)))
    now_prices={ticker:prices[ticker] for ticker in universe if prices.get(ticker) is not None}
    spy_now=prices.get("SPY")
    print(f"[backtest] price coverage {len(now_prices)}/{len(universe)}")

    # 3. Build SPY history covering the research date range
    historical_dates=[date for ticker in universe for date,_ in list_history_for_ticker(ticker)]
    start_date = min(historical_dates + [(earliest_gen or "2025-01-01")[:10]])
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

    for ticker in universe:
        for date, _key in list_history_for_ticker(ticker):
            if date not in spy_then_cache:
                spy_then_cache[date] = get_spy_then(spy_history, date)
    historical_keys=[key for ticker in universe for _date,key in list_history_for_ticker(ticker)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(read_s3_json,historical_keys+list_keys_under(CRITIQUE_PREFIX)))
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
        "audit_version": "2026-09-09.1",
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

    # An incomplete computation never replaces the last complete publication.
    require_time()
    # Write to S3
    body = json.dumps(out, default=str).encode()
    if isinstance(event, dict) and event.get("mode") == "validate_only":
        return {"ok": True, "validation_only": True, "schema_version": "audit-accounting-1.0",
                "status": "RESEARCH_ONLY", "artifact_size_bytes": len(body)}
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
