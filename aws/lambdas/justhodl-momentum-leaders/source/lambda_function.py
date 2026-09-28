"""
justhodl-momentum-leaders
═════════════════════════
Pure-momentum ranker. Surfaces stocks that are ALREADY MOVING strongly
and most likely to keep pumping. This is the academic momentum factor
(Jegadeesh-Titman 1993, Carhart 1997) applied to a curated universe.

UNIVERSE (in priority order)
════════════════════════════
1. Tickers in the momentum-breakout engine output (already filtered to
   names showing breakout characteristics)
2. Tickers in the convergence-radar pump_candidates list
3. Top-50 most-active tickers from any other available engine output

For each universe ticker, fetch 90d daily OHLCV from FMP /stable and
compute MOMENTUM COMPOSITE SCORE (0-100):

  30 × 20-day percentile rank of perf_20d (cross-sectional momentum)
  25 × 60-day percentile rank of perf_60d
  20 × Relative strength vs SPY (20d outperformance, normalized)
  15 × 52-week high proximity (current / 52w_high)
  10 × Volume surge (today_vol / 20d_avg_vol, capped at 5x)

Plus secondary tags:
  AT_52W_HIGH       — within 2% of 52-week high
  BREAKOUT_VOLUME   — today vol > 2x 20d avg
  ACCELERATING      — 5d perf > 20d perf annualized (rate of change ↑)
  RS_LEADER         — outperforming SPY by 15%+ in 20d
  ALL_GREEN         — perf positive across 5d/20d/60d horizons
  GAP_UP            — opened higher than prior close in last 3 sessions
  PUMP_CONFIRMED    — has BOTH momentum_score ≥ 70 AND in pump_candidates
                       (highest-probability continuations)

OUTPUT
══════
data/momentum-leaders.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "lookback_days":  90,
  "n_scored":       45,
  "n_pump_confirmed": 6,

  "leaders": [
    {
      "ticker":         "PLTR",
      "momentum_score": 87.3,
      "rank":           1,
      "current_price":  162.81,
      "perf_5d_pct":    19.04,
      "perf_20d_pct":   13.10,
      "perf_60d_pct":   42.5,
      "perf_5d_rank":   0.95,           # percentile within universe
      "perf_20d_rank":  0.88,
      "perf_60d_rank":  0.91,
      "rs_spy_20d_pct": 11.8,           # outperformance vs SPY
      "wk52_proximity": 0.98,           # 98% of 52w high
      "volume_surge":   2.8,            # today / 20d avg
      "tags":           ["RS_LEADER", "ALL_GREEN", "BREAKOUT_VOLUME", "PUMP_CONFIRMED"],
      "in_pump_candidates": true,
      "pump_likelihood":    54.5,       # from convergence-radar
      "n_engines":          8,
      "convergence_tier":   "ULTRA",
      "pump_confirmed":     true        # has both momentum AND convergence
    },
    ...
  ],

  "pump_confirmed": [...],              # subset where momentum AND convergence agree

  "metadata": {
    "universe_size":  60,
    "universe_sources": ["momentum-breakout", "convergence-radar", "ticker-trends"],
    "spy_perf_20d":   2.8,
    "spy_perf_60d":   5.4,
    "scoring_weights": { ... }
  }
}

SCHEDULE
════════
cron(25 * * * ? *) — hourly at :25
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
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET   = "justhodl-dashboard-live"
RADAR_KEY   = "data/convergence-radar.json"
MOMENTUM_BO_KEY = "data/momentum-breakout.json"
ETF_KEY     = "data/ticker-trends.json"
OUTPUT_KEY  = "data/momentum-leaders.json"
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))

LOOKBACK_DAYS  = 90
MAX_UNIVERSE   = 60     # cap to keep within Lambda time/cost budget
MOMENTUM_CUTOFF = 50    # min composite score to show in leaders array
PUMP_CONFIRMED_CUTOFF = 70  # min momentum_score to be PUMP_CONFIRMED

s3 = boto3.client("s3", region_name="us-east-1")
PRICE_CACHE: Dict[str, List[dict]] = {}


# ═════════════════════════════════════════════════════════════════════
# Universe building
# ═════════════════════════════════════════════════════════════════════

from momentum_research_boundary import BASIS as MOMENTUM_BASIS, DIRECT as MOMENTUM_SOURCE, exclusion as momentum_exclusion, current as momentum_current, guard as momentum_guard, transition_state as momentum_transition_state, transition_abstention as momentum_transition_abstention

def load_s3_json(key: str) -> Optional[dict]:
    if key == MOMENTUM_SOURCE:
        return momentum_exclusion()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return momentum_guard(key, json.loads(obj["Body"].read()))
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def build_universe() -> Dict[str, dict]:
    """Returns {ticker: {sources: [...], pump_data: {...}}} merging all signal lists."""
    universe: Dict[str, dict] = {}

    # 1. momentum-breakout engine
    mbo = load_s3_json(MOMENTUM_BO_KEY) or {}
    for item in (mbo.get("all_qualifying") or mbo.get("tickers") or []):
        t = item.get("symbol") or item.get("ticker")
        if t:
            universe.setdefault(t, {"sources": [], "mbo_data": {}})
            universe[t]["sources"].append("momentum-breakout")
            universe[t]["mbo_data"] = {
                "tier":   item.get("tier"),
                "score":  item.get("score"),
                "flags":  item.get("flags", []),
            }

    # 2. convergence-radar pump_candidates (and full top list)
    radar = load_s3_json(RADAR_KEY) or {}
    for r in (radar.get("pump_candidates") or []):
        t = r.get("ticker")
        if t:
            universe.setdefault(t, {"sources": [], "mbo_data": {}})
            universe[t]["sources"].append("convergence-pump")
            universe[t]["radar_data"] = {
                "tier":            r.get("tier"),
                "n_engines":       r.get("n_engines"),
                "convergence":     r.get("convergence_score"),
                "directional":     r.get("directional_score"),
                "pump_likelihood": r.get("pump_likelihood"),
                "pump_category":   r.get("pump_category"),
            }
    # Plus the rest of the top tickers (not in pump_candidates but multi-engine)
    for r in (radar.get("tickers") or [])[:30]:
        t = r.get("ticker")
        if t and t not in universe:
            universe.setdefault(t, {"sources": ["convergence-multi"], "mbo_data": {}})
            universe[t]["radar_data"] = {
                "tier":         r.get("tier"),
                "n_engines":    r.get("n_engines"),
                "convergence":  r.get("convergence_score"),
            }

    # 3. ticker-trends (additional source if available)
    tt = load_s3_json(ETF_KEY) or {}
    for item in (tt.get("trends") or tt.get("tickers") or [])[:25]:
        t = item.get("symbol") or item.get("ticker")
        if t:
            universe.setdefault(t, {"sources": [], "mbo_data": {}})
            if "ticker-trends" not in universe[t]["sources"]:
                universe[t]["sources"].append("ticker-trends")

    # Cap
    if len(universe) > MAX_UNIVERSE:
        # Prioritize: in pump_candidates first, then momentum-breakout, then others
        def priority(item):
            t, meta = item
            score = 0
            srcs = meta.get("sources", [])
            if "convergence-pump" in srcs:    score += 100
            if "momentum-breakout" in srcs:    score += 50
            if "ticker-trends" in srcs:        score += 25
            if "convergence-multi" in srcs:    score += 10
            return -score
        universe = dict(sorted(universe.items(), key=priority)[:MAX_UNIVERSE])

    return universe


# ═════════════════════════════════════════════════════════════════════
# Price + momentum calculations
# ═════════════════════════════════════════════════════════════════════

def fetch_price_rows(ticker: str) -> List[dict]:
    if ticker in PRICE_CACHE:
        return PRICE_CACHE[ticker]
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=LOOKBACK_DAYS + 200)  # extra for 52w high
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/momentum"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("historical", [])
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        PRICE_CACHE[ticker] = rows
        return rows
    except Exception as e:
        print(f"[price] {ticker}: {str(e)[:100]}")
        PRICE_CACHE[ticker] = []
        return []


def perf(rows: List[dict], days: int) -> Optional[float]:
    if not rows or len(rows) < days + 1:
        return None
    e = rows[-1].get("close"); s = rows[-(days+1)].get("close")
    if not e or not s or s <= 0: return None
    return round((e / s - 1) * 100, 2)


def fifty_two_week_high_proximity(rows: List[dict]) -> Optional[float]:
    if not rows or len(rows) < 50:
        return None
    # Use available rows (capped to ~250 trading days = 1 year)
    window = rows[-250:]
    highs = [r.get("high") for r in window if r.get("high")]
    if not highs:
        return None
    wk52_high = max(highs)
    current = rows[-1].get("close")
    if not current or wk52_high <= 0:
        return None
    return round(current / wk52_high, 4)


def volume_surge(rows: List[dict]) -> Optional[float]:
    """Today's volume / 20-day average volume."""
    if not rows or len(rows) < 21:
        return None
    today_vol = rows[-1].get("volume", 0)
    vols = [r.get("volume", 0) for r in rows[-21:-1] if r.get("volume")]
    if not vols or today_vol <= 0:
        return None
    avg = sum(vols) / len(vols)
    if avg <= 0: return None
    return round(today_vol / avg, 2)


def gap_up_count(rows: List[dict], lookback: int = 3) -> int:
    """Count days in lookback window where open > prior close."""
    if not rows or len(rows) < lookback + 1:
        return 0
    count = 0
    for i in range(-lookback, 0):
        if rows[i].get("open") and rows[i-1].get("close") and rows[i]["open"] > rows[i-1]["close"]:
            count += 1
    return count


def percentile_rank(values: List[float], target: float) -> float:
    """Where does `target` rank in the population? 0.0 = lowest, 1.0 = highest."""
    if not values:
        return 0.5
    n = sum(1 for v in values if v is not None and v <= target)
    total = sum(1 for v in values if v is not None)
    return n / total if total > 0 else 0.5


# ═════════════════════════════════════════════════════════════════════
# Per-ticker momentum dossier
# ═════════════════════════════════════════════════════════════════════

def compute_momentum_dossier(ticker: str, spy_rows: List[dict]) -> Optional[dict]:
    rows = fetch_price_rows(ticker)
    if not rows or len(rows) < 25:
        return None

    p_5  = perf(rows, 5)
    p_20 = perf(rows, 20)
    p_60 = perf(rows, 60)
    if p_20 is None:
        return None  # need at least 20d perf

    # SPY-relative for the same windows
    spy_5  = perf(spy_rows, 5)  if spy_rows else 0
    spy_20 = perf(spy_rows, 20) if spy_rows else 0
    rs_20  = (p_20 - spy_20) if (p_20 is not None and spy_20 is not None) else None

    wk52_prox = fifty_two_week_high_proximity(rows)
    vol_surge = volume_surge(rows)
    gaps      = gap_up_count(rows, 3)
    current   = rows[-1].get("close")

    return {
        "ticker":        ticker,
        "current_price": current,
        "perf_5d_pct":   p_5,
        "perf_20d_pct":  p_20,
        "perf_60d_pct":  p_60,
        "rs_spy_20d_pct": round(rs_20, 2) if rs_20 is not None else None,
        "wk52_proximity": wk52_prox,
        "volume_surge":   vol_surge,
        "gap_up_count_3d": gaps,
    }


def compute_composite_score(dossier: dict, all_dossiers: List[dict]) -> dict:
    """Calculate the 0-100 momentum composite score with percentile ranking
    across the universe."""

    # Build percentile distributions across the universe
    p20s = [d.get("perf_20d_pct") for d in all_dossiers if d.get("perf_20d_pct") is not None]
    p60s = [d.get("perf_60d_pct") for d in all_dossiers if d.get("perf_60d_pct") is not None]
    rs20s = [d.get("rs_spy_20d_pct") for d in all_dossiers if d.get("rs_spy_20d_pct") is not None]

    p20_rank = percentile_rank(p20s, dossier["perf_20d_pct"]) if dossier.get("perf_20d_pct") is not None else 0.5
    p60_rank = percentile_rank(p60s, dossier.get("perf_60d_pct", 0)) if dossier.get("perf_60d_pct") is not None else 0.5

    # RS to a 0-1 scale (clip at ±30%)
    rs = dossier.get("rs_spy_20d_pct") or 0
    rs_norm = max(0, min(1, (rs + 15) / 45))  # -15% RS = 0, +30% RS = 1

    # 52w proximity already 0-1
    wk52 = dossier.get("wk52_proximity") or 0

    # Volume surge: 1x = 0, 5x+ = 1
    vol_norm = max(0, min(1, ((dossier.get("volume_surge") or 1) - 1) / 4))

    # Composite weights
    score = (
        30 * p20_rank +
        25 * p60_rank +
        20 * rs_norm +
        15 * wk52 +
        10 * vol_norm
    )

    return {
        "momentum_score":  round(score, 1),
        "perf_5d_rank":    round(percentile_rank([d.get("perf_5d_pct") for d in all_dossiers], dossier.get("perf_5d_pct", 0)), 2) if dossier.get("perf_5d_pct") is not None else None,
        "perf_20d_rank":   round(p20_rank, 2),
        "perf_60d_rank":   round(p60_rank, 2),
    }


def derive_tags(dossier: dict, score: float, in_pump_candidates: bool) -> List[str]:
    tags = []
    if dossier.get("wk52_proximity") and dossier["wk52_proximity"] >= 0.98:
        tags.append("AT_52W_HIGH")
    elif dossier.get("wk52_proximity") and dossier["wk52_proximity"] >= 0.92:
        tags.append("NEAR_52W_HIGH")
    if dossier.get("volume_surge") and dossier["volume_surge"] >= 2.0:
        tags.append("BREAKOUT_VOLUME")
    if dossier.get("rs_spy_20d_pct") and dossier["rs_spy_20d_pct"] >= 15:
        tags.append("RS_LEADER")
    p5, p20, p60 = dossier.get("perf_5d_pct"), dossier.get("perf_20d_pct"), dossier.get("perf_60d_pct")
    if p5 and p20 and p60 and p5 > 0 and p20 > 0 and p60 > 0:
        tags.append("ALL_GREEN")
    # ACCELERATING: 5d annualized > 20d annualized
    if p5 is not None and p20 is not None:
        ann_5d = p5 * (252/5)
        ann_20d = p20 * (252/20)
        if ann_5d > ann_20d * 1.5 and p5 > 0:
            tags.append("ACCELERATING")
    if dossier.get("gap_up_count_3d", 0) >= 2:
        tags.append("MULTI_GAP_UP")
    if score >= 80:
        tags.append("MOMENTUM_LEADER")
    if score >= PUMP_CONFIRMED_CUTOFF and in_pump_candidates:
        tags.append("PUMP_CONFIRMED")
    return tags


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def _legacy_lambda_handler(event, context):
    t0 = time.time()
    print(f"[momentum] start {datetime.now(timezone.utc).isoformat()}")

    universe = build_universe()
    print(f"[momentum] universe: {len(universe)} tickers")
    if not universe:
        return _write_error("Empty universe")

    # Pre-warm SPY first (needed for RS calculations)
    spy_rows = fetch_price_rows("SPY")

    # Pre-warm all universe price data in parallel
    tickers = list(universe.keys())
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_price_rows, t) for t in tickers]
        for fut in as_completed(futures, timeout=120):
            try: fut.result()
            except Exception: pass

    # Compute per-ticker dossiers
    dossiers = []
    for t in tickers:
        d = compute_momentum_dossier(t, spy_rows)
        if d:
            dossiers.append(d)
    print(f"[momentum] {len(dossiers)} dossiers built (price-data complete)")

    if not dossiers:
        return _write_error("No tickers had enough price data")

    # Compute composite scores (need full population for percentile ranks)
    for d in dossiers:
        scoring = compute_composite_score(d, dossiers)
        d.update(scoring)

    # Sort by momentum score
    dossiers.sort(key=lambda d: -d["momentum_score"])
    for i, d in enumerate(dossiers, 1):
        d["rank"] = i

    # Attach convergence/pump info and tags
    leaders = []
    pump_confirmed = []
    for d in dossiers:
        meta = universe.get(d["ticker"], {})
        radar = meta.get("radar_data", {})
        in_pump = "convergence-pump" in meta.get("sources", [])
        d["in_pump_candidates"] = in_pump
        d["pump_likelihood"]    = radar.get("pump_likelihood")
        d["n_engines"]          = radar.get("n_engines")
        d["convergence_tier"]   = radar.get("tier")
        d["sources"]            = meta.get("sources", [])
        d["mbo_tier"]           = meta.get("mbo_data", {}).get("tier")
        d["mbo_flags"]          = meta.get("mbo_data", {}).get("flags", [])

        d["tags"] = derive_tags(d, d["momentum_score"], in_pump)

        is_pump_confirmed = ("PUMP_CONFIRMED" in d["tags"])
        d["pump_confirmed"] = is_pump_confirmed
        if is_pump_confirmed:
            pump_confirmed.append(d)

        if d["momentum_score"] >= MOMENTUM_CUTOFF:
            leaders.append(d)

    # Universe-level stats
    spy_p20 = perf(spy_rows, 20) if spy_rows else None
    spy_p60 = perf(spy_rows, 60) if spy_rows else None

    output = {
        "momentum_research_exclusion": momentum_exclusion(),
        "schema_version":    "1.0",
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":       round(time.time() - t0, 2),
        "lookback_days":     LOOKBACK_DAYS,
        "n_scored":          len(dossiers),
        "n_leaders":         len(leaders),
        "n_pump_confirmed":  len(pump_confirmed),
        "leaders":           leaders[:30],          # top 30 leaders
        "pump_confirmed":    pump_confirmed,        # all confirmed (usually <10)
        "all_scored":        dossiers,               # full dataset for the page
        "metadata": {
            "universe_size":   len(universe),
            "universe_sources": sorted(set(
                s for meta in universe.values() for s in meta.get("sources", [])
            )),
            "spy_perf_20d":   spy_p20,
            "spy_perf_60d":   spy_p60,
            "scoring_weights": {
                "perf_20d_rank":   30,
                "perf_60d_rank":   25,
                "rs_spy_20d":      20,
                "wk52_proximity":  15,
                "volume_surge":    10,
            },
            "tag_definitions": {
                "AT_52W_HIGH":     "within 2% of 52-week high",
                "NEAR_52W_HIGH":   "within 8% of 52-week high",
                "BREAKOUT_VOLUME": "today vol > 2x 20-day average",
                "RS_LEADER":       "outperforming SPY by ≥ 15% in 20d",
                "ALL_GREEN":       "positive across 5d, 20d, 60d",
                "ACCELERATING":    "5d annualized > 1.5x 20d annualized",
                "MULTI_GAP_UP":    "≥ 2 gap-up opens in last 3 sessions",
                "MOMENTUM_LEADER": "composite score ≥ 80",
                "PUMP_CONFIRMED":  "momentum ≥ 70 AND in convergence pump_candidates",
            },
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=600")

    archive_key = (f"data/archive/momentum-leaders/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":           "ok",
        "elapsed_sec":      output["elapsed_sec"],
        "n_scored":         output["n_scored"],
        "n_leaders":        output["n_leaders"],
        "n_pump_confirmed": output["n_pump_confirmed"],
        "top_3":            [l["ticker"] + ":" + str(l["momentum_score"]) for l in leaders[:3]],
        "pump_confirmed":   [c["ticker"] for c in pump_confirmed[:10]],
    }
    print(f"[momentum] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[momentum] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}


# Whole predecessor remains above. The active handler publishes dated observations only.
from pathlib import Path
import threading
from leader_price_observations import (CONTRACT,HEAD,FLAGS,INPUTS,sha,encode,strict,clock,
    symbol,endpoint,source_ref,validate_ref,content,universe,history,build)

def _leaders_source_identity():
    root=Path(__file__).resolve().parent
    paths={name:root/name for name in ('lambda_function.py','leader_price_observations.py','momentum_research_boundary.py')}
    if not paths['momentum_research_boundary.py'].exists():
        paths['momentum_research_boundary.py']=root.parents[2]/'shared/momentum_research_boundary.py'
    return {name:{'bytes':len(raw),'sha256':sha(raw)} for name,path in paths.items() for raw in [path.read_bytes()]}

def _leaders_object(key,bound):
    obj=s3.get_object(Bucket=S3_BUCKET,Key=key)
    try:raw=obj['Body'].read(bound+1)
    finally:obj['Body'].close()
    if len(raw)>bound or obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole versioned declared object required')
    return raw,obj['ETag']

def _leaders_immutable(key,raw):
    try:s3.put_object(Bucket=S3_BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    if _leaders_object(key,len(raw))[0]!=raw:raise ValueError('Whole immutable readback differs')

def _leaders_archive(raw):
    ref={'key':'data/momentum-leaders/history/'+sha(raw)+'.json','bytes':len(raw),'sha256':sha(raw)}
    _leaders_immutable(ref['key'],raw);return ref

class _LeadersSources:
    def __init__(self):self.raw={};self.bytes=0;self.lock=threading.Lock();self.stop=threading.Event()
    def capture(self,raw,url,requested_at):
        ref=source_ref(raw)
        with self.lock:
            if ref['key'] not in self.raw:
                if self.bytes+len(raw)>160*1024*1024:raise ValueError('Whole source retention budget exceeded; preserve current')
                _leaders_immutable(ref['key'],raw);self.raw[ref['key']]=raw;self.bytes+=len(raw)
        return {'status':'received','endpoint':url,'requested_at':requested_at,
            'received_at':datetime.now(timezone.utc).isoformat(),'original_ref':ref}

def _leaders_fetch(ticker,sources,remaining):
    if symbol(ticker) is None:return {'status':'invalid_symbol_not_requested'}
    url=endpoint(ticker);base={'endpoint':url,'status':'not_attempted_runtime_rate_or_size_limit'}
    if remaining()<60 or sources.bytes>=96*1024*1024 or sources.stop.is_set():return base
    if not FMP_KEY:return {**base,'status':'credential_unavailable'}
    requested=datetime.now(timezone.utc).isoformat()
    to=clock(requested).date();window={'from':(to-timedelta(days=290)).isoformat(),'to':to.isoformat()}
    request_url=url+'&from='+window['from']+'&to='+window['to']
    base['request_window']=window
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        req=urllib.request.Request(request_url,headers={'apikey':FMP_KEY,'User-Agent':'JustHodl-Leader-Observations/1.0','Accept-Encoding':'identity'})
        try:response=urllib.request.build_opener(NoRedirect()).open(req,timeout=15)
        except urllib.error.HTTPError as exc:response=exc
        with response:status=response.status;raw=response.read(8*1024*1024+1)
    except Exception as exc:
        return {**base,'status':'transport_unavailable','requested_at':requested,'received_at':datetime.now(timezone.utc).isoformat(),'error_type':type(exc).__name__}
    if status in (401,403,429):sources.stop.set()
    result={**base,'http_status':status,'requested_at':requested,'received_at':datetime.now(timezone.utc).isoformat()}
    if len(raw)>8*1024*1024:return {**result,'status':'response_exceeds_bound','original_retained':False}
    reflected=FMP_KEY.encode() in raw
    try:decoded=json.loads(raw)
    except (ValueError,UnicodeError,RecursionError):decoded=None
    pending=[decoded]
    while pending and not reflected:
        value=pending.pop()
        if isinstance(value,str):reflected=FMP_KEY in value
        elif isinstance(value,dict):pending.extend(value.keys());pending.extend(value.values())
        elif isinstance(value,list):pending.extend(value)
    if reflected:return {**result,'status':'credential_echo_withheld','original_retained':False}
    # Whole retention failures propagate; they cannot become a successful empty response.
    out=sources.capture(raw,url,requested);out['http_status']=status;out['request_window']=window;return out

def lambda_handler(event=None,context=None):
    started=time.monotonic();checked=datetime.now(timezone.utc)
    if OUTPUT_KEY!=HEAD or MAX_UNIVERSE!=60 or LOOKBACK_DAYS!=90:raise ValueError('Original output, population and lookback controls required')
    try:previous_raw,etag=_leaders_object(HEAD,64*1024*1024)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        previous_raw=None;etag=None
    previous=strict(previous_raw) if previous_raw is not None else None
    if previous is not None and not isinstance(previous,dict):raise ValueError('Malformed previous publication')
    if previous and previous.get('measurement_contract')==CONTRACT:
        stamp=clock(previous.get('generated_at'))
        if stamp is None or stamp>=checked:raise ValueError('Previous publication clock invalid')
    def remaining():
        own=240-(time.monotonic()-started)
        return min(own,context.get_remaining_time_in_millis()/1000) if context and hasattr(context,'get_remaining_time_in_millis') else own
    sources=_LeadersSources();captures={}
    for key in INPUTS:
        requested=datetime.now(timezone.utc).isoformat()
        try:raw,source_etag=_leaders_object(key,8*1024*1024)
        except Exception as exc:
            captures[key]={'endpoint':key,'status':'selection_source_unavailable','error_type':type(exc).__name__,
                'requested_at':requested,'received_at':datetime.now(timezone.utc).isoformat()}
            continue
        captures[key]=sources.capture(raw,key,requested);captures[key]['etag']=source_etag
    membership=universe(captures,sources.raw,MAX_UNIVERSE);selected=membership['selected']
    if not selected and not any(r['status']=='parsed_public_selection_source' and not r.get('shape_issues') for r in membership['input_outcomes']):
        raise ValueError('No usable selection sources; preserve current publication')
    benchmark_attempt=_leaders_fetch('SPY',sources,remaining) if selected else {'endpoint':endpoint('SPY'),'status':'not_requested_no_selected_tickers'}
    attempts=[{'status':'invalid_symbol_not_requested'} if symbol(m['ticker']) is None else {'endpoint':endpoint(m['ticker']),'status':'not_attempted_runtime_rate_or_size_limit'} for m in selected]
    stock_started=time.monotonic()
    def acquisition_remaining():return min(remaining(),180-(time.monotonic()-stock_started))
    workers=4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0,len(selected),workers):
            if acquisition_remaining()<60 or sources.bytes>=96*1024*1024 or sources.stop.is_set():break
            jobs=[]
            for i in range(offset,min(offset+workers,len(selected))):
                if selected[i]['ticker']=='SPY':attempts[i]=benchmark_attempt
                else:jobs.append((i,pool.submit(_leaders_fetch,selected[i]['ticker'],sources,acquisition_remaining)))
            for i,future in jobs:attempts[i]=future.result()
    finished=datetime.now(timezone.utc).isoformat()
    research=build(captures,attempts,sources.raw,finished,MAX_UNIVERSE,benchmark_attempt)
    if selected and not any(r['observations']['status'] in ('parsed_completed_observations','reported_empty_history','unresolved_identity_or_window','no_completed_observations') for r in research['request_records']):
        raise ValueError('No received market observations; preserve current')
    prior_ref=_leaders_archive(previous_raw) if previous_raw is not None else None
    packet={'engine':'justhodl-momentum-leaders','version':'2.0.0','schema_version':'2.0','measurement_contract':CONTRACT,
        'status':'RESEARCH_ONLY','method':'composite_selected_dated_price_observations','generated_at':finished,'acquisition_started_at':checked.isoformat(),
        'source_files':_leaders_source_identity(),'input_acquisitions':captures,'previous_publication':prior_ref,
        'acquisition_limits':{'MAX_UNIVERSE':60,'LOOKBACK_DAYS':90,'request_calendar_span':290,'original_configured_workers':8,
                              'active_workers':workers,'stock_acquisition_budget_seconds':120,'publication_budget_seconds':240},
        'retained_unique_source_bytes':sources.bytes,**research,'momentum_research_exclusion':momentum_exclusion(),
        'leaders':[],'pump_confirmed':[],'all_scored':[],'n_scored':None,'n_leaders':None,'n_pump_confirmed':None,
        'lookback_days':LOOKBACK_DAYS,'metadata':{'universe_size':len(selected),'universe_sources':list(INPUTS),
            'spy_perf_20d':None,'spy_perf_60d':None,'scoring_weights':{},'tag_definitions':{}},
        'call':None,**FLAGS,'signals_logged':0,'notifications_sent':0,'elapsed_sec':round(time.monotonic()-started,2),
        'source_documentation':['https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-full'],
        'caveats':['Every received source is retained whole, with dated coordinates, original selected-universe ancestry and failed/unattempted outcomes.',
            'The original 290-calendar-day request cannot establish a 52-week high. Available-window maxima carry actual dates, counts and scope.',
            'Missing benchmark endpoints and price windows remain unavailable. Zero historical volume remains in the denominator; no warm-container price cache is reused.',
            'No composite score, percentile leadership, pump confirmation, call, position size or execution authority is produced.',
            'Corporate actions, currency, exchange calendars, total returns, first-release vintages, historical membership and forecast performance remain unverified.']}
    raw=encode(packet)
    if len(raw)>64*1024*1024 or remaining()<15:raise ValueError('Whole publication exceeds reserve; preserve current')
    archive=_leaders_archive(raw);precondition={'IfMatch':etag} if etag else {'IfNoneMatch':'*'}
    s3.put_object(Bucket=S3_BUCKET,Key=HEAD,Body=raw,ContentType='application/json',CacheControl='public, max-age=600',**precondition)
    return {'statusCode':200,'body':json.dumps({'measurement_contract':CONTRACT,'request_occurrences':len(selected),'archive':archive})}
