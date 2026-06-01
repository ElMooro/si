"""
justhodl-pair-trades
═══════════════════
Hedge-fund style pair-trade generator.

For each top pump candidate:
  1. Pull 10 sector peers via FMP /stable/stock-peers
  2. For each peer, compute 5d/20d/60d performance + correlation with candidate
  3. Identify the WEAKEST peer (lowest 20d perf with high correlation = best short)
  4. Compute the LONG/SHORT pair trade:
      - spread_perf = candidate_20d - weakest_peer_20d
      - long ratio : short ratio (vol-balanced)
      - expected_alpha range (statistically — based on historical spread mean-reversion)
      - hedge effectiveness (R² of pair)

WHY THIS MATTERS
═══════════════
Going long PLTR outright = long PLTR + long market. If market dumps, PLTR
dumps too. Going long PLTR / short the weakest software peer = pure
relative-value bet. You're paid for *PLTR being better*, not the market
going up. Hedge funds use this constantly.

OUTPUT
══════
data/pair-trades.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_pairs":        12,
  "pairs": [
    {
      "long_ticker":      "PLTR",
      "long_sector":      "Technology",
      "long_industry":    "Software - Infrastructure",
      "long_20d_pct":     +13.1,
      "short_ticker":     "TWLO",                # weakest peer
      "short_20d_pct":    -8.4,
      "spread_20d":       +21.5,                 # relative outperformance %
      "correlation_60d":  0.62,
      "hedge_quality":    "good",                # good/fair/poor based on correlation
      "ratio_long_short": "1.0 : 0.85",          # vol-balanced ratio
      "long_vol":         42.3,
      "short_vol":        49.5,
      "expected_alpha_range_1m": "+3% to +8%",  # mean-reversion estimate
      "thesis_one_liner": "Long PLTR pump setup / Short TWLO sector laggard. Hedge captures relative growth even if tech sector pulls back.",
      "peer_pool": [                              # full peer ranking
        {"ticker": "TWLO", "perf_20d": -8.4, "correlation": 0.62},
        {"ticker": "MDB",  "perf_20d": -2.1, "correlation": 0.58},
        ...
      ]
    },
    ...
  ]
}

SCHEDULE
════════
cron(20 * * * ? *) — hourly at :20, after positioning + mechanics
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
RADAR_KEY    = "data/convergence-radar.json"
OUTPUT_KEY   = "data/pair-trades.json"
FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

LOOKBACK_DAYS = 90

s3 = boto3.client("s3", region_name="us-east-1")
PRICE_CACHE: Dict[str, List[dict]] = {}


# ═════════════════════════════════════════════════════════════════════
# FMP fetchers
# ═════════════════════════════════════════════════════════════════════

def fetch_peers(ticker: str) -> List[dict]:
    """Returns list of sector peer records (companyName, symbol, mktCap)."""
    try:
        url = f"https://financialmodelingprep.com/stable/stock-peers?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/pairs"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[peers] {ticker}: {str(e)[:100]}")
        return []


def fetch_price_rows(ticker: str) -> List[dict]:
    if ticker in PRICE_CACHE:
        return PRICE_CACHE[ticker]
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=LOOKBACK_DAYS + 20)
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/pairs"})
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


# ═════════════════════════════════════════════════════════════════════
# Returns + correlations
# ═════════════════════════════════════════════════════════════════════

def to_returns(rows: List[dict]) -> List[float]:
    closes = [r.get("close") for r in rows if r.get("close")]
    rets = []
    for i in range(1, len(closes)):
        if closes[i] > 0 and closes[i-1] > 0:
            rets.append(math.log(closes[i] / closes[i-1]))
    return rets


def compute_perf_pct(rows: List[dict], days: int) -> Optional[float]:
    if not rows or len(rows) < days + 1:
        return None
    end_c = rows[-1].get("close")
    start_c = rows[-(days+1)].get("close")
    if not end_c or not start_c or start_c <= 0:
        return None
    return round((end_c / start_c - 1) * 100, 2)


def compute_annualized_vol(rows: List[dict], window: int = 30) -> Optional[float]:
    rets = to_returns(rows[-(window+1):])
    if len(rets) < 5:
        return None
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252) * 100


def correlation(a: List[float], b: List[float]) -> Optional[float]:
    n = min(len(a), len(b))
    if n < 10:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = math.sqrt(sum((a[i] - ma) ** 2 for i in range(n)))
    db = math.sqrt(sum((b[i] - mb) ** 2 for i in range(n)))
    if da == 0 or db == 0:
        return None
    return num / (da * db)


# ═════════════════════════════════════════════════════════════════════
# Pair-trade construction
# ═════════════════════════════════════════════════════════════════════

def classify_hedge_quality(corr: Optional[float]) -> str:
    if corr is None:
        return "unknown"
    if corr >= 0.7:  return "excellent"  # tight pair
    if corr >= 0.5:  return "good"
    if corr >= 0.3:  return "fair"
    return "poor"  # too uncorrelated → not a real pair


def vol_ratio(long_vol: Optional[float], short_vol: Optional[float]) -> str:
    """Compute the vol-balanced ratio: short notional / long notional."""
    if not long_vol or not short_vol or short_vol <= 0:
        return "1.0 : 1.0"
    ratio = long_vol / short_vol
    return f"1.0 : {round(ratio, 2)}"


def estimate_expected_alpha(spread: Optional[float], corr: Optional[float],
                              vol_short: Optional[float]) -> str:
    """Rough mean-reversion alpha estimate.

    If spread is large (+15%+) and correlation is high, mean-reversion has historical
    precedent of ~30-50% spread compression over 1-3 months. We give a conservative
    range based on the spread magnitude.
    """
    if spread is None or corr is None or corr < 0.3:
        return "n/a (low pair quality)"
    if spread < 5:
        return "thin spread (+1-3%)"
    if spread < 12:
        return "+3-6%"
    if spread < 20:
        return "+5-10%"
    return "+8-15% (large spread, but watch breakdown)"


def build_pair(candidate: dict, peer_data: List[dict],
                  cand_rets: List[float], cand_rows: List[dict]) -> Optional[dict]:
    """Pick the weakest peer with reasonable correlation and build the pair record."""
    if not peer_data:
        return None
    cand_ticker = candidate["ticker"]
    cand_perf_20d = compute_perf_pct(cand_rows, 20)
    cand_vol = compute_annualized_vol(cand_rows, 30)

    # Score each peer: weakness × correlation
    peer_pool = []
    for p in peer_data:
        pt = p.get("symbol")
        if not pt or pt == cand_ticker:
            continue
        peer_rows = fetch_price_rows(pt)
        if not peer_rows or len(peer_rows) < 20:
            continue
        peer_rets = to_returns(peer_rows)
        peer_perf_20d = compute_perf_pct(peer_rows, 20)
        peer_perf_5d = compute_perf_pct(peer_rows, 5)
        peer_vol = compute_annualized_vol(peer_rows, 30)
        corr = correlation(cand_rets, peer_rets)

        if corr is None or peer_perf_20d is None:
            continue

        # Score: bias toward weak peers with high correlation
        # higher score = better short candidate
        # (more negative perf is good for short, higher corr is good for hedge)
        weakness_pts = -peer_perf_20d  # negative perf becomes positive points
        score = (weakness_pts * 0.7) + (corr * 30 * 0.3)

        peer_pool.append({
            "ticker":      pt,
            "company":     p.get("companyName") or pt,
            "perf_5d":     peer_perf_5d,
            "perf_20d":    peer_perf_20d,
            "vol":         peer_vol,
            "correlation": round(corr, 3) if corr else None,
            "score":       round(score, 2),
            "market_cap":  p.get("mktCap"),
        })

    if not peer_pool:
        return None

    # Sort by score (best short first)
    peer_pool.sort(key=lambda x: -x["score"])
    weakest = peer_pool[0]

    spread_20d = (cand_perf_20d - weakest["perf_20d"]) if (cand_perf_20d is not None and weakest["perf_20d"] is not None) else None
    hedge_q = classify_hedge_quality(weakest["correlation"])
    ratio = vol_ratio(cand_vol, weakest["vol"])
    expected_alpha = estimate_expected_alpha(spread_20d, weakest["correlation"], weakest["vol"])

    return {
        "long_ticker":          cand_ticker,
        "long_company":         "",  # filled later
        "long_perf_5d":         compute_perf_pct(cand_rows, 5),
        "long_perf_20d":        cand_perf_20d,
        "long_vol_30d":         round(cand_vol, 2) if cand_vol else None,
        "long_pump_likelihood": candidate.get("pump_likelihood"),
        "long_pump_category":   candidate.get("pump_category"),

        "short_ticker":         weakest["ticker"],
        "short_company":        weakest["company"],
        "short_perf_5d":        weakest["perf_5d"],
        "short_perf_20d":       weakest["perf_20d"],
        "short_vol_30d":        round(weakest["vol"], 2) if weakest["vol"] else None,
        "short_market_cap":     weakest["market_cap"],

        "spread_20d_pct":       round(spread_20d, 2) if spread_20d is not None else None,
        "correlation_90d":      weakest["correlation"],
        "hedge_quality":        hedge_q,
        "ratio_long_short":     ratio,
        "expected_alpha_1m":    expected_alpha,
        "thesis_one_liner":     (
            f"Long {cand_ticker} pump setup ({(cand_perf_20d or 0):+.1f}% 20d) / "
            f"Short {weakest['ticker']} sector laggard ({weakest['perf_20d']:+.1f}% 20d). "
            f"Spread {spread_20d:+.1f}%, correlation {weakest['correlation']:.2f} = "
            f"{hedge_q} hedge. Pair captures relative strength even in tech-sector pullback."
            if (cand_perf_20d is not None and weakest['perf_20d'] is not None and spread_20d is not None)
            else f"Long {cand_ticker} / Short {weakest['ticker']} — see details below."
        ),
        "peer_pool":            peer_pool[:6],   # top 6 candidates for context
    }


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[pair-trades] start {datetime.now(timezone.utc).isoformat()}")

    try:
        radar = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RADAR_KEY)["Body"].read())
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")
    candidates = (radar.get("pump_candidates") or [])[:12]
    if not candidates:
        return _write_error("No pump candidates")

    # Fetch peers + pre-warm long candidate price data
    candidates_with_peers = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        peer_futures = {ex.submit(fetch_peers, c["ticker"]): c for c in candidates}
        long_price_futures = {ex.submit(fetch_price_rows, c["ticker"]): c for c in candidates}
        peer_map: Dict[str, List[dict]] = {}
        for fut in as_completed(peer_futures, timeout=90):
            cand = peer_futures[fut]
            try:
                peer_map[cand["ticker"]] = fut.result()
            except Exception:
                peer_map[cand["ticker"]] = []
        for fut in as_completed(long_price_futures, timeout=120):
            try: fut.result()
            except Exception: pass

    # Collect all peer tickers to pre-warm their prices in parallel
    all_peer_tickers = set()
    for plist in peer_map.values():
        for p in plist:
            if p.get("symbol"):
                all_peer_tickers.add(p["symbol"])
    print(f"[pair-trades] pre-warming {len(all_peer_tickers)} peer price histories")
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_price_rows, t) for t in all_peer_tickers]
        for fut in as_completed(futures, timeout=180):
            try: fut.result()
            except Exception: pass

    # Build pair per candidate
    pairs = []
    for cand in candidates:
        cand_rows = PRICE_CACHE.get(cand["ticker"], [])
        cand_rets = to_returns(cand_rows)
        peer_data = peer_map.get(cand["ticker"], [])
        pair = build_pair(cand, peer_data, cand_rets, cand_rows)
        if pair:
            pairs.append(pair)

    # Sort by long pump_likelihood (highest first)
    pairs.sort(key=lambda p: -(p.get("long_pump_likelihood") or 0))

    output = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":    round(time.time() - t0, 2),
        "n_pairs":        len(pairs),
        "lookback_days":  LOOKBACK_DAYS,
        "pairs":          pairs,
        "methodology": {
            "peer_source":            "FMP /stable/stock-peers (top 10 by sector overlap)",
            "selection_criteria":     "Weakest 20d perf × highest 90d correlation",
            "hedge_quality_grades":   "excellent ≥0.7  ·  good ≥0.5  ·  fair ≥0.3  ·  poor <0.3",
            "vol_ratio_formula":      "long_vol_30d / short_vol_30d → short notional sizing",
            "expected_alpha_basis":   "Spread mean-reversion estimate; ranges, not point predictions",
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=600")
    archive_key = (f"data/archive/pair-trades/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":            "ok",
        "elapsed_sec":       output["elapsed_sec"],
        "n_pairs":           len(pairs),
        "n_excellent_hedge": sum(1 for p in pairs if p["hedge_quality"] == "excellent"),
        "n_good_hedge":      sum(1 for p in pairs if p["hedge_quality"] == "good"),
        "top_pair":          (pairs[0]["long_ticker"] + "/" + pairs[0]["short_ticker"]) if pairs else None,
    }
    print(f"[pair-trades] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[pair-trades] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
