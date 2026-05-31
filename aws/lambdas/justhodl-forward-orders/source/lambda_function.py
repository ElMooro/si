"""justhodl-forward-orders — institutional forward-revenue intelligence.

PROBLEM
═══════
Backward-looking fundamentals miss the fact that a company may have ALREADY
booked $20B in orders for the next 3 years. That backlog is contractually
locked-in revenue that the stock price hasn't priced in yet.

This is institutional alpha — hedge funds track:
  - RPO (Remaining Performance Obligations) from 10-Q footnotes
  - Order backlog from MD&A discussion
  - Book-to-bill ratio from earnings calls
  - Major contract announcements ($X over Y years)

WHAT WE SCORE (0-100 per ticker)
════════════════════════════════
  1. RPO YIELD               (40%): RPO / Market Cap — how many years of locked revenue
                                       at current valuation
  2. RPO GROWTH              (30%): YoY growth in RPO — accelerating backlog = future pump
  3. CONTRACT ANNOUNCEMENTS  (20%): $-weighted recent press releases mentioning
                                       "signs contract", "awarded", "selected by"
  4. BOOK-TO-BILL PROXY      (10%): RPO growth vs Revenue growth — if backlog grows
                                       faster than revenue, future quarters will accelerate

DATA SOURCES
════════════
  - SEC EDGAR companyfacts (free, structured XBRL)
    https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json
  - NewsAPI (we have the key) for contract announcement scanning
  - FMP for market cap + revenue context

OUTPUT
══════
  data/forward-orders.json — per-ticker scores, rationale, raw RPO data
  Top-20 leaderboard for system-health.html
"""

import json
import os
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/forward-orders.json"

NEWS_API_KEY = "17d36cdd13c44e139853b3a6876cf940"
FMP_KEY      = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# XBRL tag candidates (varies by industry; first found wins)
RPO_TAGS = [
    "RevenueRemainingPerformanceObligation",          # primary GAAP
    "RemainingPerformanceObligation",
    "ContractWithCustomerLiabilityCurrent",
    "OrderOrProductionBacklog",                       # defense, aerospace
    "ContractRevenueRemainingDeferred",
]
REVENUE_TAGS = [
    "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
]

# Universe: top US large/mid-caps. Pull from FMP screener.
UNIVERSE_MIN_MCAP    = 1_000_000_000  # $1B
UNIVERSE_MAX_RESULTS = 250

# News scanning
CONTRACT_QUERY_TEMPLATE = (
    '"{ticker}" AND ("signs contract" OR "awarded contract" OR "$" '
    'OR "selected to" OR "wins contract" OR "announces deal")'
)
CONTRACT_LOOKBACK_DAYS = 90
CONTRACT_MIN_USD = 100_000_000  # only count contracts ≥ $100M

HTTP_TIMEOUT = 15
USER_AGENT = "JustHodlForwardOrders/1.0 (raafouis@gmail.com)"

s3 = boto3.client("s3", region_name=REGION)


# ─── HTTP helpers ───────────────────────────────────────────────────────

def _get_json(url, headers=None, timeout=HTTP_TIMEOUT):
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:200]}


# ─── SEC: ticker → CIK + XBRL ────────────────────────────────────────────

_cik_cache = {}

def get_cik_for_ticker(ticker: str) -> str:
    """SEC publishes ticker → CIK at company_tickers.json. Cache once."""
    global _cik_cache
    if not _cik_cache:
        data = _get_json("https://www.sec.gov/files/company_tickers.json")
        if isinstance(data, dict) and "_err" not in data:
            for v in data.values():
                if isinstance(v, dict):
                    _cik_cache[v.get("ticker", "").upper()] = str(v.get("cik_str", "")).zfill(10)
    return _cik_cache.get(ticker.upper())


def get_xbrl_facts(cik: str) -> dict:
    """Fetch full companyfacts for a CIK."""
    if not cik:
        return {}
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    data = _get_json(url)
    if "_err" in data:
        return {}
    return data


def extract_rpo_series(facts: dict) -> list:
    """Find the most recent + historical RPO values across candidate tags.
    Returns list of {value, end, fy, fp} sorted by end-date desc."""
    if not facts or "facts" not in facts:
        return []
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    
    # Try tags in priority order
    for tag in RPO_TAGS:
        node = us_gaap.get(tag)
        if not node:
            continue
        units = node.get("units", {})
        usd_units = units.get("USD") or []
        if not usd_units:
            continue
        # Keep only annual + most recent quarterly
        cleaned = []
        for u in usd_units:
            if "end" in u and "val" in u:
                cleaned.append({
                    "value": u["val"],
                    "end":   u["end"],
                    "fy":    u.get("fy"),
                    "fp":    u.get("fp"),
                    "form":  u.get("form"),
                    "tag":   tag,
                })
        cleaned.sort(key=lambda r: r["end"], reverse=True)
        if cleaned:
            return cleaned
    return []


def extract_revenue_series(facts: dict) -> list:
    """Find recent annual revenue. Used for RPO/Revenue ratio."""
    if not facts:
        return []
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in REVENUE_TAGS:
        node = us_gaap.get(tag)
        if not node:
            continue
        units = node.get("units", {}).get("USD") or []
        # Keep only annual (full-year) entries
        annual = []
        for u in units:
            if u.get("fp") == "FY" or (u.get("form", "").startswith("10-K")):
                if "end" in u and "val" in u:
                    annual.append({
                        "value": u["val"],
                        "end":   u["end"],
                        "fy":    u.get("fy"),
                    })
        annual.sort(key=lambda r: r["end"], reverse=True)
        # Dedupe by fy
        seen = set()
        dedup = []
        for r in annual:
            if r["fy"] not in seen:
                seen.add(r["fy"])
                dedup.append(r)
        return dedup[:5]
    return []


# ─── FMP: universe + market cap ──────────────────────────────────────────

def fmp_get(path, **params):
    base = f"https://financialmodelingprep.com/stable/{path}"
    qp = urllib.parse.urlencode({**params, "apikey": FMP_KEY})
    data = _get_json(f"{base}?{qp}")
    if isinstance(data, dict) and "_err" in data:
        return None
    return data


def get_universe():
    """Top US large/mid-caps. Returns list of {symbol, marketCap, sector}.
    
    Filters out foreign-listed dual-tickers (e.g. LMT.BA) which would
    otherwise eat budget without producing alpha (SEC XBRL is US-only)."""
    res = fmp_get("company-screener",
                    marketCapMoreThan=UNIVERSE_MIN_MCAP,
                    isActivelyTrading="true",
                    country="US",
                    exchange="NYSE,NASDAQ",
                    limit=UNIVERSE_MAX_RESULTS)
    if not res:
        return []
    return [
        {
            "symbol":    r.get("symbol"),
            "name":      r.get("companyName"),
            "marketCap": r.get("marketCap"),
            "sector":    r.get("sector"),
            "industry":  r.get("industry"),
        }
        for r in res
        if r.get("symbol") and r.get("marketCap") and "." not in r.get("symbol")
    ]


# ─── News: contract announcement scanning ────────────────────────────────

_dollar_re = re.compile(
    r"\$([0-9]+(?:\.[0-9]+)?)\s*([BbMm])(?:illion)?", re.IGNORECASE,
)

def parse_dollar_amount(text: str) -> int:
    """Extract the largest $ amount mentioned. Returns USD or None."""
    if not text:
        return None
    matches = _dollar_re.findall(text)
    best = 0
    for amt, unit in matches:
        try:
            v = float(amt)
        except ValueError:
            continue
        if unit.upper() == "B":
            v *= 1_000_000_000
        elif unit.upper() == "M":
            v *= 1_000_000
        if v > best:
            best = int(v)
    return best if best > 0 else None


def scan_contracts_for(ticker: str, name: str) -> dict:
    """Find contract-announcement news in last CONTRACT_LOOKBACK_DAYS days.
    Returns total $ + count + sample headlines."""
    # Use company name for better matching (NewsAPI doesn't tokenize tickers well)
    q = (name or ticker) + ' AND ("signs contract" OR "wins contract" '\
        'OR "awarded contract" OR "selected to" OR "announces" OR "deal")'
    since = (datetime.now(timezone.utc) - timedelta(days=CONTRACT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    
    qp = urllib.parse.urlencode({
        "q":         q,
        "from":      since,
        "language":  "en",
        "sortBy":    "publishedAt",
        "pageSize":  20,
        "apiKey":    NEWS_API_KEY,
    })
    data = _get_json(f"https://newsapi.org/v2/everything?{qp}")
    if not isinstance(data, dict) or data.get("status") != "ok":
        return {"n": 0, "total_usd": 0, "headlines": [], "err": data.get("message", "")[:120]}
    
    headlines_with_money = []
    total_usd = 0
    
    for art in data.get("articles") or []:
        title = art.get("title") or ""
        desc = art.get("description") or ""
        combined = f"{title}. {desc}"
        amt = parse_dollar_amount(combined)
        if amt and amt >= CONTRACT_MIN_USD:
            total_usd += amt
            headlines_with_money.append({
                "amount_usd": amt,
                "title":      title[:140],
                "source":     (art.get("source") or {}).get("name"),
                "publishedAt": art.get("publishedAt"),
                "url":        art.get("url"),
            })
    
    return {
        "n":         len(headlines_with_money),
        "total_usd": total_usd,
        "headlines": sorted(headlines_with_money, key=lambda h: -h["amount_usd"])[:5],
    }


# ─── Scoring ────────────────────────────────────────────────────────────

def score_rpo_yield(rpo_value, market_cap) -> float:
    """RPO / Market Cap — higher = more locked future revenue per $ of equity."""
    if not rpo_value or not market_cap or market_cap <= 0:
        return 0
    ratio = rpo_value / market_cap
    # Calibration: RPO yield > 1.0 (100% of mcap in future revenue) is exceptional
    # 0.5+ is very strong, 0.2-0.5 is good, <0.1 is weak
    if ratio >= 1.0:   return 100
    if ratio >= 0.75:  return 90
    if ratio >= 0.50:  return 80
    if ratio >= 0.30:  return 65
    if ratio >= 0.15:  return 45
    if ratio >= 0.05:  return 25
    return 10


def score_rpo_growth(rpo_series) -> tuple:
    """YoY growth in RPO. Need at least 2 annual-ish data points."""
    if not rpo_series or len(rpo_series) < 2:
        return 0, None
    
    # Find latest + ~year-ago entries
    latest = rpo_series[0]
    year_ago = None
    latest_end = datetime.fromisoformat(latest["end"]).date()
    for r in rpo_series[1:]:
        rd = datetime.fromisoformat(r["end"]).date()
        days_diff = (latest_end - rd).days
        if 300 <= days_diff <= 420:
            year_ago = r
            break
    if not year_ago:
        # Fallback: closest entry > 200 days back
        for r in rpo_series[1:]:
            rd = datetime.fromisoformat(r["end"]).date()
            if (latest_end - rd).days >= 200:
                year_ago = r
                break
    if not year_ago or not year_ago["value"]:
        return 0, None
    
    growth_pct = (latest["value"] - year_ago["value"]) / abs(year_ago["value"]) * 100
    
    # Score: 50%+ YoY growth = exceptional; -20% = bad
    if growth_pct >= 50:  s = 100
    elif growth_pct >= 30: s = 85
    elif growth_pct >= 15: s = 70
    elif growth_pct >= 5:  s = 50
    elif growth_pct >= 0:  s = 30
    elif growth_pct >= -10: s = 15
    else: s = 0
    return s, round(growth_pct, 1)


def score_contracts(contracts, market_cap) -> float:
    """Recent contract win value as fraction of market cap."""
    if not contracts or not market_cap:
        return 0
    total = contracts.get("total_usd", 0)
    ratio = total / market_cap
    if ratio >= 0.25:  return 100  # contracts worth 25%+ of mcap is huge
    if ratio >= 0.10:  return 80
    if ratio >= 0.05:  return 65
    if ratio >= 0.02:  return 45
    if ratio >= 0.01:  return 25
    if total > 0:      return 10
    return 0


def score_book_to_bill(rpo_growth_pct, revenue_series) -> tuple:
    """Compare RPO growth to revenue growth. If RPO growing faster than
    revenue, future quarters will accelerate."""
    if rpo_growth_pct is None or not revenue_series or len(revenue_series) < 2:
        return 0, None
    
    rev_now = revenue_series[0].get("value")
    rev_prior = revenue_series[1].get("value")
    if not rev_now or not rev_prior:
        return 0, None
    
    rev_growth = (rev_now - rev_prior) / abs(rev_prior) * 100
    spread = rpo_growth_pct - rev_growth  # positive = RPO outpacing revenue
    
    if spread >= 20:   s = 100
    elif spread >= 10: s = 80
    elif spread >= 5:  s = 60
    elif spread >= 0:  s = 40
    elif spread >= -5: s = 25
    else: s = 10
    return s, round(spread, 1)


WEIGHTS = {
    "rpo_yield":          0.30,    # was 0.40
    "rpo_growth":         0.25,    # was 0.30
    "rpo_acceleration":   0.15,    # NEW — multi-quarter QoQ trend
    "contracts":          0.15,    # was 0.20
    "book_to_bill":       0.10,    # was 0.10
    "peer_percentile":    0.05,    # NEW — vs industry median
}


def compute_rpo_acceleration(rpo_series) -> tuple:
    """Multi-quarter QoQ growth trend. Detects acceleration vs flat/decel.
    Returns (score 0-100, qoq_change_pct, n_quarters_used).
    
    With 4 most-recent quarters (Q-3, Q-2, Q-1, Q-0):
      qoq_recent = (Q0 - Q-1) / Q-1
      qoq_prior  = (Q-1 - Q-2) / Q-2
      acceleration = qoq_recent - qoq_prior
    
    Positive acceleration = backlog growing FASTER than prior quarter.
    This is the strongest forward signal — order book momentum is increasing."""
    if not rpo_series or len(rpo_series) < 4:
        return 0, None, 0
    
    # Use first 4 entries (most recent)
    vals = [r["value"] for r in rpo_series[:4] if r.get("value")]
    if len(vals) < 4:
        return 0, None, len(vals)
    
    q0, q1, q2, q3 = vals[0], vals[1], vals[2], vals[3]
    if not all([q0, q1, q2, q3]) or q1 <= 0 or q2 <= 0:
        return 0, None, len(vals)
    
    qoq_recent = (q0 - q1) / abs(q1) * 100
    qoq_prior  = (q1 - q2) / abs(q2) * 100
    acceleration_pp = qoq_recent - qoq_prior
    
    # Score: +10pp acceleration is exceptional
    if acceleration_pp >= 15:   s = 100
    elif acceleration_pp >= 8:  s = 85
    elif acceleration_pp >= 3:  s = 70
    elif acceleration_pp >= 0:  s = 50
    elif acceleration_pp >= -5: s = 30
    elif acceleration_pp >= -10: s = 15
    else: s = 0
    return s, round(acceleration_pp, 1), len(vals)


# Module-level peer cache populated mid-run for percentile scoring
_peer_yields_by_sector = {}

def compute_peer_percentile(sector, my_yield_pct) -> tuple:
    """Score = percentile rank of this ticker's RPO yield within its sector.
    Sector-relative analysis: a 30% RPO yield is exceptional for industrials
    but middling for SaaS (Oracle at 85%). Use sector median as benchmark."""
    if not sector or not my_yield_pct:
        return 50, None  # neutral if can't compare
    
    peer_yields = _peer_yields_by_sector.get(sector, [])
    if len(peer_yields) < 3:
        return 50, None  # not enough peers
    
    sorted_peers = sorted(peer_yields)
    n = len(sorted_peers)
    # Find rank of my_yield
    rank = sum(1 for p in sorted_peers if p < my_yield_pct)
    percentile = round(rank / n * 100, 1)
    
    # Score: 80th-percentile peer = 90 score
    score = min(100, percentile * 1.1)
    return score, percentile



def analyze_ticker(stock):
    """Run all sub-scores for one ticker."""
    ticker = stock["symbol"]
    mcap = stock.get("marketCap")
    
    cik = get_cik_for_ticker(ticker)
    facts = get_xbrl_facts(cik) if cik else {}
    rpo_series = extract_rpo_series(facts)
    rev_series = extract_revenue_series(facts)
    
    if not rpo_series:
        # No RPO disclosed — skip (most retail-tech doesn't but software, defense,
        # aerospace, EVs typically do)
        return None
    
    rpo_latest = rpo_series[0]["value"]
    s_yield = score_rpo_yield(rpo_latest, mcap)
    s_growth, growth_pct = score_rpo_growth(rpo_series)
    
    # NEW v3: multi-quarter acceleration
    s_accel, accel_pp, n_quarters = compute_rpo_acceleration(rpo_series)
    
    contracts = scan_contracts_for(ticker, stock.get("name"))
    s_contracts = score_contracts(contracts, mcap)
    
    s_b2b, spread = score_book_to_bill(growth_pct, rev_series)
    
    # NEW v3: peer-relative percentile (sector benchmark)
    my_yield_pct = (rpo_latest / mcap * 100) if mcap else None
    s_peer, peer_pct = compute_peer_percentile(stock.get("sector"), my_yield_pct)
    
    composite = (
        s_yield     * WEIGHTS["rpo_yield"] +
        s_growth    * WEIGHTS["rpo_growth"] +
        s_accel     * WEIGHTS["rpo_acceleration"] +
        s_contracts * WEIGHTS["contracts"] +
        s_b2b       * WEIGHTS["book_to_bill"] +
        s_peer      * WEIGHTS["peer_percentile"]
    )
    
    return {
        "ticker":         ticker,
        "name":           stock.get("name"),
        "sector":         stock.get("sector"),
        "industry":       stock.get("industry"),
        "market_cap":     mcap,
        "composite":      round(composite, 1),
        "subscores": {
            "rpo_yield":         round(s_yield, 1),
            "rpo_growth":        round(s_growth, 1),
            "rpo_acceleration":  round(s_accel, 1),
            "contracts":         round(s_contracts, 1),
            "book_to_bill":      round(s_b2b, 1),
            "peer_percentile":   round(s_peer, 1),
        },
        "data": {
            "rpo_latest_usd":           rpo_latest,
            "rpo_yield_pct":            round(my_yield_pct, 1) if my_yield_pct else None,
            "rpo_growth_yoy_pct":       growth_pct,
            "rpo_qoq_acceleration_pp":  accel_pp,
            "rpo_quarters_available":   n_quarters,
            "book_to_bill_spread_pct":  spread,
            "peer_percentile":          peer_pct,
            "rpo_tag":                  rpo_series[0].get("tag"),
            "rpo_as_of":                rpo_series[0].get("end"),
            "rpo_history":              rpo_series[:6],
            "revenue_history":          rev_series[:3],
        },
        "contracts": contracts,
        "thesis":  _build_thesis(s_yield, s_growth, s_contracts, s_b2b,
                                   rpo_latest, mcap, growth_pct, contracts,
                                   accel_pp, peer_pct),
    }


def _build_thesis(s_yield, s_growth, s_contracts, s_b2b,
                    rpo, mcap, growth_pct, contracts, accel_pp=None, peer_pct=None):
    bits = []
    if s_yield >= 70:
        bits.append(f"RPO ${rpo/1e9:.1f}B = {rpo/mcap*100:.0f}% of mcap")
    if s_growth >= 70 and growth_pct:
        bits.append(f"RPO growing {growth_pct:+.0f}% YoY")
    if accel_pp is not None and accel_pp >= 5:
        bits.append(f"acceleration +{accel_pp:.0f}pp QoQ")
    if s_contracts >= 65:
        bits.append(f"${contracts.get('total_usd',0)/1e9:.1f}B in 90d contract wins")
    if s_b2b >= 70:
        bits.append("RPO outpacing revenue → future accel")
    if peer_pct is not None and peer_pct >= 80:
        bits.append(f"top-{100-int(peer_pct)}% peer")
    if not bits:
        return "Moderate forward-orders signal"
    return " · ".join(bits)


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # ─── 1. Build universe ──────────────────────────────────────────────
    print("[fwd-orders] fetching universe…")
    universe = get_universe()
    print(f"[fwd-orders] universe size: {len(universe)} tickers")
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no universe"})}
    
    # ─── 2. Pre-warm CIK cache ──────────────────────────────────────────
    print("[fwd-orders] pre-warming SEC ticker→CIK cache…")
    get_cik_for_ticker("AAPL")  # forces the cache fill
    print(f"[fwd-orders] CIK cache loaded: {len(_cik_cache)} entries")
    
    # ─── 3. Analyze each ticker (single pass — peer percentile is iterative) ─
    # SEC rate limit is 10 requests/sec — pace ourselves
    results = []
    for i, stock in enumerate(universe):
        try:
            r = analyze_ticker(stock)
            if r and r["composite"] >= 20:  # only keep meaningful scores
                results.append(r)
                # As we score, accumulate yields per sector so subsequent
                # tickers benefit from the peer-comparison signal
                sector = stock.get("sector")
                yp = r["data"].get("rpo_yield_pct")
                if sector and yp:
                    _peer_yields_by_sector.setdefault(sector, []).append(yp)
        except Exception as e:
            print(f"[fwd-orders] err on {stock['symbol']}: {e}")
        # Rate-limit pacing for SEC
        if (i + 1) % 8 == 0:
            time.sleep(1.0)
        if (i + 1) % 25 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            print(f"[fwd-orders] processed {i+1}/{len(universe)}  found {len(results)}  "
                  f"elapsed {elapsed:.0f}s")
        # Defensive timeout
        if (datetime.now(timezone.utc) - started).total_seconds() > 720:  # 12 min cap
            print("[fwd-orders] time budget exhausted, stopping early")
            break
    
    # Second pass: re-score peer percentile now that we have full sector populations.
    # Only re-runs the peer dimension (cheap; no network calls).
    for r in results:
        sector = r.get("sector")
        yp = r["data"].get("rpo_yield_pct")
        if sector and yp:
            s_peer, peer_pct = compute_peer_percentile(sector, yp)
            # Update subscore + composite
            old_s_peer = r["subscores"]["peer_percentile"]
            r["subscores"]["peer_percentile"] = round(s_peer, 1)
            r["data"]["peer_percentile"] = peer_pct
            # Recompute composite (since we changed one of the 6 subscores)
            ss = r["subscores"]
            r["composite"] = round(
                ss["rpo_yield"] * WEIGHTS["rpo_yield"] +
                ss["rpo_growth"] * WEIGHTS["rpo_growth"] +
                ss["rpo_acceleration"] * WEIGHTS["rpo_acceleration"] +
                ss["contracts"] * WEIGHTS["contracts"] +
                ss["book_to_bill"] * WEIGHTS["book_to_bill"] +
                ss["peer_percentile"] * WEIGHTS["peer_percentile"],
                1,
            )
    
    # Re-sort
    results.sort(key=lambda r: -r["composite"])
    
    # ─── 4. Emit composite output ───────────────────────────────────────
    out = {
        "schema_version":     "3.0",
        "method":             "forward_orders_v3",
        "generated_at":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":         round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "n_universe":         len(universe),
        "n_with_rpo":         len(results),
        "weights":            WEIGHTS,
        "top_25_by_score":    results[:25],
        "all_results":        results,
        "notes": (
            "RPO = Remaining Performance Obligations (locked future revenue). "
            "RPO Yield = RPO/MarketCap. Book-to-bill spread = RPO growth - Revenue growth. "
            "Sectors most likely to disclose RPO: software (Salesforce), defense, "
            "aerospace, construction, EV-makers, telecom."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[fwd-orders] wrote {len(body):,}B to {OUTPUT_KEY}")
    
    # ─── 5. Emit events for top conviction ──────────────────────────────
    try:
        from system_events import publish_many
        top_conviction = [r for r in results if r["composite"] >= 75][:5]
        if top_conviction:
            events_to_pub = [
                ("forward_orders.high_conviction", {
                    "ticker":        r["ticker"],
                    "composite":     r["composite"],
                    "rpo_yield_pct": r["data"]["rpo_yield_pct"],
                    "rpo_growth_pct": r["data"]["rpo_growth_yoy_pct"],
                    "thesis":        r["thesis"],
                })
                for r in top_conviction
            ]
            publish_many(events_to_pub)
    except Exception as e:
        print(f"[fwd-orders] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_with_rpo": len(results),
        "top_score": results[0]["composite"] if results else None,
        "top_ticker": results[0]["ticker"] if results else None,
        "duration_s": out["duration_s"],
    })}


lambda_handler = handler
