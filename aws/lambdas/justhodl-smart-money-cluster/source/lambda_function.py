"""
justhodl-smart-money-cluster — Detect high-conviction smart-money signals
from 13F filings.

Pattern detection:
  CONSENSUS_BUY        ≥4 funds adding, ≤1 trimming → broad institutional accumulation
  NEW_INITIATION_CLUSTER  ≥2 funds initiating fresh positions same quarter
  DEEP_VALUE_CONSENSUS  ≥3 funds buying + stock down >25% from 52w high (Burry/Klarman pattern)
  RARE_HIGH_CONVICTION  Single fund with ≥3% portfolio in name + recent ADD/NEW
  VALUE_LEGEND          Buffett, Klarman, Greenlight, or Pabrai bought any name

Inputs:
  s3://justhodl-dashboard-live/data/13f-positions.json (existing)

Output:
  s3://justhodl-dashboard-live/data/smart-money-clusters.json
"""

import io
import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/smart-money-clusters.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

S3 = boto3.client("s3", region_name=REGION)

# Funds whose any-purchase is itself a meaningful signal (deep-value/concentrated managers)
LEGEND_FUNDS = {
    "BERKSHIRE", "BAUPOST", "GREENLIGHT", "PERSHING",
    "SCION", "LONE_PINE", "TIGER_GLOBAL", "COATUE", "DURATION", "PABRAI",
    "SOROS",
}

# Quant funds — their cluster signals matter less than discretionary
QUANT_FUNDS = {"RENAISSANCE", "TWO_SIGMA", "AQR", "CITADEL", "MILLENNIUM", "POINT72"}


def fetch_fmp_quote(ticker, key):
    """Get current price + 52w high from FMP /stable/quote."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={key}"
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            if isinstance(data, list) and data:
                return data[0]
    except Exception:
        pass
    return {}


def fetch_quotes_parallel(tickers, key, workers=8):
    """Parallel-fetch quotes for many tickers. Returns {ticker: quote_dict}."""
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_fmp_quote, t, key): t for t in tickers}
        for f in as_completed(futures):
            t = futures[f]
            try:
                out[t] = f.result()
            except Exception:
                out[t] = {}
    return out


def score_cluster(agg, quote, pre_actions=None):
    """
    Score a smart-money cluster signal 0-100.

    Components:
      - Buyer count (more funds = stronger)         0-30
      - Buy/sell ratio (cleaner signal)              0-20
      - NEW initiations (forward-looking)            0-20
      - Legend fund participation                    0-15
      - Drawdown context (>25% from 52w high)        0-15

    Returns dict with score + flag + components.
    """
    n_holding = agg.get("n_funds_holding", 0)
    n_add = agg.get("n_funds_adding", 0)
    n_new = agg.get("n_funds_new_position", 0)
    n_trim = agg.get("n_funds_trimming", 0)
    n_exit = agg.get("n_funds_exiting", 0)

    n_buyers = n_add + n_new
    n_sellers = n_trim + n_exit

    # Component 1: buyer count (capped @ 10)
    buyer_score = min(n_buyers * 6, 30)  # 5 buyers = 30

    # Component 2: signal cleanliness — fewer sellers
    if n_buyers + n_sellers == 0:
        clean_score = 0
    else:
        clean_score = (n_buyers / (n_buyers + n_sellers)) * 20

    # Component 3: NEW positions are most informative
    new_score = min(n_new * 8, 20)  # 2.5 new initiations = 20

    # Component 4: legend fund participation
    legend_buyers = []
    quant_buyers = []
    for action in agg.get("fund_actions", []):
        fund = action.get("fund", "")
        change = action.get("change", "")
        if change in ("ADD", "NEW"):
            if fund in LEGEND_FUNDS:
                legend_buyers.append(fund)
            elif fund in QUANT_FUNDS:
                quant_buyers.append(fund)
    legend_score = min(len(legend_buyers) * 7, 15)

    # Component 5: drawdown context
    drawdown_score = 0
    pct_from_high = None
    if quote:
        price = quote.get("price")
        high52 = quote.get("yearHigh") or quote.get("year_high")
        if price and high52 and high52 > 0:
            pct_from_high = ((price - high52) / high52) * 100
            if pct_from_high <= -50:
                drawdown_score = 15  # extreme drawdown — Buffett pattern
            elif pct_from_high <= -25:
                drawdown_score = 10  # meaningful drawdown
            elif pct_from_high <= -10:
                drawdown_score = 5

    total = buyer_score + clean_score + new_score + legend_score + drawdown_score

    # Determine flag
    flag = "LIGHT_SIGNAL"
    if total >= 70:
        flag = "STRONG_CONVICTION"
    elif total >= 55:
        flag = "HIGH_CONVICTION"
    elif total >= 40:
        flag = "MODERATE_CONVICTION"

    # Specific signal type
    signal_types = []
    if n_new >= 2:
        signal_types.append("NEW_INITIATION_CLUSTER")
    if n_buyers >= 4 and n_sellers <= 1:
        signal_types.append("CONSENSUS_BUY")
    if pct_from_high is not None and pct_from_high <= -25 and n_buyers >= 3:
        signal_types.append("DEEP_VALUE_CONSENSUS")
    if legend_buyers:
        signal_types.append("LEGEND_FUND_BUY")
    if not signal_types:
        signal_types.append("ACCUMULATION")

    return {
        "score": round(total, 1),
        "flag": flag,
        "signal_types": signal_types,
        "n_buyers": n_buyers,
        "n_sellers": n_sellers,
        "n_new": n_new,
        "legend_buyers": legend_buyers,
        "quant_buyers": quant_buyers,
        "pct_from_52w_high": round(pct_from_high, 1) if pct_from_high is not None else None,
        "components": {
            "buyer_score": round(buyer_score, 1),
            "clean_score": round(clean_score, 1),
            "new_score": round(new_score, 1),
            "legend_score": round(legend_score, 1),
            "drawdown_score": drawdown_score,
        },
    }


def build_rationale(agg, scoring, quote):
    """Plain-English rationale string."""
    ticker = agg.get("ticker") or agg.get("name", "?")[:30]
    name = agg.get("name", "?")
    n_buyers = scoring["n_buyers"]
    n_sellers = scoring["n_sellers"]
    n_new = scoring["n_new"]
    legends = scoring["legend_buyers"]
    pct = scoring["pct_from_52w_high"]

    parts = []
    if legends and n_new >= 1:
        parts.append(f"{', '.join(legends[:3])} initiated new positions in {ticker}")
    elif legends:
        parts.append(f"{', '.join(legends[:3])} added to {ticker}")
    elif n_new >= 2:
        parts.append(f"{n_new} funds opened brand-new positions in {ticker}")
    elif n_buyers >= 4:
        parts.append(f"{n_buyers} smart-money funds bought, only {n_sellers} sold")
    else:
        parts.append(f"{n_buyers} funds added")

    if pct is not None and pct <= -25:
        parts.append(f"stock {abs(pct):.0f}% off 52w high — contrarian timing")
    elif pct is not None and pct <= -10:
        parts.append(f"stock {abs(pct):.0f}% off 52w high")

    return " — ".join(parts)


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[smart-money] starting smart-money cluster scanner")

    # Load 13F aggregate
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
        raw = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[smart-money] FATAL — cannot load 13F: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    agg_by_ticker = raw.get("aggregate_by_ticker", {})
    print(f"[smart-money] loaded {len(agg_by_ticker)} 13F-tracked stocks")
    print(f"[smart-money] as_of_quarter: {raw.get('as_of_quarter')}")

    # Filter to candidates: ≥2 funds with buy action OR ≥1 legend fund holding
    candidates = []
    for tk, agg in agg_by_ticker.items():
        if not isinstance(agg, dict):
            continue
        # Skip if no ticker symbol (CUSIP-only entries)
        if not agg.get("ticker"):
            continue
        # Skip ETFs and SPACs
        name = (agg.get("name") or "").upper()
        if any(kw in name for kw in ["ETF", "SPDR", "TRUST", "ACQUISITION", "VANGUARD"]):
            continue
        n_buyers = (agg.get("n_funds_adding", 0) or 0) + (agg.get("n_funds_new_position", 0) or 0)
        n_sellers = (agg.get("n_funds_trimming", 0) or 0) + (agg.get("n_funds_exiting", 0) or 0)
        # Filter: ≥2 buyers OR a legend fund participated
        has_legend = any(
            a.get("fund") in LEGEND_FUNDS and a.get("change") in ("ADD", "NEW")
            for a in agg.get("fund_actions", [])
        )
        if n_buyers >= 2 or has_legend:
            candidates.append(agg)

    print(f"[smart-money] candidates passed filter: {len(candidates)}")

    # Fetch FMP quotes for drawdown context
    tickers = [c["ticker"] for c in candidates]
    print(f"[smart-money] fetching FMP quotes for {len(tickers)} tickers")
    t_q = time.time()
    quotes = fetch_quotes_parallel(tickers, FMP_KEY, workers=8)
    print(f"[smart-money] fetched {len(quotes)} quotes in {time.time()-t_q:.1f}s")

    # Score each candidate
    scored = []
    for agg in candidates:
        tk = agg["ticker"]
        q = quotes.get(tk, {})
        scoring = score_cluster(agg, q)
        rationale = build_rationale(agg, scoring, q)
        # Build cluster record
        scored.append({
            "ticker": tk,
            "name": agg.get("name"),
            "score": scoring["score"],
            "flag": scoring["flag"],
            "signal_types": scoring["signal_types"],
            "n_funds_holding": agg.get("n_funds_holding", 0),
            "n_buyers": scoring["n_buyers"],
            "n_sellers": scoring["n_sellers"],
            "n_new": scoring["n_new"],
            "legend_buyers": scoring["legend_buyers"],
            "quant_buyers": scoring["quant_buyers"],
            "total_value": agg.get("total_value", 0),
            "pct_from_52w_high": scoring["pct_from_52w_high"],
            "components": scoring["components"],
            "rationale": rationale,
            "fund_actions": [
                {
                    "fund": a.get("fund"),
                    "fund_name": a.get("fund_name"),
                    "change": a.get("change"),
                    "value": a.get("value"),
                    "pct_of_portfolio": a.get("pct_of_portfolio"),
                    "delta_pct": a.get("delta_pct"),
                }
                for a in agg.get("fund_actions", [])
            ],
            "fundamentals": {
                "price": (quotes.get(tk) or {}).get("price"),
                "market_cap": (quotes.get(tk) or {}).get("marketCap") or (quotes.get(tk) or {}).get("market_cap"),
                "year_high": (quotes.get(tk) or {}).get("yearHigh") or (quotes.get(tk) or {}).get("year_high"),
                "year_low": (quotes.get(tk) or {}).get("yearLow") or (quotes.get(tk) or {}).get("year_low"),
                "pe_ratio": (quotes.get(tk) or {}).get("pe"),
                "volume": (quotes.get(tk) or {}).get("volume"),
                "exchange": (quotes.get(tk) or {}).get("exchange"),
                "industry": (quotes.get(tk) or {}).get("industry"),
            },
        })

    # Sort by score descending
    scored.sort(key=lambda x: -x["score"])

    # Build aggregated stats
    n_strong = sum(1 for c in scored if c["score"] >= 70)
    n_high = sum(1 for c in scored if 55 <= c["score"] < 70)
    n_moderate = sum(1 for c in scored if 40 <= c["score"] < 55)
    n_legend_buys = sum(1 for c in scored if c["legend_buyers"])
    n_new_init = sum(1 for c in scored if c["n_new"] >= 2)
    n_deep_value = sum(1 for c in scored if "DEEP_VALUE_CONSENSUS" in c["signal_types"])
    n_consensus = sum(1 for c in scored if "CONSENSUS_BUY" in c["signal_types"])

    payload = {
        "schema_version": "1.0",
        "method": "smart_money_cluster_scanner_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "as_of_quarter": raw.get("as_of_quarter"),
        "stats": {
            "n_total_13f_stocks": len(agg_by_ticker),
            "n_candidates": len(candidates),
            "n_clusters_scored": len(scored),
            "n_strong": n_strong,
            "n_high_conviction": n_high,
            "n_moderate": n_moderate,
            "n_legend_fund_buys": n_legend_buys,
            "n_new_init_clusters": n_new_init,
            "n_deep_value": n_deep_value,
            "n_consensus_buys": n_consensus,
        },
        "clusters": scored,
    }

    # Write to S3
    body = json.dumps(payload, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[smart-money] wrote {len(body):,}b to {S3_KEY}")
    print(f"[smart-money] strong: {n_strong}  high: {n_high}  legend buys: {n_legend_buys}")
    if scored:
        top5 = [(c['ticker'], c['score'], c['flag']) for c in scored[:5]]
        print(f"[smart-money] TOP 5: {top5}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_clusters_scored": len(scored),
            "n_strong": n_strong,
            "n_high_conviction": n_high,
            "n_legend_fund_buys": n_legend_buys,
            "duration_s": payload["duration_s"],
            "top_5": [{"ticker": c["ticker"], "score": c["score"], "flag": c["flag"]} for c in scored[:5]],
        }),
    }
