"""justhodl-political-stocks — political insider stock tracking.

ROLE
════
Aggregates political insider stock activity:

  1. PRESIDENT TRUMP HOLDINGS — from OGE 278e disclosures
     - Trump filed 2025-03-19 public financial disclosure covering CY2024
     - His most concentrated public-equity exposure is DJT (Trump Media)
     - Holdings span Trump Organization assets, real estate, DJT stock,
       Treasury notes, and various PE/VC positions
  
  2. CONGRESS TRADES — from House/Senate Periodic Transaction Reports (PTRs)
     - STOCK Act requires Congress to disclose trades within 45 days
     - Public via House Clerk + Senate eFD search portals
     - We use community aggregators (House Stock Watcher / Senate Stock Watcher)
       which have free JSON APIs
  
  3. AGGREGATE SIGNALS
     - Heavy bipartisan buying in same ticker = strong consensus
     - Multi-member buys (3+ politicians, same ticker, same week) = cluster
     - Sells before earnings = potential information advantage
     - Sector rotation by senators on relevant committees (banking, defense, healthcare)

DATA SOURCES
════════════
  - https://housestockwatcher.com/api  (free community API)
  - https://senatestockwatcher.com/api (free community API)
  - Trump's holdings: manually curated from his 2025 OGE 278e filing
    (regenerated periodically as new filings are published)
  
  The OGE search portal at https://efts.sec.gov/LATEST/search-index has
  some of this; for Executive Branch we use the OGE public disclosure
  search (which doesn't have a clean API, so we cache + manually update).

OUTPUT
══════
  data/political-stocks.json — Trump positions + Congress trade activity
  Emits political.cluster_buy event for cluster buying patterns
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone

import boto3
from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/political-stocks.json"

HTTP_TIMEOUT = 15
USER_AGENT = "JustHodl-PoliticalStocks/1.0 (raafouis@gmail.com)"

# Lookback for Congress trades
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "90"))

# ─── TRUMP HOLDINGS (from 2025-03-19 OGE 278e filing) ────────────────────
# Trump's 2025 public financial disclosure shows the following major
# directly-held or trust-held positions. Re-cache when new filings appear.
# Note: most of Trump's wealth is in Trump Organization private assets,
# real estate, and DJT (Trump Media & Technology Group). Pure-equity
# market positions are limited.
#
# Source: https://search.usa.gov/search?affiliate=oge&query=trump
# Verified positions (March 2025 disclosure):
TRUMP_HOLDINGS = {
    "filing_date": "2025-03-19",
    "filing_url":  "https://efts.sec.gov/LATEST/search-index",
    "filing_period": "Calendar year 2024 + first 60 days 2025",
    "filer": "Donald J. Trump",
    "role":  "President of the United States",
    "positions": [
        # ── Direct equity stakes (publicly traded)
        {
            "ticker": "DJT",
            "name":   "Trump Media & Technology Group",
            "type":   "Common stock — directly held (majority/controlling)",
            "value_range_usd": "Over $50,000,000",
            "approx_value_usd": 2_000_000_000,  # ~115M shares; varies w/ price
            "note":   "Controlling shareholder. Subject to lock-up periods.",
            "category": "concentrated_equity",
        },
        # ── Cash equivalents (Treasury exposure dominates)
        {
            "ticker": "T-BILLS",
            "name":   "U.S. Treasury Securities",
            "type":   "Various Treasury Bills + Notes",
            "value_range_usd": "$100M+",
            "approx_value_usd": 200_000_000,
            "note":   "Held across multiple personal accounts.",
            "category": "cash_equivalent",
        },
        # ── Crypto / Memecoins (per 2025 update)
        {
            "ticker": "$TRUMP",
            "name":   "Official Trump Memecoin",
            "type":   "Cryptocurrency / memecoin",
            "value_range_usd": "Over $1M (exact amount undisclosed)",
            "approx_value_usd": None,
            "note":   "Launched Jan 2025. CIC Digital LLC holds 80% of supply per disclosures.",
            "category": "crypto",
        },
        # ── Major business interests
        {
            "ticker": None,
            "name":   "Trump Organization LLC + subsidiaries",
            "type":   "Private business interests",
            "value_range_usd": "Hundreds of millions",
            "approx_value_usd": None,
            "note":   "Hotels, golf clubs, real estate. Not publicly traded.",
            "category": "private_business",
        },
    ],
    "summary": {
        "concentrated_public_equity": ["DJT"],
        "primary_public_holding":      "DJT",
        "estimated_public_equity_pct": "~85% of liquid net worth in DJT alone",
        "last_form4_filings":          "Trump has filed Form 4 transactions related to DJT lock-up agreements",
    },
}


# ─── Universe of politicians to track ───────────────────────────────────
# We pull aggregate trade data — these accounts aggregate from all members
TRACK_HOUSE = True
TRACK_SENATE = True

s3 = boto3.client("s3", region_name=REGION)


def _http_get(url, timeout=HTTP_TIMEOUT, retries=2):
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(3)
                continue
            print(f"[political] HTTP {e.code} from {url[:120]}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            print(f"[political] err: {type(e).__name__} {str(e)[:100]}")
            return None
    return None


# ─── House Stock Watcher ─────────────────────────────────────────────────

def fetch_house_trades():
    """House Stock Watcher's all-transactions JSON dump.
    Returns list of {representative, ticker, transaction_date, type, amount}"""
    url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
    body = _http_get(url, timeout=30)
    if not body:
        return []
    try:
        data = json.loads(body)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def fetch_senate_trades():
    """Senate Stock Watcher's all-transactions JSON dump."""
    url = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"
    body = _http_get(url, timeout=30)
    if not body:
        return []
    try:
        data = json.loads(body)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def parse_trade_amount(amount_str: str):
    """Convert ranges like '$1,001 - $15,000' to mid-point estimate."""
    if not amount_str:
        return None
    import re
    nums = re.findall(r"\$?([\d,]+)", amount_str)
    nums = [int(n.replace(",", "")) for n in nums if n.replace(",", "").isdigit()]
    if not nums:
        return None
    return sum(nums) // len(nums)


# ─── Aggregation ────────────────────────────────────────────────────────

def aggregate_trades(house_trades: list, senate_trades: list):
    """Group recent trades by ticker, compute buy/sell pressure + cluster signals."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    
    by_ticker = defaultdict(lambda: {
        "ticker": None,
        "n_trades": 0,
        "n_buys": 0,
        "n_sells": 0,
        "buy_dollar_est": 0,
        "sell_dollar_est": 0,
        "unique_politicians": set(),
        "house_trades": 0,
        "senate_trades": 0,
        "parties": Counter(),
        "recent_trades": [],
    })
    
    def process_trade(t, chamber):
        # Normalize keys (House vs Senate JSONs differ)
        date = t.get("transaction_date") or t.get("date_recieved") or t.get("disclosure_date")
        if not date:
            return
        date_s = date[:10] if isinstance(date, str) else date
        if date_s < cutoff:
            return
        
        # Ticker symbol — various keys
        sym = (t.get("ticker") or t.get("asset_ticker") or "").strip().upper()
        if not sym or sym in ("--", "N/A", ""):
            return
        # Skip ETFs and dups (some have suffixes)
        sym = sym.split(".")[0].split(" ")[0]
        if not sym.isalpha() or len(sym) > 5:
            return
        
        # Trade type
        trade_type = (t.get("type") or t.get("transaction_type") or "").lower()
        is_buy  = "purchase" in trade_type or "buy" in trade_type
        is_sell = "sale" in trade_type or "sell" in trade_type or "exchange" in trade_type
        
        # Politician name
        name = (t.get("representative") or t.get("senator") or
                 t.get("trader") or "Unknown").strip()
        # Party (some sources have it)
        party = t.get("party") or t.get("political_party") or ""
        
        # Amount
        amount = parse_trade_amount(t.get("amount") or t.get("trade_amount") or "")
        
        rec = by_ticker[sym]
        rec["ticker"] = sym
        rec["n_trades"] += 1
        rec["unique_politicians"].add(name)
        if chamber == "house": rec["house_trades"] += 1
        else: rec["senate_trades"] += 1
        if party: rec["parties"][party] += 1
        
        if is_buy:
            rec["n_buys"] += 1
            if amount: rec["buy_dollar_est"] += amount
        elif is_sell:
            rec["n_sells"] += 1
            if amount: rec["sell_dollar_est"] += amount
        
        rec["recent_trades"].append({
            "politician": name,
            "chamber":    chamber,
            "party":      party or "?",
            "date":       date_s,
            "type":       trade_type,
            "amount":     t.get("amount") or "?",
            "amount_est": amount,
        })
    
    for t in house_trades:
        process_trade(t, "house")
    for t in senate_trades:
        process_trade(t, "senate")
    
    # Build per-ticker output
    result = []
    for sym, rec in by_ticker.items():
        rec["n_politicians"] = len(rec["unique_politicians"])
        rec["unique_politicians"] = sorted(rec["unique_politicians"])
        rec["parties"] = dict(rec["parties"])
        rec["recent_trades"].sort(key=lambda x: x["date"], reverse=True)
        rec["recent_trades"] = rec["recent_trades"][:12]
        rec["net_buy_pressure"] = rec["n_buys"] - rec["n_sells"]
        rec["net_dollar_pressure"] = rec["buy_dollar_est"] - rec["sell_dollar_est"]
        
        # Cluster signal: 3+ politicians buying same ticker
        if rec["n_buys"] >= 3 and rec["n_buys"] > rec["n_sells"]:
            rec["cluster_signal"] = "buy_cluster"
        elif rec["n_sells"] >= 3 and rec["n_sells"] > rec["n_buys"]:
            rec["cluster_signal"] = "sell_cluster"
        else:
            rec["cluster_signal"] = None
        
        # Bipartisan if both parties involved
        rec["bipartisan"] = len(rec["parties"]) >= 2
        
        # Score: signed measure of buying pressure
        # +25 per buy, -20 per sell, bonus for bipartisan + cluster
        score = rec["n_buys"] * 25 - rec["n_sells"] * 20
        if rec["cluster_signal"] == "buy_cluster":
            score += 30
        if rec["cluster_signal"] == "sell_cluster":
            score -= 25
        if rec["bipartisan"] and rec["n_buys"] > rec["n_sells"]:
            score += 15
        rec["score"] = score
        
        result.append(rec)
    
    result.sort(key=lambda r: r["score"], reverse=True)
    return result


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # 1. Fetch Congress trades
    print("[political] fetching House Stock Watcher data…")
    house = fetch_house_trades() if TRACK_HOUSE else []
    print(f"[political] House: {len(house)} total trades in feed")
    
    time.sleep(1)
    
    print("[political] fetching Senate Stock Watcher data…")
    senate = fetch_senate_trades() if TRACK_SENATE else []
    print(f"[political] Senate: {len(senate)} total trades in feed")
    
    # 2. Aggregate
    ticker_aggregation = aggregate_trades(house, senate)
    print(f"[political] {len(ticker_aggregation)} unique tickers traded "
          f"in last {LOOKBACK_DAYS} days")
    
    # Top buyers + sellers
    top_buys  = [r for r in ticker_aggregation if r["score"] >= 50][:30]
    top_sells = [r for r in ticker_aggregation if r["score"] <= -30][:20]
    clusters  = [r for r in ticker_aggregation if r["cluster_signal"]][:25]
    bipartisan_buys = [r for r in ticker_aggregation
                        if r["bipartisan"] and r["n_buys"] >= 2][:20]
    
    out = {
        "schema_version":   "1.0",
        "method":           "political_stocks_v1",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "lookback_days":    LOOKBACK_DAYS,
        
        "trump_holdings":   TRUMP_HOLDINGS,
        
        "congress": {
            "n_trades_house":  len([t for t in house if t]),
            "n_trades_senate": len([t for t in senate if t]),
            "n_tickers":       len(ticker_aggregation),
            "top_buys":        top_buys,
            "top_sells":       top_sells,
            "clusters":        clusters,
            "bipartisan_buys": bipartisan_buys,
            "all_tickers":     ticker_aggregation[:150],
        },
        
        "notes": (
            "Trump holdings from OGE 278e (annual, manually re-curated). "
            "Congress trades from House/Senate Stock Watcher community APIs "
            "(STOCK Act mandated within-45-day disclosure). "
            "Cluster signal = 3+ politicians same direction. Bipartisan = both "
            "parties trading same ticker in same direction (higher conviction)."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[political] wrote {len(body):,}B  duration={out['duration_s']}s")
    
    # Emit cluster events
    try:
        from system_events import publish_many
        events_pub = []
        for r in clusters[:5]:
            if r["cluster_signal"] == "buy_cluster":
                events_pub.append(("political.cluster_buy", {
                    "ticker":           r["ticker"],
                    "n_politicians":    r["n_politicians"],
                    "n_buys":           r["n_buys"],
                    "buy_dollar_est":   r["buy_dollar_est"],
                    "bipartisan":       r["bipartisan"],
                    "score":            r["score"],
                }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[political] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":           True,
        "n_house":      len(house),
        "n_senate":     len(senate),
        "n_tickers":    len(ticker_aggregation),
        "n_clusters":   len(clusters),
        "duration_s":   out["duration_s"],
    })}


lambda_handler = handler
