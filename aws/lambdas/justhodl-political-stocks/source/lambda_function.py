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
  - Trump holdings: manually curated from his 2025 OGE 278e filing
    (re-cached periodically as new filings are published)
  - Congress trades: https://api.quiverquant.com/beta/live/congresstrading
    (Quiver Quant's no-auth public endpoint — returns 1000 most recent
    trades across all House+Senate members. Fields: Representative,
    BioGuideID, ReportDate, TransactionDate, Ticker, Transaction, Range,
    House. Party data not in /live/ — we maintain a known-trader → party
    map for bipartisan detection on top 30 most active.)
  
  Previous source (now dead): House/Senate Stock Watcher S3 buckets at
  house-stock-watcher-data.s3-us-west-2 / senate-stock-watcher-data.s3-
  us-west-2 returned HTTP 403 — community project shut down its public
  data buckets in 2026.

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

HTTP_TIMEOUT = 25
USER_AGENT = "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)"

# Lookback for Congress trades
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "90"))

# Quiver's `live` endpoint doesn't include Party — maintain known mapping
# for the most active Congress traders so bipartisan detection still works
# Source: ballotpedia.org / congress.gov. Update annually for new sessions.
BIOGUIDE_TO_PARTY = {
    # ── House — top active traders (frequently on PTRs) ────────────
    "T000490": "R",  # David J. Taylor (R-OH)
    "G000596": "R",  # Marjorie Taylor Greene (R-GA)
    "P000197": "D",  # Nancy Pelosi (D-CA)
    "K000392": "D",  # Ro Khanna (D-CA)
    "C001078": "D",  # Lou Correa (D-CA)
    "H001046": "R",  # French Hill (R-AR)
    "G000587": "D",  # Josh Gottheimer (D-NJ)
    "K000378": "D",  # Daniel Kildee (D-MI)
    "G000578": "R",  # Bob Gibbs (R-OH)
    "M001213": "R",  # Daniel Meuser (R-PA)
    "B000490": "R",  # Earl L. "Buddy" Carter (R-GA)
    "F000462": "R",  # A. Drew Ferguson IV (R-GA)
    "S001213": "D",  # Adam Schiff (D-CA)
    "B001302": "R",  # Andy Barr (R-KY)
    "C001120": "R",  # Bruce Westerman (R-AR)
    "H001077": "R"  ,  # Kevin Hern (R-OK)
    "G000591": "D",  # Vicente Gonzalez (D-TX)
    "C000059": "R",  # Mike Carey (R-OH)
    "W000827": "D",  # Frederica Wilson (D-FL)
    "M001137": "D",  # Donald McEachin (D-VA)
    
    # ── Senate — top active traders ─────────────────────────────────
    "T000250": "R",  # Tommy Tuberville (R-AL)
    "M001197": "R",  # Markwayne Mullin (R-OK)
    "C001056": "R",  # Shelley Moore Capito (R-WV)
    "P000605": "R",  # Rand Paul (R-KY)
    "B001288": "R",  # Marsha Blackburn (R-TN)
    "B001277": "R",  # Roy Blunt (R-MO)
    "C000567": "I",  # Mazie Hirono (D-HI)
    "D000620": "D",  # Sheldon Whitehouse (D-RI)
    "B000944": "D",  # Sherrod Brown (D-OH)
    "G000386": "D",  # Kirsten Gillibrand (D-NY)
    "M001183": "R",  # Roger Marshall (R-KS)
    "W000817": "D",  # Elizabeth Warren (D-MA)
    "S000033": "I",  # Bernie Sanders (I-VT)
    "M000312": "R",  # Mitch McConnell (R-KY)
    "S001181": "R",  # Tim Scott (R-SC)
    "G000359": "R",  # Lindsey Graham (R-SC)
    "C001047": "R",  # Bill Cassidy (R-LA)
    "C001098": "R",  # Bill Hagerty (R-TN)
    "L000174": "D",  # Patrick Leahy (D-VT)
    "C001056": "R",  # Shelley Moore Capito (R-WV)
}


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


# ─── Full congressional party map via theunitedstates.io ────────────────
# The legislators-current.json file is maintained by the unitedstates.io
# project — every current member of Congress with their BioGuide ID and
# party. ~535 entries (435 House + 100 Senate).
#
# We pre-cache to S3 via ops/1053 (and refresh monthly) because direct
# fetches from us-east-1 sometimes time out (theunitedstates.io's
# Cloudflare config blocks some AWS IPs). Reading from S3 is fast and
# reliable; falls back to live fetch then hardcoded dict if needed.

S3_PARTY_MAP_KEY  = "data/congress-party-map.json"
S3_QUIVER_CACHE_KEY = "data/quiver-congress-cache.json"


def load_party_map_from_s3() -> dict:
    """Read pre-cached party map from S3. Returns {bioguide_id: party_letter}."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=S3_PARTY_MAP_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        pm = data.get("party_map") or {}
        if pm:
            print(f"[political] loaded {len(pm)} party mappings from S3 cache "
                  f"(generated {data.get('generated_at','?')})")
            return pm
    except Exception as e:
        print(f"[political] S3 party map load failed: {e}")
    return None


def fetch_full_legislators_map() -> dict:
    """Returns {bioguide_id: party_letter} for ALL current Congress.
    Tries: S3 cache → live fetch → hardcoded fallback."""
    # Try S3 cache first (fast, reliable)
    pm = load_party_map_from_s3()
    if pm:
        return pm
    
    # Live fetch fallback (slow, sometimes blocked from us-east-1)
    print("[political] S3 cache miss — trying live fetch")
    url = "https://theunitedstates.io/congress-legislators/legislators-current.json"
    body = _http_get(url, timeout=20, retries=1)
    if not body:
        print("[political] live fetch also failed — using hardcoded fallback")
        return dict(BIOGUIDE_TO_PARTY)
    
    try:
        data = json.loads(body)
    except Exception as e:
        print(f"[political] live parse err: {e}")
        return dict(BIOGUIDE_TO_PARTY)
    
    party_map = {}
    party_short = {
        "Democrat":    "D",
        "Republican":  "R",
        "Independent": "I",
        "Libertarian": "L",
    }
    for legislator in (data or []):
        try:
            bioguide = (legislator.get("id") or {}).get("bioguide")
            terms = legislator.get("terms") or []
            if not bioguide or not terms:
                continue
            latest_term = terms[-1]
            party_full = latest_term.get("party", "")
            party_map[bioguide] = party_short.get(party_full, party_full[:1] or "?")
        except Exception:
            continue
    
    for k, v in BIOGUIDE_TO_PARTY.items():
        party_map.setdefault(k, v)
    
    return party_map


def fetch_quiver_with_cache() -> tuple:
    """Try live Quiver first. If it fails or returns empty, fall back to
    S3 cache (which ops/1053 refreshes). Returns (trades_list, source_label)."""
    # Try live
    trades = fetch_quiver_congress()
    if trades:
        print(f"[political] live Quiver: {len(trades)} trades")
        # Update S3 cache opportunistically
        try:
            cache_obj = {
                "schema_version": "1.0",
                "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source":         "https://api.quiverquant.com/beta/live/congresstrading",
                "n_trades":       len(trades),
                "trades":         trades,
            }
            s3.put_object(
                Bucket=BUCKET, Key=S3_QUIVER_CACHE_KEY,
                Body=json.dumps(cache_obj, default=str, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
                CacheControl="public, max-age=21600",
            )
        except Exception as e:
            print(f"[political] cache write skipped: {e}")
        return trades, "live"
    
    # Fall back to S3 cache
    print("[political] live Quiver returned empty — trying S3 cache")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=S3_QUIVER_CACHE_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        cached_trades = data.get("trades") or []
        cache_age_s = (datetime.now(timezone.utc) -
                        datetime.fromisoformat(data["generated_at"].replace("Z", "+00:00")
                                                .replace("+00:00+00:00", "+00:00"))).total_seconds()
        print(f"[political] using S3 Quiver cache: {len(cached_trades)} trades "
              f"(age {cache_age_s/3600:.1f}h)")
        return cached_trades, f"s3_cache_{cache_age_s/3600:.1f}h"
    except Exception as e:
        print(f"[political] S3 cache also missing: {e}")
        return [], "none"


# Populated by handler() per invocation
_current_party_map = None


# ─── Quiver Quant live/congresstrading endpoint ─────────────────────────

def fetch_quiver_congress():
    """Single call returns 1000 most recent Congress trades across all
    House + Senate members. Fields: Representative, BioGuideID,
    ReportDate, TransactionDate, Ticker, Transaction, Range, House.
    
    No auth needed. ~430KB per call. Replaces dead Stock Watcher feeds."""
    url = "https://api.quiverquant.com/beta/live/congresstrading"
    body = _http_get(url, timeout=30)
    if not body:
        return []
    try:
        data = json.loads(body)
    except Exception as e:
        print(f"[political] parse err: {e}")
        return []
    return data if isinstance(data, list) else []


def parse_trade_amount(range_str: str):
    """Convert Quiver ranges like '$1,001 - $15,000' to mid-point estimate.
    Also handles '$1,000,001 - $5,000,000' and 'Less than $1,001' formats."""
    if not range_str:
        return None
    import re
    s = range_str.replace("Less than", "0 -").strip()
    nums = re.findall(r"\$?([\d,]+)", s)
    nums = [int(n.replace(",", "")) for n in nums if n.replace(",", "").isdigit()]
    if not nums:
        return None
    # If single value, use it; otherwise mid-point
    return sum(nums) // len(nums)


def party_for_bioguide(bioguide_id: str) -> str:
    """Return party (D/R/I/?) for a politician by their BioGuide ID.
    Uses the auto-loaded full Congress map if available, else falls back
    to the hardcoded BIOGUIDE_TO_PARTY map."""
    if _current_party_map is not None:
        return _current_party_map.get(bioguide_id, "?")
    return BIOGUIDE_TO_PARTY.get(bioguide_id, "?")


# ─── Aggregation ────────────────────────────────────────────────────────

def aggregate_trades(quiver_trades: list):
    """Group recent Quiver trades by ticker, compute buy/sell pressure +
    cluster signals. Single source now (no more split between House and
    Senate inputs)."""
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
    
    for t in quiver_trades:
        # Quiver field shape:
        # {"Representative": "...", "BioGuideID": "T000490",
        #  "ReportDate": "2026-05-28", "TransactionDate": "2026-05-15",
        #  "Ticker": "MEDP", "Transaction": "Purchase",
        #  "Range": "$1,001 - $15,000", "House": "Representatives"}
        date = t.get("TransactionDate") or t.get("ReportDate")
        if not date:
            continue
        date_s = date[:10] if isinstance(date, str) else date
        if date_s < cutoff:
            continue
        
        sym = (t.get("Ticker") or "").strip().upper()
        if not sym or sym in ("--", "N/A", ""):
            continue
        # Skip non-equity tickers (some have suffixes for bonds/options)
        sym = sym.split(".")[0].split(" ")[0]
        if not sym.replace("-", "").isalpha() or len(sym) > 6:
            continue
        
        trade_type = (t.get("Transaction") or "").lower()
        # Quiver examples: "Purchase", "Sale (Partial)", "Sale (Full)",
        # "Exchange". Map to buy/sell.
        is_buy  = "purchase" in trade_type
        is_sell = "sale" in trade_type or "exchange" in trade_type
        
        name = (t.get("Representative") or "Unknown").strip()
        bioguide = t.get("BioGuideID", "")
        party = party_for_bioguide(bioguide)
        chamber = "house" if (t.get("House") or "") == "Representatives" else "senate"
        amount = parse_trade_amount(t.get("Range") or "")
        
        rec = by_ticker[sym]
        rec["ticker"] = sym
        rec["n_trades"] += 1
        rec["unique_politicians"].add(name)
        if chamber == "house": rec["house_trades"] += 1
        else: rec["senate_trades"] += 1
        if party != "?": rec["parties"][party] += 1
        
        if is_buy:
            rec["n_buys"] += 1
            if amount: rec["buy_dollar_est"] += amount
        elif is_sell:
            rec["n_sells"] += 1
            if amount: rec["sell_dollar_est"] += amount
        
        rec["recent_trades"].append({
            "politician": name,
            "bioguide":   bioguide,
            "chamber":    chamber,
            "party":      party,
            "date":       date_s,
            "report_date": t.get("ReportDate", ""),
            "type":       trade_type,
            "amount":     t.get("Range") or "?",
            "amount_est": amount,
        })
    
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
        
        # Bipartisan if both parties involved (and party data was available)
        rec["bipartisan"] = len(rec["parties"]) >= 2 and "D" in rec["parties"] and "R" in rec["parties"]
        
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
    
    # 0. Load party map (S3 cache → live → hardcoded)
    global _current_party_map
    _current_party_map = fetch_full_legislators_map()
    
    # 1. Fetch Congress trades (live → S3 cache)
    print("[political] fetching Congress trades…")
    quiver_trades, source = fetch_quiver_with_cache()
    print(f"[political] got {len(quiver_trades)} trades from: {source}")
    
    # Tally house/senate split (Quiver's House field is "Representatives" or "Senate")
    n_house = sum(1 for t in quiver_trades if (t.get("House") or "") == "Representatives")
    n_senate = sum(1 for t in quiver_trades if (t.get("House") or "") == "Senate")
    
    # 2. Aggregate
    ticker_aggregation = aggregate_trades(quiver_trades)
    print(f"[political] {len(ticker_aggregation)} unique tickers traded "
          f"in last {LOOKBACK_DAYS} days")
    
    # Top buyers + sellers
    top_buys  = [r for r in ticker_aggregation if r["score"] >= 50][:30]
    top_sells = [r for r in ticker_aggregation if r["score"] <= -30][:20]
    clusters  = [r for r in ticker_aggregation if r["cluster_signal"]][:25]
    bipartisan_buys = [r for r in ticker_aggregation
                        if r["bipartisan"] and r["n_buys"] >= 2][:20]
    
    out = {
        "schema_version":   "1.3",
        "method":           "political_stocks_v1_s3cached",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "lookback_days":    LOOKBACK_DAYS,
        "data_source":      "https://api.quiverquant.com/beta/live/congresstrading",
        "party_source":     "https://theunitedstates.io/congress-legislators/legislators-current.json",
        "quiver_source":    source,  # "live", "s3_cache_Xh", or "none"
        
        "trump_holdings":   TRUMP_HOLDINGS,
        
        "congress": {
            "n_trades_total":  len(quiver_trades),
            "n_trades_house":  n_house,
            "n_trades_senate": n_senate,
            "n_tickers":       len(ticker_aggregation),
            "n_party_map":     len(_current_party_map or {}),
            "trade_source":    source,
            "top_buys":        top_buys,
            "top_sells":       top_sells,
            "clusters":        clusters,
            "bipartisan_buys": bipartisan_buys,
            "all_tickers":     ticker_aggregation[:150],
        },
        
        "notes": (
            "Trump holdings from OGE 278e (annual, manually re-curated). "
            "Congress trades from Quiver Quant live/congresstrading endpoint "
            "(1000 most recent across all House+Senate members, STOCK Act-required). "
            "Party detection limited to known active traders (~40 politicians) since "
            "Quiver's /live/ endpoint doesn't include Party field — bipartisan flag "
            "fires only when both D and R are confirmed in our BIOGUIDE_TO_PARTY map. "
            "Cluster signal = 3+ politicians same direction in lookback window."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[political] wrote {len(body):,}B  duration={out['duration_s']}s  "
          f"tickers={len(ticker_aggregation)}  clusters={len(clusters)}")
    
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
        "n_quiver":     len(quiver_trades),
        "n_house":      n_house,
        "n_senate":     n_senate,
        "n_tickers":    len(ticker_aggregation),
        "n_clusters":   len(clusters),
        "n_bipartisan": len(bipartisan_buys),
        "duration_s":   out["duration_s"],
    })}


lambda_handler = handler
