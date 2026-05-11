"""
justhodl-ma-tracker — Mergers & Acquisitions Activity Tracker (v2)

Lambda with a Function URL. On invocation:
  1. Check S3 for screener/ma-latest.json
  2. If <1h old, return that
  3. Otherwise fetch fresh from FMP:
       - /stable/mergers-acquisitions-latest (5 pages × 100 = up to 500 deals)
  4. Enrich each deal with acquirer + target profile data (sector, mcap)
     using a small batch of profile lookups (cap at 50 unique tickers)
  5. Write to S3, return JSON

Cached + S3-backed so the page loads fast and we don't burn FMP quota.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

# ───── CONFIG ─────
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/ma-latest.json"
CACHE_TTL_SECONDS = 3600  # 1 hour

s3 = boto3.client("s3", region_name="us-east-1")

# In-memory cache (warm container reuse)
_MEMO = {"json": None, "ts": 0}


def fmp(path, params="", retries=2):
    """GET FMP endpoint with retries on transient errors."""
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-MA/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code} {path}"
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {last_err}: {e}")
            return None
        except Exception as e:
            last_err = str(e)[:120]
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path} err: {last_err}")
            return None
    return None


def load_s3_cache():
    """Try to load cached ma-latest.json from S3."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        body = obj["Body"].read()
        last_modified = obj["LastModified"]
        age_seconds = (datetime.now(timezone.utc) - last_modified).total_seconds()
        return json.loads(body), age_seconds
    except s3.exceptions.NoSuchKey:
        return None, None
    except Exception as e:
        print(f"[cache] load err: {e}")
        return None, None


def fetch_all_deals(max_pages=5):
    """Fetch M&A deals across multiple pages, dedupe + return list."""
    all_deals = []
    seen = set()
    for page in range(max_pages):
        data = fmp("mergers-acquisitions-latest", f"&page={page}")
        if not isinstance(data, list) or not data:
            break
        for d in data:
            # Dedupe by (acquirer cik, target cik, accepted_date) since same
            # deal can repeat across pages on edge dates
            key = (d.get("cik"), d.get("targetedCik"),
                     d.get("acceptedDate", "")[:10])
            if key in seen:
                continue
            seen.add(key)
            all_deals.append(d)
    return all_deals


def enrich_with_profiles(deals, max_lookups=50):
    """Fetch sector/industry/mcap for acquirers (cap to top by recency to
    keep latency low). Returns dict {symbol: profile} for use by page."""
    # Collect unique acquirer + target symbols, prefer acquirers
    symbols = []
    seen_sym = set()
    for d in deals:
        for sym_field in ("symbol", "targetedSymbol"):
            s = d.get(sym_field)
            if s and s not in seen_sym and len(s) <= 6:
                seen_sym.add(s)
                symbols.append(s)
                if len(symbols) >= max_lookups:
                    break
        if len(symbols) >= max_lookups:
            break

    profiles = {}
    def fetch_one(sym):
        data = fmp("profile", f"&symbol={sym}")
        if isinstance(data, list) and data:
            p = data[0]
            return sym, {
                "sector": p.get("sector"),
                "industry": p.get("industry"),
                "mcap": p.get("mktCap"),
                "exchange": p.get("exchangeShortName"),
                "name": p.get("companyName"),
                "image": p.get("image"),
            }
        return sym, None

    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, profile in ex.map(fetch_one, symbols):
            if profile:
                profiles[sym] = profile
    return profiles


def build_summary(deals, profiles):
    """Build aggregate stats for the page."""
    # Count by acquirer sector
    sector_counts = {}
    for d in deals:
        sym = d.get("symbol")
        p = profiles.get(sym) if sym else None
        sec = (p or {}).get("sector") or "Unknown"
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    # Top serial acquirers (most deals as acquirer)
    acq_counts = {}
    for d in deals:
        sym = d.get("symbol")
        if sym:
            acq_counts[sym] = acq_counts.get(sym, 0) + 1
    top_acquirers = sorted(acq_counts.items(), key=lambda kv: -kv[1])[:20]

    # Date range
    dates = sorted([d.get("transactionDate", "")[:10] for d in deals if d.get("transactionDate")])
    return {
        "n_deals": len(deals),
        "date_range": {"start": dates[0] if dates else None,
                        "end": dates[-1] if dates else None},
        "by_sector": [{"sector": s, "count": c} for s, c in
                        sorted(sector_counts.items(), key=lambda kv: -kv[1])],
        "top_acquirers": [{"symbol": s, "count": c,
                              "name": (profiles.get(s) or {}).get("name"),
                              "sector": (profiles.get(s) or {}).get("sector")}
                             for s, c in top_acquirers],
    }


CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "public, max-age=600",
    "Content-Type": "application/json",
}


def lambda_handler(event, context):
    """Main entry. Returns cached JSON if <1h old, else fetches fresh."""
    # Handle OPTIONS preflight
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    qs = event.get("queryStringParameters") or {}
    force = (qs.get("force") == "1")

    # Check in-memory cache first
    now = time.time()
    if not force and _MEMO["json"] and (now - _MEMO["ts"]) < CACHE_TTL_SECONDS:
        return {"statusCode": 200, "headers": CORS_HEADERS,
                  "body": json.dumps(_MEMO["json"], default=str)}

    # Try S3 cache
    if not force:
        cached, age = load_s3_cache()
        if cached and age is not None and age < CACHE_TTL_SECONDS:
            _MEMO["json"] = cached
            _MEMO["ts"] = now
            return {"statusCode": 200, "headers": CORS_HEADERS,
                      "body": json.dumps(cached, default=str)}

    # Fetch fresh
    print("[ma] fetching fresh from FMP...")
    started = time.time()
    deals = fetch_all_deals(max_pages=5)
    print(f"[ma] fetched {len(deals)} deals in {time.time()-started:.1f}s")
    profiles = enrich_with_profiles(deals, max_lookups=60)
    print(f"[ma] enriched {len(profiles)} profiles")
    summary = build_summary(deals, profiles)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "summary": summary,
        "deals": deals,
        "profiles": profiles,
    }

    # Write to S3 for caching across cold starts
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
            CacheControl="public, max-age=600",
        )
    except Exception as e:
        print(f"[s3] write err: {e}")

    _MEMO["json"] = payload
    _MEMO["ts"] = now

    return {"statusCode": 200, "headers": CORS_HEADERS,
              "body": json.dumps(payload, default=str)}
