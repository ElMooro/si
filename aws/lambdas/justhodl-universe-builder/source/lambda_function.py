"""
justhodl-universe-builder — maintains data/universe.json, the master ticker
list with enriched fundamentals.

Strategy:
  1. Pull FMP /stable/stock-list (returns ~10K US tickers)
  2. Filter: US-listed (NASDAQ/NYSE), exchange != OTC, mcap >= MIN_MCAP
  3. Enrich each candidate with /stable/profile + /stable/quote (parallel)
  4. Output sorted by market cap descending

Universe size: ~1500-2000 names typically (after mcap filter)
Runtime: ~3-4 minutes with 12 workers and a 240s budget.

Output: data/universe.json with full enrichment for each name.
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
S3_KEY = os.environ.get("S3_KEY", "data/universe.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

MIN_MCAP = float(os.environ.get("MIN_MCAP", "200000000"))   # $200M
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "2500"))
ENRICH_WORKERS = int(os.environ.get("ENRICH_WORKERS", "16"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "NYSEARCA", "BATS", "PNK", "OTC"}

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Universe/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_stock_list():
    """FMP /stable/stock-list returns roughly 10K tickers."""
    url = f"https://financialmodelingprep.com/stable/stock-list?apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=30)
        if isinstance(d, list):
            return d
    except Exception as e:
        print(f"[universe] stock-list fetch failed: {e}")
    return []


def fetch_quote(symbol):
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_profile(symbol):
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def enrich(symbol, deadline_at):
    """Pull quote + profile in parallel for one ticker."""
    if time.time() > deadline_at:
        return None
    q = fetch_quote(symbol)
    if not q:
        return None
    mcap = q.get("marketCap") or 0
    if mcap < MIN_MCAP:
        return None
    p = fetch_profile(symbol)
    sector = (p or {}).get("sector") or q.get("sector") or ""
    industry = (p or {}).get("industry") or q.get("industry") or ""
    company = (p or {}).get("companyName") or q.get("name") or symbol
    price = q.get("price") or 0
    yhigh = q.get("yearHigh") or 0
    ylow = q.get("yearLow") or 0
    exchange = (p or {}).get("exchange") or q.get("exchange") or ""
    country = (p or {}).get("country") or "US"
    pct_from_52h = ((price - yhigh) / yhigh * 100) if yhigh else 0
    pct_from_52l = ((price - ylow) / ylow * 100) if ylow else 0
    return {
        "symbol": symbol,
        "name": company,
        "sector": sector,
        "industry": industry,
        "market_cap": mcap,
        "price": price,
        "year_high": yhigh,
        "year_low": ylow,
        "pct_from_52w_high": round(pct_from_52h, 1),
        "pct_from_52w_low": round(pct_from_52l, 1),
        "exchange": exchange,
        "country": country,
        "volume": q.get("volume") or 0,
        "avg_volume": q.get("avgVolume") or 0,
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[universe] starting v1.0, max_tickers={MAX_TICKERS}, min_mcap=${MIN_MCAP/1e9:.2f}B")

    # Step 1: pull master list
    raw = fetch_stock_list()
    print(f"[universe] FMP stock-list returned {len(raw)} tickers")

    # Step 2: pre-filter
    candidates = []
    for r in raw:
        sym = (r.get("symbol") or "").upper().strip()
        ex = (r.get("exchangeShortName") or r.get("exchange") or "").upper()
        if not sym or len(sym) > 6:
            continue
        if "." in sym or "-" in sym:
            continue  # skip preferred / non-equity instruments
        if ex and ex not in ALLOWED_EXCHANGES:
            continue
        candidates.append(sym)
    candidates = sorted(set(candidates))
    print(f"[universe] pre-filter retained {len(candidates)} candidates")

    # Cap to MAX_TICKERS at this stage to bound enrichment time
    if len(candidates) > MAX_TICKERS:
        candidates = candidates[:MAX_TICKERS]
        print(f"[universe] capped to {MAX_TICKERS} for enrichment budget")

    # Step 3: enrich in parallel
    enriched = []
    statuses = {"ok": 0, "no_quote": 0, "below_mcap": 0, "deadline": 0}
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = {pool.submit(enrich, s, deadline_at): s for s in candidates}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=30)
            except Exception:
                statuses["deadline"] += 1
                continue
            if r is None:
                statuses["below_mcap"] += 1
                continue
            enriched.append(r)
            statuses["ok"] += 1
    enriched.sort(key=lambda x: -(x["market_cap"] or 0))
    print(f"[universe] enriched: {len(enriched)} stocks, statuses: {statuses}")
    print(f"[universe] runtime: {time.time() - started:.1f}s")

    # Stats
    by_sector = {}
    by_mcap_bucket = {"mega (>$200B)": 0, "large ($10-200B)": 0, "mid ($2-10B)": 0,
                      "small ($300M-2B)": 0, "micro (<$300M)": 0}
    for s in enriched:
        sec = s.get("sector") or "Unknown"
        by_sector[sec] = by_sector.get(sec, 0) + 1
        mc = s["market_cap"]
        if mc >= 2e11: by_mcap_bucket["mega (>$200B)"] += 1
        elif mc >= 1e10: by_mcap_bucket["large ($10-200B)"] += 1
        elif mc >= 2e9: by_mcap_bucket["mid ($2-10B)"] += 1
        elif mc >= 3e8: by_mcap_bucket["small ($300M-2B)"] += 1
        else: by_mcap_bucket["micro (<$300M)"] += 1

    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_total": len(enriched),
            "n_raw_input": len(raw),
            "n_pre_filter": len(candidates),
            "by_sector": by_sector,
            "by_mcap_bucket": by_mcap_bucket,
            "statuses": statuses,
        },
        "stocks": enriched,
    }
    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[universe] wrote {len(body):,}b to {S3_KEY}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_total": len(enriched),
            "duration_s": out["duration_s"],
            "n_by_sector": len(by_sector),
        }),
    }
