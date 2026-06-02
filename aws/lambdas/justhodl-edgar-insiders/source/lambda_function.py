"""justhodl-edgar-insiders

Fetches Form 4 insider transactions directly from SEC EDGAR. This is the
free, authoritative source for officer/director trades — works around the
FMP plan limit which blocks /stable/insider-trading on our tier.

API: SEC EDGAR (no auth, public, free)
  1. Ticker → CIK lookup: https://www.sec.gov/files/company_tickers.json
  2. CIK → recent filings: https://data.sec.gov/submissions/CIK{padded10}.json
  3. Filter to Form 4 / 4/A only
  4. For each filing, fetch the index: archives/edgar/data/{cik}/{accession}/
  5. Find primary XML doc, parse for transactions (date, shares, price,
     buy/sell, officer name + role)
  6. Aggregate: net 90-day flow, cluster detection, blue-chip weighting

OUTPUT
  GET /?ticker=AAPL → {
    ticker, cik, n_filings_90d, n_buys, n_sells,
    net_shares_90d, net_dollars_90d, signal_score, signal_label,
    cluster_detected, ranks: {ceo_cfo: N, vp: N, other: N},
    filings: [{date, filer, role, transaction, shares, price_avg, dollars,
                 form, accession}],
    by_filer: {filer_name: {role, total_shares, total_dollars, n_filings}},
  }

SIGNAL LOGIC
  Net flow + role weighting → 0-100 score, plus directional label:
  - Score >= 70 + net buys → BULLISH_INSIDER_BUY
  - Score >= 70 + net sells → BEARISH_INSIDER_SELL (less actionable; sells are noisier)
  - Cluster (3+ buys in 14d by 2+ unique filers, weighted) → STRONG_CLUSTER_BUY
  - Otherwise → NEUTRAL

CACHE
  24h S3 cache at edgar-insiders/{TICKER}.json (matches FMP cache TTL)

SCHEDULE
  Lambda is on-demand (no schedule). The equity-research Lambda can call
  it during synthesis, or why.html can fetch it directly via Lambda URL.

RATE LIMITS
  SEC EDGAR: 10 req/sec MAX with proper User-Agent. We do ~3-5 req/ticker
  (lookup + submissions + 1-3 filing XMLs) with 0.15s spacing.
"""
import json
import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree as ET
import boto3

# ═══════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════
S3_BUCKET = "justhodl-dashboard-live"
CACHE_PREFIX = "edgar-insiders/"
CACHE_TTL = 86400  # 24h

# SEC EDGAR requires a proper User-Agent identifying the consumer.
# Format per their fair-access policy: "Company Name contact@email.com"
# Without this, you get 403s.
SEC_HEADERS = {
    "User-Agent": "JustHodl.AI Khalid raafouis@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}
SEC_DATA_HEADERS = {**SEC_HEADERS, "Host": "data.sec.gov"}

SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL  = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVE_BASE     = "https://www.sec.gov/Archives/edgar/data"

LOOKBACK_DAYS = 90  # 90-day window for "recent" insider activity

s3 = boto3.client("s3")

# ═══════════════════════════════════════════════════════════════════
# HTTP with retry + SEC rate limiting
# ═══════════════════════════════════════════════════════════════════
_last_sec_request = 0.0

def _sec_get(url, headers=None, timeout=20):
    """Throttled SEC GET. Enforces ~7 req/sec ceiling."""
    global _last_sec_request
    gap = time.time() - _last_sec_request
    if gap < 0.15:
        time.sleep(0.15 - gap)
    _last_sec_request = time.time()
    req = urllib.request.Request(url, headers=headers or SEC_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            # Handle gzip
            if r.headers.get("Content-Encoding") == "gzip":
                import gzip
                data = gzip.decompress(data)
            return data
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"SEC HTTP {e.code} on {url[:90]}: {e.reason}")


# ═══════════════════════════════════════════════════════════════════
# Step 1: Ticker → CIK lookup
# ═══════════════════════════════════════════════════════════════════
_ticker_cache = None  # warm in-memory cache per Lambda container

def lookup_cik(ticker: str) -> str:
    """Resolve ticker to padded 10-digit CIK. Returns '' if not found."""
    global _ticker_cache
    if _ticker_cache is None:
        try:
            data = _sec_get(SEC_TICKER_INDEX_URL)
            _ticker_cache = json.loads(data)
        except Exception as e:
            print(f"[cik] ticker index fetch failed: {e}")
            return ""
    t = ticker.upper().strip()
    # File is an object keyed by string ints; each value is {cik_str, ticker, title}
    for _, row in _ticker_cache.items():
        if row.get("ticker", "").upper() == t:
            cik = str(row.get("cik_str", "")).zfill(10)
            return cik
    return ""


# ═══════════════════════════════════════════════════════════════════
# Step 2: Fetch recent filings, filter to Form 4
# ═══════════════════════════════════════════════════════════════════
def fetch_form4_filings(cik: str, lookback_days: int = LOOKBACK_DAYS) -> list:
    """Returns list of {accession, date, form, primary_doc} for recent Form 4s."""
    url = SEC_SUBMISSIONS_URL.format(cik=cik)
    try:
        data = _sec_get(url, headers=SEC_DATA_HEADERS)
        sub = json.loads(data)
    except Exception as e:
        print(f"[filings] CIK {cik} submissions fetch failed: {e}")
        return []

    recent = sub.get("filings", {}).get("recent", {})
    forms     = recent.get("form", [])
    accs      = recent.get("accessionNumber", [])
    dates     = recent.get("filingDate", [])
    primaries = recent.get("primaryDocument", [])

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    out = []
    for i, form in enumerate(forms):
        if form not in ("4", "4/A"):
            continue
        try:
            filing_dt = datetime.fromisoformat(dates[i]).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if filing_dt < cutoff:
            continue
        out.append({
            "accession": accs[i],
            "date":      dates[i],
            "form":      form,
            "primary":   primaries[i] if i < len(primaries) else "",
        })
    return out


# ═══════════════════════════════════════════════════════════════════
# Step 3: Parse Form 4 XML
# ═══════════════════════════════════════════════════════════════════
# Officer rank weights: CEO/CFO trades carry more signal than rank-and-file
ROLE_WEIGHTS = {
    "ceo": 3.0, "chief executive": 3.0, "chairman": 2.5,
    "cfo": 3.0, "chief financial": 3.0,
    "coo": 2.5, "chief operating": 2.5,
    "president": 2.5,
    "evp": 2.0, "executive vp": 2.0, "executive vice": 2.0,
    "svp": 1.8, "senior vp": 1.8, "senior vice": 1.8,
    "vp": 1.5, "vice president": 1.5,
    "director": 1.3,
    "10%": 1.2,    # 10% beneficial owner
}

def classify_role(title: str) -> tuple:
    """Returns (weight, normalized_bucket). Bucket is 'ceo_cfo' / 'vp' / 'other'."""
    t = (title or "").lower()
    weight = 1.0
    for keyword, w in ROLE_WEIGHTS.items():
        if keyword in t:
            weight = max(weight, w)
    bucket = "other"
    if any(k in t for k in ["ceo", "cfo", "coo", "chief", "president", "chairman"]):
        bucket = "ceo_cfo"
    elif any(k in t for k in ["vp", "vice president", "director", "officer"]):
        bucket = "vp"
    return weight, bucket


def parse_form4_xml(cik: str, accession: str, primary: str) -> dict:
    """Parse a Form 4 XML document. Returns {filer, role, transactions}."""
    # Construct archive URL: accession e.g. "0001234567-23-000001" → "000123456723000001"
    acc_clean = accession.replace("-", "")
    # primary may be an .xml or .html; we need the XML version
    if primary and primary.endswith(".xml"):
        doc = primary
    else:
        # Try the common pattern: form4.xml or wf-form4_*.xml
        # We'll fetch the filing index first to find the XML doc.
        idx_url = f"{SEC_ARCHIVE_BASE}/{int(cik)}/{acc_clean}/"
        try:
            idx_html = _sec_get(idx_url).decode("utf-8", errors="ignore")
        except Exception as e:
            return {"error": f"index fetch: {e}"}
        # Find any *.xml link
        m = re.search(r'href="([^"]+\.xml)"', idx_html, re.IGNORECASE)
        if not m:
            return {"error": "no XML doc found"}
        doc = m.group(1).split("/")[-1]

    xml_url = f"{SEC_ARCHIVE_BASE}/{int(cik)}/{acc_clean}/{doc}"
    try:
        xml_bytes = _sec_get(xml_url)
    except Exception as e:
        return {"error": f"xml fetch: {e}"}

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        return {"error": f"xml parse: {e}"}

    def f(elem, path):
        """Find element text by path, return None if missing."""
        n = elem.find(path)
        return n.text.strip() if n is not None and n.text else None

    out = {"transactions": []}

    # Filer name + role from reportingOwner
    ro = root.find(".//reportingOwner")
    if ro is not None:
        out["filer"] = f(ro, "reportingOwnerId/rptOwnerName") or "(unknown)"
        rels = []
        r = ro.find("reportingOwnerRelationship")
        if r is not None:
            if (r.find("isOfficer") is not None and r.find("isOfficer").text or "").strip() in ("1","true"):
                rels.append("Officer")
            if (r.find("isDirector") is not None and r.find("isDirector").text or "").strip() in ("1","true"):
                rels.append("Director")
            if (r.find("isTenPercentOwner") is not None and r.find("isTenPercentOwner").text or "").strip() in ("1","true"):
                rels.append("10% Owner")
            title = f(r, "officerTitle")
            if title:
                rels.append(title)
        out["role"] = " · ".join(rels) if rels else "(unknown)"
    else:
        out["filer"] = "(unknown)"
        out["role"] = "(unknown)"

    weight, bucket = classify_role(out["role"])
    out["weight"] = weight
    out["bucket"] = bucket

    # Non-derivative (common stock) transactions
    for txn in root.findall(".//nonDerivativeTransaction"):
        try:
            t_date  = f(txn, "transactionDate/value")
            code    = f(txn, "transactionCoding/transactionCode")  # A=accepted (grant), D=disposed, P=purchase, S=sale, M=option exercise
            shares  = f(txn, "transactionAmounts/transactionShares/value")
            price   = f(txn, "transactionAmounts/transactionPricePerShare/value")
            ad      = f(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")  # A or D
            if not (t_date and code and shares):
                continue
            shares_f = float(shares)
            price_f  = float(price) if price else 0.0
            # Only count P (open-market buys), S (sales), F (tax withholding sells), and skip A (grants/awards), M (options)
            if code not in ("P", "S", "F"):
                continue
            direction = "BUY" if (ad == "A" and code == "P") else "SELL"
            # F is forced sell-for-tax; treat as soft sell
            if code == "F":
                direction = "TAX_SELL"
            out["transactions"].append({
                "date": t_date, "code": code, "direction": direction,
                "shares": shares_f, "price": price_f,
                "dollars": shares_f * price_f,
            })
        except Exception as e:
            continue

    return out


# ═══════════════════════════════════════════════════════════════════
# Step 4: Aggregate signal
# ═══════════════════════════════════════════════════════════════════
def aggregate_signal(filings_parsed: list) -> dict:
    """Build the aggregate signal block from parsed filings."""
    all_txns = []
    by_filer = {}
    ranks = {"ceo_cfo": 0, "vp": 0, "other": 0}

    for f in filings_parsed:
        if "error" in f or not f.get("transactions"):
            continue
        ranks[f.get("bucket", "other")] = ranks.get(f.get("bucket","other"), 0) + 1
        filer = f.get("filer", "(unknown)")
        if filer not in by_filer:
            by_filer[filer] = {
                "role": f.get("role"), "weight": f.get("weight", 1.0),
                "n_filings": 0, "total_shares_buy": 0, "total_shares_sell": 0,
                "total_dollars_buy": 0, "total_dollars_sell": 0,
            }
        by_filer[filer]["n_filings"] += 1
        for txn in f["transactions"]:
            txn["filer"] = filer
            txn["role"] = f.get("role")
            txn["weight"] = f.get("weight", 1.0)
            all_txns.append(txn)
            if txn["direction"] == "BUY":
                by_filer[filer]["total_shares_buy"] += txn["shares"]
                by_filer[filer]["total_dollars_buy"] += txn["dollars"]
            elif txn["direction"] in ("SELL", "TAX_SELL"):
                by_filer[filer]["total_shares_sell"] += txn["shares"]
                by_filer[filer]["total_dollars_sell"] += txn["dollars"]

    buys  = [t for t in all_txns if t["direction"] == "BUY"]
    sells = [t for t in all_txns if t["direction"] == "SELL"]

    total_buy_dollars  = sum(t["dollars"] for t in buys)
    total_sell_dollars = sum(t["dollars"] for t in sells)
    net_dollars        = total_buy_dollars - total_sell_dollars

    total_buy_shares  = sum(t["shares"] for t in buys)
    total_sell_shares = sum(t["shares"] for t in sells)
    net_shares        = total_buy_shares - total_sell_shares

    # Weighted score: each transaction contributes weight × dollars
    weighted_buy  = sum(t["dollars"] * t["weight"] for t in buys)
    weighted_sell = sum(t["dollars"] * t["weight"] for t in sells)
    weighted_net  = weighted_buy - weighted_sell

    # Cluster detection: 3+ buys in any 14-day window by 2+ unique filers
    cluster_detected = False
    cluster_window = None
    if len(buys) >= 3:
        buys_by_date = sorted(buys, key=lambda t: t["date"])
        unique_filers = set()
        for i, t in enumerate(buys_by_date):
            window_buys = [b for b in buys_by_date[i:]
                            if (datetime.fromisoformat(b["date"]) - datetime.fromisoformat(t["date"])).days <= 14]
            window_filers = set(b["filer"] for b in window_buys)
            if len(window_buys) >= 3 and len(window_filers) >= 2:
                cluster_detected = True
                cluster_window = {
                    "start": t["date"],
                    "n_buys": len(window_buys),
                    "n_unique_filers": len(window_filers),
                    "dollars": round(sum(b["dollars"] for b in window_buys), 2),
                }
                break

    # Signal label
    # We weight buys higher than sells (sells often planned, taxes, etc.)
    if cluster_detected and weighted_buy > 0:
        signal_label = "STRONG_CLUSTER_BUY"
        signal_score = min(100, 70 + len(buys) * 5)
    elif weighted_net > 1_000_000 and len(buys) >= 2:
        signal_label = "BULLISH_INSIDER_BUY"
        signal_score = min(90, 50 + (weighted_buy / max(1, weighted_buy + weighted_sell)) * 50)
    elif weighted_net < -10_000_000 and len(sells) >= 3:
        signal_label = "BEARISH_INSIDER_SELL"
        signal_score = max(10, 50 - (weighted_sell / max(1, weighted_buy + weighted_sell)) * 50)
    else:
        signal_label = "NEUTRAL"
        signal_score = 50

    return {
        "n_buys":              len(buys),
        "n_sells":             len(sells),
        "total_shares_buy":    round(total_buy_shares, 0),
        "total_shares_sell":   round(total_sell_shares, 0),
        "total_dollars_buy":   round(total_buy_dollars, 0),
        "total_dollars_sell":  round(total_sell_dollars, 0),
        "net_shares_90d":      round(net_shares, 0),
        "net_dollars_90d":     round(net_dollars, 0),
        "weighted_buy_score":  round(weighted_buy, 0),
        "weighted_sell_score": round(weighted_sell, 0),
        "weighted_net_score":  round(weighted_net, 0),
        "signal_score":        round(signal_score, 0),
        "signal_label":        signal_label,
        "cluster_detected":    cluster_detected,
        "cluster_window":      cluster_window,
        "ranks":               ranks,
        "by_filer":            by_filer,
        "transactions":        sorted(all_txns, key=lambda t: t["date"], reverse=True)[:50],
    }


# ═══════════════════════════════════════════════════════════════════
# Main handler
# ═══════════════════════════════════════════════════════════════════
def _http_ok(payload):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": f"public, max-age={CACHE_TTL}",
        },
        "body": json.dumps(payload, default=str),
    }


def _http_error(code, msg):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": msg}),
    }


def lambda_handler(event, context):
    t0 = time.time()

    ticker = None
    force_refresh = False
    try:
        if isinstance(event, dict):
            qs = event.get("queryStringParameters") or {}
            ticker = qs.get("ticker")
            force_refresh = qs.get("refresh") in ("1", "true", "yes")
    except Exception as e:
        return _http_error(400, f"parse error: {e}")
    if not ticker:
        return _http_error(400, "missing ticker")
    ticker = ticker.strip().upper()
    if not re.fullmatch(r"[A-Z0-9.\-]{1,10}", ticker):
        return _http_error(400, f"invalid ticker: {ticker}")

    cache_key = f"{CACHE_PREFIX}{ticker}.json"

    # Cache check
    if not force_refresh:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=cache_key)
            cached = json.loads(obj["Body"].read())
            cached["from_cache"] = True
            return _http_ok(cached)
        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            print(f"[cache] read miss: {e}")

    # 1. Ticker → CIK
    cik = lookup_cik(ticker)
    if not cik:
        return _http_error(404, f"CIK not found for ticker {ticker}")
    print(f"[edgar] {ticker} → CIK {cik}")

    # 2. Recent Form 4 filings
    filings = fetch_form4_filings(cik)
    print(f"[edgar] {ticker} has {len(filings)} Form 4 filings in last {LOOKBACK_DAYS}d")

    if not filings:
        result = {
            "ticker": ticker, "cik": cik,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": LOOKBACK_DAYS,
            "n_filings_90d": 0,
            "signal_label": "NO_DATA",
            "signal_score": 50,
            "message": f"No Form 4 filings for {ticker} in last {LOOKBACK_DAYS} days",
            "elapsed_s": round(time.time() - t0, 2),
        }
        s3.put_object(Bucket=S3_BUCKET, Key=cache_key,
                       Body=json.dumps(result, default=str).encode(),
                       ContentType="application/json",
                       CacheControl=f"public, max-age={CACHE_TTL}")
        return _http_ok(result)

    # 3. Parse each filing in parallel (cap to 30 most recent)
    filings = filings[:30]
    parsed = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(parse_form4_xml, cik, f["accession"], f["primary"]): f
                for f in filings}
        for fut in as_completed(futs):
            f = futs[fut]
            try:
                p = fut.result()
                p["accession"] = f["accession"]
                p["date"] = f["date"]
                p["form"] = f["form"]
                parsed.append(p)
            except Exception as e:
                parsed.append({"error": str(e), "accession": f["accession"]})

    n_parsed_ok = sum(1 for p in parsed if "error" not in p)
    print(f"[edgar] {ticker} parsed {n_parsed_ok}/{len(parsed)} XML docs successfully")

    # 4. Aggregate
    agg = aggregate_signal(parsed)

    result = {
        "ticker": ticker,
        "cik": cik,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "n_filings_90d": len(filings),
        "n_filings_parsed_ok": n_parsed_ok,
        "n_filings_parsed_fail": len(parsed) - n_parsed_ok,
        **agg,
        "elapsed_s": round(time.time() - t0, 2),
    }

    # Cache
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=cache_key,
                       Body=json.dumps(result, default=str).encode(),
                       ContentType="application/json",
                       CacheControl=f"public, max-age={CACHE_TTL}")
    except Exception as e:
        print(f"[cache] write failed: {e}")

    print(f"[edgar] {ticker} DONE: {result['n_buys']} buys / {result['n_sells']} sells, "
          f"signal={result['signal_label']} ({result['signal_score']}/100), "
          f"elapsed={result['elapsed_s']}s")
    return _http_ok(result)
