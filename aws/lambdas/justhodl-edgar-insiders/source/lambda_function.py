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
# Note: we do NOT set Host header explicitly — urllib auto-sets it from the
# URL, and explicit Host can cause conflicts in certain Python versions.
SEC_HEADERS = {
    "User-Agent": "JustHodl.AI Khalid raafouis@gmail.com",
    "Accept-Encoding": "gzip, deflate",
}
SEC_DATA_HEADERS = SEC_HEADERS  # same headers for both www.sec.gov and data.sec.gov

SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL  = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVE_BASE     = "https://www.sec.gov/Archives/edgar/data"

LOOKBACK_DAYS = 180  # 180-day window so we can split into recent 90 vs prior 90
                      # The signal compares recent activity to the prior baseline.
RECENT_WINDOW_DAYS = 90

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
    """Parse a Form 4 XML document. Returns {filer, role, transactions}.

    SEC EDGAR returns 'primary' from submissions.json with an XSL-transform
    subdirectory prefix like 'xslF345X05/wf-form4_xxx.xml' — that URL serves
    the human-readable HTML rendering, NOT the raw XML. We strip the xsl*
    prefix to get the actual XML document.
    """
    # Construct archive URL: accession e.g. "0001234567-23-000001" → "000123456723000001"
    acc_clean = accession.replace("-", "")

    # Resolve the actual XML document path
    doc = None
    if primary and primary.endswith(".xml"):
        # Strip XSL transform prefix if present (e.g. 'xslF345X05/foo.xml' → 'foo.xml')
        # XSL directories on EDGAR start with 'xsl' and contain a transformation
        # stylesheet. The raw XML lives at the same accession root without the prefix.
        if "/" in primary:
            parts = primary.split("/")
            if parts[0].lower().startswith("xsl"):
                doc = "/".join(parts[1:])
            else:
                doc = primary  # might be a real subdirectory, keep it
        else:
            doc = primary
    else:
        # Fallback: fetch filing index, find a Form 4 XML link.
        # Prefer paths NOT under xsl* directories.
        idx_url = f"{SEC_ARCHIVE_BASE}/{int(cik)}/{acc_clean}/"
        try:
            idx_html = _sec_get(idx_url).decode("utf-8", errors="ignore")
        except Exception as e:
            return {"error": f"index fetch: {e}"}
        # Find all .xml hrefs
        xml_candidates = re.findall(r'href="([^"]+\.xml)"', idx_html, re.IGNORECASE)
        # Pick the first one NOT under an xsl* directory
        for c in xml_candidates:
            if "/xsl" not in c.lower():
                # could be absolute or relative; take just the filename
                doc = c.split("/")[-1]
                break
        if not doc and xml_candidates:
            doc = xml_candidates[0].split("/")[-1]
        if not doc:
            return {"error": "no XML doc found"}

    xml_url = f"{SEC_ARCHIVE_BASE}/{int(cik)}/{acc_clean}/{doc}"
    try:
        xml_bytes = _sec_get(xml_url)
    except Exception as e:
        return {"error": f"xml fetch ({doc}): {e}"}

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        return {"error": f"xml parse ({doc}): {e}"}

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
            code    = f(txn, "transactionCoding/transactionCode")  # P/S/A/M/D/F/G/W/I/V
            shares  = f(txn, "transactionAmounts/transactionShares/value")
            price   = f(txn, "transactionAmounts/transactionPricePerShare/value")
            ad      = f(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")  # A=acquired or D=disposed
            if not (t_date and code and shares):
                continue
            shares_f = float(shares)
            price_f  = float(price) if price else 0.0

            # Filter out non-market-signal transactions
            #   A = grant/award (RSU vesting etc) — not a conviction signal
            #   M = exercise of derivative security (options) — not a market signal
            #   G = bona fide gift
            #   W = transfer by will or laws of descent
            #   I = discretionary transaction by employee benefit plan
            #   V = voluntary disclosure (often a re-filing)
            if code in ("A", "M", "G", "W", "I", "V"):
                continue

            # Direction from acquired/disposed code — more reliable than guessing
            # from transactionCode alone (P/S aren't the only codes for buys/sells).
            # Common remaining codes:
            #   P = open-market purchase (BUY)
            #   S = open-market sale (SELL)
            #   D = sale or disposition back to issuer (SELL)
            #   F = payment of exercise price or tax via withholding (TAX_SELL)
            if ad == "A":
                direction = "BUY"
            elif ad == "D":
                direction = "TAX_SELL" if code == "F" else "SELL"
            else:
                # Skip transactions without a clear direction
                continue

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
    """Build the aggregate signal block from parsed filings.

    Signal v2 (post-2026-06-02): baseline-relative, not absolute-flow.

    Megacaps have routine RSU vesting and 10b5-1 plan selling. The original
    absolute-flow logic flagged every major company as BEARISH because any
    $10M+ net sell tripped it. A PM doesn't care about routine selling — they
    care about *acceleration* or *unusual concentration*.

    NEW LOGIC
    ─────────
    Split the 180-day window into:
      recent_90d : last 90 days of activity
      prior_90d  : the 90 days before that (the baseline)

    Compute acceleration = recent_sells / max(prior_sells, threshold)

    Labels:
      STRONG_CLUSTER_BUY   — 3+ buys in 14d by 2+ filers (rare, valuable, unchanged)
      INSIDER_BUYING       — any meaningful open-market buys (also rare + bullish)
      ACCELERATING_SELL    — recent sells >= 2x prior baseline AND >= 1 C-suite seller
      ROUTINE_SELLING      — sells present but within normal baseline range
      QUIET                — minimal or no material activity
    """
    # Bucket transactions by recency
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(days=RECENT_WINDOW_DAYS)

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
                "bucket": f.get("bucket", "other"),
                "n_filings": 0, "total_shares_buy": 0, "total_shares_sell": 0,
                "total_dollars_buy": 0, "total_dollars_sell": 0,
            }
        by_filer[filer]["n_filings"] += 1
        for txn in f["transactions"]:
            txn["filer"] = filer
            txn["role"] = f.get("role")
            txn["weight"] = f.get("weight", 1.0)
            txn["bucket"] = f.get("bucket", "other")
            try:
                txn_date = datetime.fromisoformat(txn["date"]).replace(tzinfo=timezone.utc)
                txn["is_recent"] = (txn_date >= recent_cutoff)
            except Exception:
                txn["is_recent"] = True  # treat undated as recent (be conservative)
            all_txns.append(txn)
            if txn["direction"] == "BUY":
                by_filer[filer]["total_shares_buy"] += txn["shares"]
                by_filer[filer]["total_dollars_buy"] += txn["dollars"]
            elif txn["direction"] in ("SELL", "TAX_SELL"):
                by_filer[filer]["total_shares_sell"] += txn["shares"]
                by_filer[filer]["total_dollars_sell"] += txn["dollars"]

    # Split into recent vs prior
    buys_all  = [t for t in all_txns if t["direction"] == "BUY"]
    sells_all = [t for t in all_txns if t["direction"] == "SELL"]

    recent_buys  = [t for t in buys_all  if t["is_recent"]]
    recent_sells = [t for t in sells_all if t["is_recent"]]
    prior_buys   = [t for t in buys_all  if not t["is_recent"]]
    prior_sells  = [t for t in sells_all if not t["is_recent"]]

    def sum_dollars(txns):
        return sum(t["dollars"] for t in txns)

    recent_buy_dollars  = sum_dollars(recent_buys)
    recent_sell_dollars = sum_dollars(recent_sells)
    prior_sell_dollars  = sum_dollars(prior_sells)
    prior_buy_dollars   = sum_dollars(prior_buys)

    net_dollars_90d = recent_buy_dollars - recent_sell_dollars
    net_shares_90d  = (sum(t["shares"] for t in recent_buys)
                        - sum(t["shares"] for t in recent_sells))

    # Acceleration ratio. If prior period was quiet (< $500K), we set
    # prior to $500K to avoid division-by-zero amplification. Any
    # recent-90d $1M+ sell against a quiet prior is interesting; the
    # ratio captures that without being silly when prior is literally 0.
    BASELINE_FLOOR = 500_000
    sell_acceleration = recent_sell_dollars / max(prior_sell_dollars, BASELINE_FLOOR)
    buy_acceleration  = recent_buy_dollars  / max(prior_buy_dollars,  BASELINE_FLOOR)

    # C-suite involvement in recent sells (CEO/CFO selling matters more)
    recent_csuite_sellers = set(t["filer"] for t in recent_sells if t["bucket"] == "ceo_cfo")
    n_csuite_sellers      = len(recent_csuite_sellers)

    # Weighted scores (role-weighted)
    weighted_recent_buy  = sum(t["dollars"] * t["weight"] for t in recent_buys)
    weighted_recent_sell = sum(t["dollars"] * t["weight"] for t in recent_sells)

    # Cluster detection (unchanged): 3+ buys in 14d window by 2+ filers
    cluster_detected = False
    cluster_window = None
    if len(buys_all) >= 3:
        buys_by_date = sorted(buys_all, key=lambda t: t["date"])
        for i, t in enumerate(buys_by_date):
            window_buys = [b for b in buys_by_date[i:]
                            if (datetime.fromisoformat(b["date"]) - datetime.fromisoformat(t["date"])).days <= 14]
            window_filers = set(b["filer"] for b in window_buys)
            if len(window_buys) >= 3 and len(window_filers) >= 2:
                cluster_detected = True
                cluster_window = {
                    "start":           t["date"],
                    "n_buys":          len(window_buys),
                    "n_unique_filers": len(window_filers),
                    "dollars":         round(sum(b["dollars"] for b in window_buys), 2),
                }
                break

    # ── Signal labelling (v2)
    if cluster_detected and recent_buy_dollars > 0:
        signal_label = "STRONG_CLUSTER_BUY"
        signal_score = min(100, 75 + len(recent_buys) * 5)
        signal_note = f"⚡ {cluster_window['n_buys']} buys by {cluster_window['n_unique_filers']} insiders in 14d (${cluster_window['dollars']:,.0f})"
    elif recent_buy_dollars >= 100_000 and len(recent_buys) >= 1:
        # Even a single meaningful open-market buy at a megacap is notable
        signal_label = "INSIDER_BUYING"
        signal_score = min(85, 60 + min(25, recent_buy_dollars / 1_000_000 * 5))
        signal_note = f"${recent_buy_dollars:,.0f} in open-market buys (90d)"
    elif sell_acceleration >= 2.0 and recent_sell_dollars >= 5_000_000 and n_csuite_sellers >= 1:
        signal_label = "ACCELERATING_SELL"
        signal_score = max(15, 40 - min(25, (sell_acceleration - 2) * 5))
        signal_note = (f"sells are {sell_acceleration:.1f}× the prior 90d baseline; "
                        f"{n_csuite_sellers} C-suite officer{'s' if n_csuite_sellers>1 else ''} selling")
    elif recent_sell_dollars > 0 and recent_buy_dollars == 0:
        signal_label = "ROUTINE_SELLING"
        signal_score = 45  # slight tilt — no buying is mildly bearish but not actionable
        signal_note = (f"${recent_sell_dollars:,.0f} sold (90d), within normal range "
                        f"({sell_acceleration:.1f}× prior baseline)")
    elif len(all_txns) == 0:
        signal_label = "QUIET"
        signal_score = 50
        signal_note = "No reportable insider activity in 180 days"
    else:
        signal_label = "NEUTRAL"
        signal_score = 50
        signal_note = "Mixed or low-volume activity"

    # ── Top sellers (top 5 by dollar amount in recent 90d)
    sellers_summary = {}
    for t in recent_sells:
        f = t["filer"]
        if f not in sellers_summary:
            sellers_summary[f] = {"filer": f, "role": t["role"], "bucket": t["bucket"],
                                    "shares": 0, "dollars": 0, "n_txns": 0}
        sellers_summary[f]["shares"]  += t["shares"]
        sellers_summary[f]["dollars"] += t["dollars"]
        sellers_summary[f]["n_txns"]  += 1
    top_sellers = sorted(sellers_summary.values(), key=lambda s: -s["dollars"])[:5]
    for s in top_sellers:
        s["shares"] = int(round(s["shares"]))
        s["dollars"] = round(s["dollars"], 0)

    # ── Top buyers (rarer; named individuals matter)
    buyers_summary = {}
    for t in recent_buys:
        f = t["filer"]
        if f not in buyers_summary:
            buyers_summary[f] = {"filer": f, "role": t["role"], "bucket": t["bucket"],
                                   "shares": 0, "dollars": 0, "n_txns": 0}
        buyers_summary[f]["shares"]  += t["shares"]
        buyers_summary[f]["dollars"] += t["dollars"]
        buyers_summary[f]["n_txns"]  += 1
    top_buyers = sorted(buyers_summary.values(), key=lambda s: -s["dollars"])[:5]
    for s in top_buyers:
        s["shares"] = int(round(s["shares"]))
        s["dollars"] = round(s["dollars"], 0)

    return {
        # Recent window (last 90d) — main signal window
        "n_buys":              len(recent_buys),
        "n_sells":             len(recent_sells),
        "total_shares_buy":    round(sum(t["shares"] for t in recent_buys), 0),
        "total_shares_sell":   round(sum(t["shares"] for t in recent_sells), 0),
        "total_dollars_buy":   round(recent_buy_dollars, 0),
        "total_dollars_sell":  round(recent_sell_dollars, 0),
        "net_shares_90d":      round(net_shares_90d, 0),
        "net_dollars_90d":     round(net_dollars_90d, 0),
        # Prior 90d (baseline window)
        "prior_n_buys":         len(prior_buys),
        "prior_n_sells":        len(prior_sells),
        "prior_dollars_buy":    round(prior_buy_dollars, 0),
        "prior_dollars_sell":   round(prior_sell_dollars, 0),
        # Acceleration ratios — the heart of the v2 signal
        "sell_acceleration":   round(sell_acceleration, 2),
        "buy_acceleration":    round(buy_acceleration, 2),
        "n_csuite_sellers":    n_csuite_sellers,
        # Weighted (kept for backward compat / future tuning)
        "weighted_buy_score":  round(weighted_recent_buy, 0),
        "weighted_sell_score": round(weighted_recent_sell, 0),
        "weighted_net_score":  round(weighted_recent_buy - weighted_recent_sell, 0),
        # Final signal
        "signal_score":        round(signal_score, 0),
        "signal_label":        signal_label,
        "signal_note":         signal_note,
        # Cluster detail
        "cluster_detected":    cluster_detected,
        "cluster_window":      cluster_window,
        # Rollups
        "ranks":               ranks,
        "by_filer":            by_filer,
        "top_sellers":         top_sellers,
        "top_buyers":          top_buyers,
        # Transactions — newest first, recent-period only (top 50 = ample for UI)
        "transactions":        sorted(recent_buys + recent_sells, key=lambda t: t["date"], reverse=True)[:50],
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

    # 3. Parse each filing SERIALLY (cap to 30 most recent)
    # Serial because: (a) SEC's 10 req/sec ceiling, (b) our _last_sec_request
    # throttle isn't thread-safe, (c) each XML fetch is 0.2-0.5s so 30 × 0.4s
    # = 12s total, comfortably within Lambda's 120s timeout.
    filings = filings[:30]
    parsed = []
    for f in filings:
        try:
            p = parse_form4_xml(cik, f["accession"], f["primary"])
            p["accession"] = f["accession"]
            p["date"] = f["date"]
            p["form"] = f["form"]
            parsed.append(p)
        except Exception as e:
            parsed.append({"error": str(e)[:200], "accession": f["accession"]})

    # Track first parse error for debugging
    parse_errors = [p.get("error") for p in parsed if "error" in p][:3]

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
        "parse_errors_sample": parse_errors,  # first 3 errors for debugging
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
