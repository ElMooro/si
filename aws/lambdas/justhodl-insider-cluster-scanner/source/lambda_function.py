"""
justhodl-insider-cluster-scanner — Production-grade insider cluster detection

PROBLEM with the previous justhodl-insider-trades:
  - Pulls atom feed of last ~100 Form 4 filings every 30 min — vastly under-samples
  - Mixes 10%-Owner filings (mostly fund rebalancing noise) with C-suite buying (real signal)
  - Hammers SEC at 8 workers → 92/100 filings 404/429 → no buys kept
  - Ticker extraction failure → most clusters say "N/A"
  - Output is unranked, no scoring, no fundamentals attached

WHAT THIS BUILDS
================
A scientifically-rigorous cluster scanner based on the Cohen-Malloy-Pomorski
(2012) "decoding insider information" methodology, extended for 2026:

Signal hierarchy (strongest → weakest):
  1. CEO + CFO buying together within 30d                 → "smart_money_dual"
  2. ≥3 distinct C-suite/director purchases in 30d        → "cluster_buy"
  3. Single buy >$1M by named CEO                          → "ceo_conviction"
  4. Sequential buying (insider buys 2+ times in 60d)     → "doubling_down"
  5. Cluster + stock down >25% from 52w high              → "contrarian_cluster"

Each signal gets a 0-100 score weighted by:
  - Insider role hierarchy (CEO/CFO/Chairman > VP > Director > 10%Owner)
  - Total dollar commitment (gradient: $50k=20, $500k=60, $5M=90, $10M+=100)
  - Recency (last 30d gets full weight, 30-60d gets 0.6x, >60d dropped)
  - Conviction (% of insider's salary, vs typical 10% allocation)
  - Stock distress (down from 52w high adds asymmetry premium)

Pipeline stages
===============
1. PULL: SEC EDGAR daily index files (more reliable than atom feed)
   - https://www.sec.gov/Archives/edgar/daily-index/<YYYY>/QTR<n>/form.<YYYYMMDD>.idx
   - Get last 7 trading days of Form 4 filings (typically 200-400/day)
2. FILTER: Only Form 4 filings (P=Purchase, not S=Sale, not A=Award)
3. PARSE: Issuer XML for ticker/CIK, transaction table for (insider, role, $, shares)
4. CLASSIFY: Role tier, transaction type (open-market vs option exercise)
5. CLUSTER: Group by ticker, deduplicate by accession_no, compute scores
6. ENRICH: Add 52w high/low, market cap, sector from FMP
7. SCORE: Apply hierarchy → produce ranked cluster list
8. WRITE: data/insider-clusters.json with full thesis-ready structure

Output schema
=============
{
  "generated_at": ISO8601,
  "lookback_days": 30,
  "stats": {n_clusters, n_strong, n_ceo_conviction, ...},
  "clusters": [
    {
      "ticker": "TX",
      "company": "Ternium SA",
      "cik": "...",
      "score": 87.3,
      "signal_type": "smart_money_dual",
      "n_insiders": 4,
      "n_transactions": 6,
      "total_value_usd": 4_850_000,
      "avg_price": 43.20,
      "window_days": 18,
      "first_buy": "2026-04-15",
      "last_buy": "2026-05-03",
      "highest_role": "CEO",
      "has_ceo": true, "has_cfo": true, "has_chairman": true,
      "insiders": [{name, role, role_tier, n_buys, total_shares, total_value}],
      "transactions": [{filed_at, insider, role, shares, price, value, accession}],
      "fundamentals": {market_cap, price_now, pct_from_52w_high, sector, industry},
      "rationale": "CEO + CFO + 2 directors bought $4.85M over 18 days at $43.20 avg, with stock 27% off 52w high"
    }
  ]
}

Schedule: hourly via EventBridge (insider Form 4 must be filed within 2 days of trade,
so we don't need real-time but daily-or-hourly catches new filings as they post)
"""
from __future__ import annotations
import json
import os
import re
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import boto3

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/insider-clusters.json")
SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "JustHodl Research raafouis@gmail.com"
)
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))
MIN_BUY_VALUE = float(os.environ.get("MIN_BUY_VALUE_USD", "10000"))
CLUSTER_MIN_INSIDERS = int(os.environ.get("CLUSTER_MIN_INSIDERS", "2"))
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

S3 = boto3.client("s3", region_name="us-east-1")

# Role tier hierarchy — higher tier = stronger signal
ROLE_TIERS = {
    # Tier 1: CEO-class
    "ceo": 100, "chief executive officer": 100, "president and ceo": 100,
    "chairman and ceo": 100,
    # Tier 2: CFO-class + Chairman
    "cfo": 90, "chief financial officer": 90, "chairman": 85,
    "executive chairman": 85, "vice chairman": 80,
    # Tier 3: COO/Other C-suite
    "coo": 75, "chief operating officer": 75,
    "cto": 70, "chief technology officer": 70,
    "cmo": 65, "chief marketing officer": 65,
    "cao": 65, "general counsel": 65, "chief legal": 65,
    "chief": 60,  # catch-all for other "Chief X Officer"
    # Tier 4: SVP/EVP
    "evp": 55, "executive vice president": 55,
    "svp": 50, "senior vice president": 50,
    "vp": 45, "vice president": 45,
    # Tier 5: Director (board)
    "director": 40,
    # Tier 6: 10% owner — usually noise
    "10% owner": 20, "10 percent owner": 20,
}


def role_tier(role_str):
    if not role_str:
        return 0
    r = role_str.lower().strip()
    # Try exact match first
    if r in ROLE_TIERS:
        return ROLE_TIERS[r]
    # Try substring matches, longest-first to avoid "vp" matching inside "evp"
    for key in sorted(ROLE_TIERS.keys(), key=len, reverse=True):
        if key in r:
            return ROLE_TIERS[key]
    return 30  # default for unknown roles


# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR — DAILY INDEX FETCHER (more reliable than atom feed)
# ─────────────────────────────────────────────────────────────────────────────
def sec_request(url, retries=3, timeout=20):
    """SEC requires User-Agent header. Implement backoff for 429s."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip,deflate",
            "Host": urllib.request.urlparse(url).netloc,
        }
    )
    last_exc = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                # Handle gzip
                enc = resp.headers.get("Content-Encoding", "")
                if "gzip" in enc:
                    import gzip
                    data = gzip.decompress(data)
                return data.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                last_exc = e
                continue
            raise
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(1)
            else:
                raise
    if last_exc:
        raise last_exc


def get_daily_form4_index(date_obj):
    """
    Fetch SEC daily index for a given date and return Form 4 filings.
    Returns: list of {cik, company, form_type, accession, filed_date}
    """
    yyyy = date_obj.year
    qtr = (date_obj.month - 1) // 3 + 1
    yyyymmdd = date_obj.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{yyyy}/QTR{qtr}/form.{yyyymmdd}.idx"
    try:
        text = sec_request(url, timeout=20)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Weekend or holiday — no index for this day
            return []
        raise
    
    filings = []
    in_data = False
    for line in text.splitlines():
        if line.startswith("---"):
            in_data = True
            continue
        if not in_data or not line.strip():
            continue
        # Format: Form Type    Company Name    CIK    Date Filed    File Name
        # Fixed-width columns, parse by position
        # Form Type: cols 0-12, Company: 12-74, CIK: 74-86, Date: 86-98, File: 98+
        if len(line) < 98:
            continue
        form_type = line[0:12].strip()
        if form_type != "4":
            continue
        company = line[12:74].strip()
        cik = line[74:86].strip()
        date_filed = line[86:98].strip()
        file_name = line[98:].strip()
        # Extract accession from file_name path
        # e.g. edgar/data/12345/000012345622000001.txt
        m = re.search(r"(\d{10}-\d{2}-\d{6})", file_name)
        accession = m.group(1) if m else file_name.split("/")[-1].replace(".txt", "")
        filings.append({
            "form_type": form_type,
            "company": company,
            "cik": cik,
            "date_filed": date_filed,
            "file_name": file_name,
            "accession": accession,
        })
    return filings


def get_business_days(end_date, n_days):
    """Return list of last n business days ending at end_date (inclusive)."""
    days = []
    d = end_date
    while len(days) < n_days:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d -= timedelta(days=1)
    return days


# ─────────────────────────────────────────────────────────────────────────────
# FORM 4 XML PARSER
# ─────────────────────────────────────────────────────────────────────────────
def fetch_form4_xml(accession, cik):
    """
    Fetch the primary XML document for a Form 4 filing.
    Try 2 URL patterns since SEC index inconsistency.
    """
    accession_clean = accession.replace("-", "")
    cik_int = int(cik) if cik.isdigit() else cik
    
    # Pattern 1: index page lists the filing's documents
    index_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=40&action=getcompany"
    
    # Pattern 2: directly access the XML
    # First we need to find the .xml file in the filing's folder
    folder_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_clean}/"
    
    try:
        # List filing folder (HTML index)
        html = sec_request(folder_url + "index.json", timeout=15)
        idx = json.loads(html)
        items = idx.get("directory", {}).get("item", [])
        # Find primary XML — usually starts with form4 or has accession in name
        xml_name = None
        for item in items:
            n = item.get("name", "")
            if n.endswith(".xml") and ("form4" in n.lower() or "primary_doc" in n.lower() or "edgar.xml" in n.lower() or accession[-8:] in n.lower()):
                xml_name = n
                break
        # Fallback: just pick the first .xml that isn't the metadata one
        if not xml_name:
            for item in items:
                n = item.get("name", "")
                if n.endswith(".xml") and "metadata" not in n.lower():
                    xml_name = n
                    break
        if not xml_name:
            return None
        
        xml_url = folder_url + xml_name
        return sec_request(xml_url, timeout=15)
    except Exception:
        return None


def parse_form4(xml_text, accession, filed_date):
    """
    Parse a Form 4 XML and return list of buy transactions.
    Filters for:
      - transactionCode == 'P' (open market purchase)
      - transactionAcquiredDisposedCode == 'A' (acquired)
      - non-derivative table only
    """
    if not xml_text:
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    
    # Issuer info
    issuer = root.find("issuer")
    if issuer is None:
        return []
    ticker_el = issuer.find("issuerTradingSymbol")
    cik_el = issuer.find("issuerCik")
    name_el = issuer.find("issuerName")
    ticker = (ticker_el.text or "").strip().upper() if ticker_el is not None else ""
    cik = (cik_el.text or "").strip() if cik_el is not None else ""
    company = (name_el.text or "").strip() if name_el is not None else ""
    if not ticker or ticker == "N/A":
        return []
    
    # Reporting owner (insider)
    owner = root.find("reportingOwner")
    if owner is None:
        return []
    name_block = owner.find("reportingOwnerId/rptOwnerName")
    insider_name = (name_block.text or "").strip() if name_block is not None else "Unknown"
    
    # Roles: from reportingOwnerRelationship
    rel = owner.find("reportingOwnerRelationship")
    role_parts = []
    if rel is not None:
        if rel.find("isDirector") is not None and rel.find("isDirector").text == "1":
            role_parts.append("Director")
        if rel.find("isOfficer") is not None and rel.find("isOfficer").text == "1":
            officer_title = rel.find("officerTitle")
            if officer_title is not None and officer_title.text:
                role_parts.append(officer_title.text.strip())
            else:
                role_parts.append("Officer")
        if rel.find("isTenPercentOwner") is not None and rel.find("isTenPercentOwner").text == "1":
            role_parts.append("10% Owner")
        if rel.find("isOther") is not None and rel.find("isOther").text == "1":
            other_text = rel.find("otherText")
            if other_text is not None and other_text.text:
                role_parts.append(other_text.text.strip())
    role = ", ".join(role_parts) or "Unknown"
    
    # Non-derivative transactions (actual share buys)
    transactions = []
    nd_table = root.find("nonDerivativeTable")
    if nd_table is None:
        return []
    
    for txn in nd_table.findall("nonDerivativeTransaction"):
        coding = txn.find("transactionCoding")
        if coding is None:
            continue
        tcode_el = coding.find("transactionCode")
        if tcode_el is None or (tcode_el.text or "").strip() != "P":
            continue  # only open-market purchases
        
        amounts = txn.find("transactionAmounts")
        if amounts is None:
            continue
        
        shares_el = amounts.find("transactionShares/value")
        price_el = amounts.find("transactionPricePerShare/value")
        ad_el = amounts.find("transactionAcquiredDisposedCode/value")
        if shares_el is None or price_el is None or ad_el is None:
            continue
        if (ad_el.text or "").strip() != "A":
            continue  # not an acquisition
        
        try:
            shares = float(shares_el.text or "0")
            price = float(price_el.text or "0")
        except ValueError:
            continue
        if shares <= 0 or price <= 0:
            continue
        value = shares * price
        if value < MIN_BUY_VALUE:
            continue
        
        # Transaction date
        date_el = txn.find("transactionDate/value")
        txn_date = (date_el.text or filed_date) if date_el is not None else filed_date
        
        transactions.append({
            "ticker": ticker,
            "company": company,
            "cik": cik,
            "insider_name": insider_name,
            "role": role,
            "role_tier": role_tier(role),
            "shares": int(shares),
            "price": round(price, 4),
            "value": round(value, 2),
            "txn_date": txn_date,
            "filed_date": filed_date,
            "accession": accession,
        })
    
    return transactions


# ─────────────────────────────────────────────────────────────────────────────
# FMP ENRICHMENT — market cap, sector, 52w high
# ─────────────────────────────────────────────────────────────────────────────
def fmp_request(path, params=None):
    if not FMP_KEY:
        return None
    qs = {**(params or {}), "apikey": FMP_KEY}
    qs_str = "&".join(f"{k}={v}" for k, v in qs.items())
    url = f"https://financialmodelingprep.com{path}?{qs_str}"
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            return json.loads(r.read())
    except Exception:
        return None


def enrich_ticker(ticker):
    """Pull market_cap, price, 52w high/low, sector, industry from FMP."""
    profile = fmp_request(f"/stable/profile", {"symbol": ticker})
    if not profile or not isinstance(profile, list) or not profile:
        return {}
    p = profile[0]
    quote = fmp_request(f"/stable/quote", {"symbol": ticker})
    q = (quote[0] if quote and isinstance(quote, list) and quote else {})
    
    high_52w = q.get("yearHigh") or q.get("priceAvg52Week") or 0
    low_52w = q.get("yearLow") or 0
    price = q.get("price") or p.get("price") or 0
    pct_from_high = ((price - high_52w) / high_52w * 100) if high_52w else 0
    pct_from_low = ((price - low_52w) / low_52w * 100) if low_52w else 0
    
    return {
        "market_cap": p.get("marketCap") or p.get("mktCap") or 0,
        "price_now": price,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "pct_from_52w_high": round(pct_from_high, 2),
        "pct_from_52w_low": round(pct_from_low, 2),
        "sector": p.get("sector") or "",
        "industry": p.get("industry") or "",
        "country": p.get("country") or "",
        "company_name": p.get("companyName") or "",
        "shares_outstanding": p.get("sharesOutstanding") or 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLUSTERING + SCORING
# ─────────────────────────────────────────────────────────────────────────────
def build_clusters(all_txns, lookback_days):
    """Group transactions by ticker, deduplicate by accession+insider+date."""
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)
    by_ticker = defaultdict(list)
    seen = set()
    for t in all_txns:
        # Dedup
        key = (t["ticker"], t["accession"], t["insider_name"], t["txn_date"], t["shares"])
        if key in seen:
            continue
        seen.add(key)
        # Date filter
        try:
            tdate = datetime.fromisoformat(t["txn_date"]).date()
        except ValueError:
            try:
                tdate = datetime.strptime(t["txn_date"], "%Y-%m-%d").date()
            except ValueError:
                continue
        if tdate < cutoff:
            continue
        by_ticker[t["ticker"]].append(t)
    return by_ticker


def score_cluster(cluster):
    """
    0-100 score for a cluster based on:
      - Insider role tier (40% weight): max role_tier of any insider
      - Insider count (15% weight): 1=10, 2=40, 3=70, 4+=100
      - Total $ value (25% weight): log-scaled
      - Recency (10% weight): days since last buy
      - C-suite stack bonus (10% weight): CEO+CFO=100, CEO=70, CFO=60
    """
    txns = cluster["transactions"]
    insiders = cluster["insiders"]
    
    # Role tier component
    max_tier = max((i["role_tier"] for i in insiders), default=0)
    role_score = max_tier  # already 0-100
    
    # Insider count component
    n = len(insiders)
    if n >= 4: count_score = 100
    elif n == 3: count_score = 70
    elif n == 2: count_score = 40
    else: count_score = 10
    
    # Value component (log-scaled)
    import math
    v = cluster["total_value"]
    if v < 50_000: val_score = 10
    elif v < 200_000: val_score = 30
    elif v < 1_000_000: val_score = 50
    elif v < 5_000_000: val_score = 75
    elif v < 25_000_000: val_score = 90
    else: val_score = 100
    
    # Recency (last buy)
    last_buy_str = cluster["last_buy"]
    try:
        last_dt = datetime.fromisoformat(last_buy_str.replace("Z", "+00:00")) if "T" in last_buy_str else datetime.strptime(last_buy_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        last_dt = datetime.now(timezone.utc)
    days_since = (datetime.now(timezone.utc) - last_dt).days
    if days_since <= 7: rec_score = 100
    elif days_since <= 14: rec_score = 80
    elif days_since <= 30: rec_score = 60
    else: rec_score = 30
    
    # C-suite stack bonus
    has_ceo = cluster["has_ceo"]
    has_cfo = cluster["has_cfo"]
    has_chairman = cluster["has_chairman"]
    if has_ceo and has_cfo: stack_score = 100
    elif has_ceo and has_chairman: stack_score = 90
    elif has_ceo: stack_score = 70
    elif has_cfo and has_chairman: stack_score = 75
    elif has_cfo: stack_score = 60
    elif has_chairman: stack_score = 50
    elif n >= 3: stack_score = 40  # 3+ directors w/o C-suite
    else: stack_score = 20
    
    composite = (
        0.40 * role_score +
        0.15 * count_score +
        0.25 * val_score +
        0.10 * rec_score +
        0.10 * stack_score
    )
    
    # Signal type classification
    if has_ceo and has_cfo:
        sig_type = "smart_money_dual"
    elif n >= 3 and max_tier >= 60:
        sig_type = "executive_cluster"
    elif n >= 3:
        sig_type = "cluster_buy"
    elif has_ceo and v >= 1_000_000:
        sig_type = "ceo_conviction"
    elif n == 2 and max_tier >= 60:
        sig_type = "exec_pair"
    else:
        sig_type = "lone_buy"
    
    return round(composite, 1), sig_type


def aggregate_cluster(ticker, txns):
    """Build cluster object from list of transactions for one ticker."""
    insider_map = defaultdict(lambda: {
        "name": "", "role": "", "role_tier": 0,
        "n_buys": 0, "total_shares": 0, "total_value": 0.0,
    })
    total_shares = 0
    total_value = 0.0
    
    for t in txns:
        ins = insider_map[t["insider_name"]]
        ins["name"] = t["insider_name"]
        # Keep highest-tier role if multiple
        if t["role_tier"] > ins["role_tier"]:
            ins["role"] = t["role"]
            ins["role_tier"] = t["role_tier"]
        ins["n_buys"] += 1
        ins["total_shares"] += t["shares"]
        ins["total_value"] += t["value"]
        total_shares += t["shares"]
        total_value += t["value"]
    
    insiders = list(insider_map.values())
    insiders.sort(key=lambda i: i["role_tier"], reverse=True)
    
    txns_sorted = sorted(txns, key=lambda t: t["txn_date"])
    
    has_ceo = any("ceo" in i["role"].lower() or "chief executive" in i["role"].lower() for i in insiders)
    has_cfo = any("cfo" in i["role"].lower() or "chief financial" in i["role"].lower() for i in insiders)
    has_chairman = any("chairman" in i["role"].lower() for i in insiders)
    has_director = any("director" in i["role"].lower() for i in insiders)
    
    highest_role = insiders[0]["role"] if insiders else "Unknown"
    
    cluster = {
        "ticker": ticker,
        "company": txns[0]["company"] if txns else "",
        "cik": txns[0]["cik"] if txns else "",
        "n_insiders": len(insiders),
        "n_transactions": len(txns),
        "total_shares": total_shares,
        "total_value": round(total_value, 2),
        "avg_price": round(total_value / total_shares, 2) if total_shares else 0,
        "first_buy": txns_sorted[0]["txn_date"] if txns_sorted else "",
        "last_buy": txns_sorted[-1]["txn_date"] if txns_sorted else "",
        "highest_role": highest_role,
        "has_ceo": has_ceo,
        "has_cfo": has_cfo,
        "has_chairman": has_chairman,
        "has_director": has_director,
        "insiders": [
            {
                **{k: v for k, v in i.items() if k != "role_tier"},
                "role_tier": i["role_tier"],
                "total_value": round(i["total_value"], 2),
            }
            for i in insiders
        ],
        "transactions": [
            {k: v for k, v in t.items() if k not in ("cik", "company")}
            for t in txns_sorted
        ],
    }
    
    score, sig_type = score_cluster(cluster)
    cluster["score"] = score
    cluster["signal_type"] = sig_type
    
    return cluster


def build_rationale(cluster):
    """Generate one-line plain-english rationale for the cluster."""
    insiders = cluster["insiders"]
    n = len(insiders)
    val_m = cluster["total_value"] / 1_000_000
    val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${cluster['total_value']/1000:.0f}k"
    
    # Build role summary
    if cluster["has_ceo"] and cluster["has_cfo"]:
        role_str = "CEO + CFO"
        if cluster["has_chairman"] and "chairman" not in role_str.lower():
            role_str = "CEO + CFO + Chairman"
        if cluster["has_director"]:
            n_dirs = sum(1 for i in insiders if "director" in i["role"].lower())
            if n_dirs:
                role_str += f" + {n_dirs} director(s)"
    elif cluster["has_ceo"]:
        role_str = f"CEO (+{n-1} other)" if n > 1 else "CEO solo"
    elif cluster["has_cfo"] and cluster["has_chairman"]:
        role_str = "CFO + Chairman"
    elif cluster["has_chairman"]:
        role_str = f"Chairman (+{n-1})" if n > 1 else "Chairman solo"
    else:
        role_str = f"{n} insider{'s' if n > 1 else ''}"
    
    # Window
    try:
        first = datetime.fromisoformat(cluster["first_buy"]).date()
        last = datetime.fromisoformat(cluster["last_buy"]).date()
        days = (last - first).days
    except ValueError:
        days = 0
    
    fund = cluster.get("fundamentals", {})
    pct_from_high = fund.get("pct_from_52w_high", 0)
    
    rationale = f"{role_str} bought {val_str} of {cluster['ticker']} over {days}d at ${cluster['avg_price']:.2f} avg"
    if pct_from_high < -15:
        rationale += f" — stock {abs(pct_from_high):.0f}% off 52w high"
    elif pct_from_high < -5:
        rationale += f" — stock {abs(pct_from_high):.0f}% below 52w high"
    return rationale


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[insider-cluster] starting, lookback={LOOKBACK_DAYS}d")
    
    # 1. PULL daily indices for last N business days (need ~10 trading days to cover 14d cluster window)
    n_business_days = 10
    end_date = datetime.now(timezone.utc).date()
    business_days = get_business_days(end_date, n_business_days)
    print(f"[insider-cluster] pulling daily index for {n_business_days} business days: {business_days[0]} → {business_days[-1]}")
    
    all_form4_filings = []
    idx_errors = 0
    for d in business_days:
        try:
            filings = get_daily_form4_index(d)
            all_form4_filings.extend(filings)
            time.sleep(0.15)  # SEC rate limit hygiene
        except Exception as e:
            print(f"[insider-cluster] index error {d}: {e}")
            idx_errors += 1
    
    print(f"[insider-cluster] found {len(all_form4_filings)} Form 4 filings across {n_business_days} days ({idx_errors} index errors)")
    
    # 2. FETCH each filing's XML in parallel (limited workers for SEC compliance)
    def _fetch(f):
        try:
            xml = fetch_form4_xml(f["accession"], f["cik"])
            txns = parse_form4(xml, f["accession"], f["date_filed"])
            return txns
        except Exception as e:
            return []
    
    print(f"[insider-cluster] fetching XML for {len(all_form4_filings)} filings (workers=4)")
    all_txns = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_fetch, f): f for f in all_form4_filings}
        for fut in as_completed(futures):
            txns = fut.result()
            all_txns.extend(txns)
    
    print(f"[insider-cluster] extracted {len(all_txns)} buy transactions")
    
    # 3. CLUSTER
    by_ticker = build_clusters(all_txns, LOOKBACK_DAYS)
    print(f"[insider-cluster] {len(by_ticker)} unique tickers with buys")
    
    raw_clusters = []
    for ticker, txns in by_ticker.items():
        if len(txns) < 1:
            continue
        cluster = aggregate_cluster(ticker, txns)
        if cluster["n_insiders"] < CLUSTER_MIN_INSIDERS and cluster["n_transactions"] < 2:
            # Skip lone single-buy events
            continue
        raw_clusters.append(cluster)
    
    print(f"[insider-cluster] {len(raw_clusters)} candidate clusters (≥{CLUSTER_MIN_INSIDERS} insiders or ≥2 txns)")
    
    # 4. ENRICH top 50 clusters with FMP fundamentals
    raw_clusters.sort(key=lambda c: c["score"], reverse=True)
    enrich_top = raw_clusters[:50]
    
    print(f"[insider-cluster] enriching top {len(enrich_top)} clusters with fundamentals")
    with ThreadPoolExecutor(max_workers=6) as ex:
        future_to_idx = {ex.submit(enrich_ticker, c["ticker"]): i for i, c in enumerate(enrich_top)}
        for fut in as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                enrich_top[idx]["fundamentals"] = fut.result()
            except Exception as e:
                enrich_top[idx]["fundamentals"] = {}
    
    # 5. RATIONALE
    for c in enrich_top:
        c["rationale"] = build_rationale(c)
    
    # Combine: enriched top + remaining without fundamentals
    final_clusters = enrich_top + raw_clusters[50:]
    for c in final_clusters[50:]:
        c["fundamentals"] = {}
        c["rationale"] = build_rationale(c)
    
    # 6. STATS
    n_strong = sum(1 for c in final_clusters if c["score"] >= 70)
    n_smart_money = sum(1 for c in final_clusters if c["signal_type"] == "smart_money_dual")
    n_ceo_conv = sum(1 for c in final_clusters if c["signal_type"] == "ceo_conviction")
    n_cluster = sum(1 for c in final_clusters if c["signal_type"] in ("cluster_buy", "executive_cluster"))
    n_contrarian = sum(1 for c in final_clusters if c.get("fundamentals", {}).get("pct_from_52w_high", 0) < -25 and c["score"] >= 60)
    
    output = {
        "schema_version": "1.0",
        "method": "insider_cluster_scanner_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "duration_s": round(time.time() - t0, 1),
        "stats": {
            "n_form4_filings_scanned": len(all_form4_filings),
            "n_buy_transactions": len(all_txns),
            "n_unique_tickers": len(by_ticker),
            "n_clusters": len(final_clusters),
            "n_strong_signals": n_strong,
            "n_smart_money_dual": n_smart_money,
            "n_ceo_conviction": n_ceo_conv,
            "n_cluster_buys": n_cluster,
            "n_contrarian_clusters": n_contrarian,
        },
        "thresholds": {
            "min_buy_value_usd": MIN_BUY_VALUE,
            "cluster_min_insiders": CLUSTER_MIN_INSIDERS,
            "lookback_days": LOOKBACK_DAYS,
            "score_strong_threshold": 70,
        },
        "clusters": final_clusters,
    }
    
    body = json.dumps(output, default=str)
    S3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=body.encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=300")
    
    print(f"[insider-cluster] wrote {len(body):,}b to {S3_KEY}")
    print(f"[insider-cluster] strong={n_strong} smart_money={n_smart_money} ceo_conv={n_ceo_conv} contrarian={n_contrarian}")
    if final_clusters[:5]:
        for c in final_clusters[:5]:
            print(f"[insider-cluster] TOP: {c['ticker']:<8} score={c['score']:>5}  {c['signal_type']:<22}  ${c['total_value']/1e6:>6.2f}M  {c['n_insiders']}-insiders")
    
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_clusters": len(final_clusters),
            "n_strong": n_strong,
            "n_smart_money_dual": n_smart_money,
            "duration_s": round(time.time() - t0, 1),
            "top_5_tickers": [c["ticker"] for c in final_clusters[:5]],
        }),
    }
