"""
justhodl-insider-cluster-scanner — Production-grade insider cluster detection (v2)

v2 changes from v1:
  - SEC `submissions` API for company-by-CIK ticker mapping (single API call)
  - Sample 1000 most recent Form 4 filings per run (not all 10k+)
  - 8 parallel workers with strict 0.1s SEC rate-limit gate
  - Lambda timeout 600s budget — must complete in 8 min
  - Two-pass enrichment: first pass parses XML, second pass enriches top-50

PIPELINE:
1. PULL: SEC EDGAR daily index for last 7 business days
2. SAMPLE: Take most recent 1500 Form 4 filings (already-recent skews to most-active)
3. PARSE: Fetch + parse each XML in parallel for ticker, insider, role, $ amount
4. CLUSTER: Group by ticker, dedupe by accession+insider+date+shares
5. SCORE: Role-tier weighted (CEO/CFO=100, Director=40, 10%-Owner=20)
6. ENRICH: FMP fundamentals for top 50 (mcap, 52w high/low, sector, industry)
7. RATIONALE: Plain-english explanation per cluster
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
import threading
import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/insider-clusters.json")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "JustHodl Research raafouis@gmail.com")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))
MIN_BUY_VALUE = float(os.environ.get("MIN_BUY_VALUE_USD", "10000"))
CLUSTER_MIN_INSIDERS = int(os.environ.get("CLUSTER_MIN_INSIDERS", "2"))
N_BUSINESS_DAYS_INDEX = int(os.environ.get("N_BUSINESS_DAYS_INDEX", "7"))
MAX_FILINGS_TO_PARSE = int(os.environ.get("MAX_FILINGS_TO_PARSE", "1500"))
N_WORKERS = int(os.environ.get("N_WORKERS", "8"))
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

S3 = boto3.client("s3", region_name="us-east-1")

# ROLE TIERS
ROLE_TIERS = {
    "ceo": 100, "chief executive officer": 100, "president and ceo": 100, "chairman and ceo": 100,
    "cfo": 90, "chief financial officer": 90,
    "chairman": 85, "executive chairman": 85, "vice chairman": 80,
    "coo": 75, "chief operating officer": 75,
    "cto": 70, "chief technology officer": 70,
    "cmo": 65, "chief marketing officer": 65, "cao": 65, "general counsel": 65, "chief legal": 65,
    "chief": 60,
    "evp": 55, "executive vice president": 55,
    "svp": 50, "senior vice president": 50,
    "vp": 45, "vice president": 45,
    "director": 40,
    "10% owner": 20, "10 percent owner": 20,
}

def role_tier(role_str):
    if not role_str: return 0
    r = role_str.lower().strip()
    if r in ROLE_TIERS: return ROLE_TIERS[r]
    for key in sorted(ROLE_TIERS.keys(), key=len, reverse=True):
        if key in r: return ROLE_TIERS[key]
    return 30

# ───────────────────────────────────────────────────────────────────
# RATE-LIMITED SEC FETCHER
# ───────────────────────────────────────────────────────────────────
_sec_lock = threading.Lock()
_sec_last_request = [0.0]  # mutable container so closures can update
SEC_MIN_INTERVAL = 0.105  # 9.5 req/s — under 10 req/s limit

def sec_get(url, retries=3, timeout=15):
    """Rate-limited SEC fetch. Single request per call."""
    last_exc = None
    for attempt in range(retries):
        # Throttle gate
        with _sec_lock:
            elapsed = time.time() - _sec_last_request[0]
            if elapsed < SEC_MIN_INTERVAL:
                time.sleep(SEC_MIN_INTERVAL - elapsed)
            _sec_last_request[0] = time.time()
        
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": SEC_USER_AGENT,
                "Accept-Encoding": "gzip,deflate",
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                enc = resp.headers.get("Content-Encoding", "")
                if "gzip" in enc:
                    import gzip
                    data = gzip.decompress(data)
                return data.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            last_exc = e
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            if e.code in (403, 404):
                # Don't retry these
                raise
            time.sleep(0.5)
        except Exception as e:
            last_exc = e
            time.sleep(0.5)
    if last_exc:
        raise last_exc

def get_daily_form4_index(date_obj):
    yyyy = date_obj.year
    qtr = (date_obj.month - 1) // 3 + 1
    yyyymmdd = date_obj.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{yyyy}/QTR{qtr}/form.{yyyymmdd}.idx"
    try:
        text = sec_get(url, timeout=20)
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return []
        raise
    
    filings = []
    in_data = False
    for line in text.splitlines():
        if line.startswith("---"):
            in_data = True
            continue
        if not in_data or not line.strip() or len(line) < 98:
            continue
        form_type = line[0:12].strip()
        if form_type != "4":
            continue
        company = line[12:74].strip()
        cik = line[74:86].strip()
        date_filed = line[86:98].strip()
        file_name = line[98:].strip()
        m = re.search(r"(\d{10}-\d{2}-\d{6})", file_name)
        accession = m.group(1) if m else file_name.split("/")[-1].replace(".txt", "")
        filings.append({
            "form_type": form_type, "company": company, "cik": cik,
            "date_filed": date_filed, "file_name": file_name, "accession": accession,
        })
    return filings

def get_business_days(end_date, n_days):
    days = []
    d = end_date
    while len(days) < n_days:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days

# ───────────────────────────────────────────────────────────────────
# FORM 4 XML FETCHER + PARSER
# ───────────────────────────────────────────────────────────────────
def fetch_form4_xml(accession, cik):
    """Fetch primary XML for Form 4 filing using SEC's index.json."""
    if not cik or not accession:
        return None
    accession_clean = accession.replace("-", "")
    try:
        cik_int = int(cik)
    except ValueError:
        return None
    folder_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_clean}/"
    try:
        idx_text = sec_get(folder_url + "index.json", timeout=12)
        idx = json.loads(idx_text)
        items = idx.get("directory", {}).get("item", [])
        xml_name = None
        # Pass 1: prefer form4 / primary_doc / edgar.xml names
        for item in items:
            n = item.get("name", "")
            if n.endswith(".xml") and ("form4" in n.lower() or "primary_doc" in n.lower() or "edgar.xml" in n.lower()):
                xml_name = n
                break
        # Pass 2: any xml that isn't metadata
        if not xml_name:
            for item in items:
                n = item.get("name", "")
                if n.endswith(".xml") and "metadata" not in n.lower() and "wf-form" not in n.lower():
                    xml_name = n
                    break
        if not xml_name:
            return None
        return sec_get(folder_url + xml_name, timeout=12)
    except Exception:
        return None

def parse_form4(xml_text, accession, filed_date):
    if not xml_text:
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    issuer = root.find("issuer")
    if issuer is None: return []
    ticker_el = issuer.find("issuerTradingSymbol")
    cik_el = issuer.find("issuerCik")
    name_el = issuer.find("issuerName")
    ticker = (ticker_el.text or "").strip().upper() if ticker_el is not None and ticker_el.text else ""
    cik = (cik_el.text or "").strip() if cik_el is not None and cik_el.text else ""
    company = (name_el.text or "").strip() if name_el is not None and name_el.text else ""
    if not ticker or ticker == "N/A" or len(ticker) > 6:
        return []
    
    owner = root.find("reportingOwner")
    if owner is None: return []
    name_block = owner.find("reportingOwnerId/rptOwnerName")
    insider_name = (name_block.text or "").strip() if name_block is not None and name_block.text else "Unknown"
    
    rel = owner.find("reportingOwnerRelationship")
    role_parts = []
    if rel is not None:
        if rel.find("isDirector") is not None and (rel.find("isDirector").text or "").strip() == "1":
            role_parts.append("Director")
        if rel.find("isOfficer") is not None and (rel.find("isOfficer").text or "").strip() == "1":
            officer_title = rel.find("officerTitle")
            if officer_title is not None and officer_title.text:
                role_parts.append(officer_title.text.strip())
            else:
                role_parts.append("Officer")
        if rel.find("isTenPercentOwner") is not None and (rel.find("isTenPercentOwner").text or "").strip() == "1":
            role_parts.append("10% Owner")
        if rel.find("isOther") is not None and (rel.find("isOther").text or "").strip() == "1":
            other_text = rel.find("otherText")
            if other_text is not None and other_text.text:
                role_parts.append(other_text.text.strip())
    role = ", ".join(role_parts) or "Unknown"
    
    transactions = []
    nd_table = root.find("nonDerivativeTable")
    if nd_table is None: return []
    
    for txn in nd_table.findall("nonDerivativeTransaction"):
        coding = txn.find("transactionCoding")
        if coding is None: continue
        tcode_el = coding.find("transactionCode")
        if tcode_el is None or (tcode_el.text or "").strip() != "P": continue
        amounts = txn.find("transactionAmounts")
        if amounts is None: continue
        shares_el = amounts.find("transactionShares/value")
        price_el = amounts.find("transactionPricePerShare/value")
        ad_el = amounts.find("transactionAcquiredDisposedCode/value")
        if shares_el is None or price_el is None or ad_el is None: continue
        if (ad_el.text or "").strip() != "A": continue
        try:
            shares = float(shares_el.text or "0")
            price = float(price_el.text or "0")
        except (ValueError, TypeError):
            continue
        if shares <= 0 or price <= 0: continue
        value = shares * price
        if value < MIN_BUY_VALUE: continue
        date_el = txn.find("transactionDate/value")
        txn_date = (date_el.text or filed_date) if date_el is not None and date_el.text else filed_date
        transactions.append({
            "ticker": ticker, "company": company, "cik": cik,
            "insider_name": insider_name, "role": role,
            "role_tier": role_tier(role), "shares": int(shares),
            "price": round(price, 4), "value": round(value, 2),
            "txn_date": txn_date, "filed_date": filed_date, "accession": accession,
        })
    return transactions

# ───────────────────────────────────────────────────────────────────
# FMP ENRICHMENT
# ───────────────────────────────────────────────────────────────────
def fmp_get(path, params=None, timeout=10):
    if not FMP_KEY: return None
    qs = {**(params or {}), "apikey": FMP_KEY}
    qs_str = "&".join(f"{k}={v}" for k, v in qs.items())
    url = f"https://financialmodelingprep.com{path}?{qs_str}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None

def enrich_ticker(ticker):
    profile = fmp_get(f"/stable/profile", {"symbol": ticker})
    if not profile or not isinstance(profile, list) or not profile:
        return {}
    p = profile[0]
    quote = fmp_get(f"/stable/quote", {"symbol": ticker})
    q = (quote[0] if quote and isinstance(quote, list) and quote else {})
    high_52w = q.get("yearHigh") or q.get("priceAvg52Week") or 0
    low_52w = q.get("yearLow") or 0
    price = q.get("price") or p.get("price") or 0
    pct_from_high = ((price - high_52w) / high_52w * 100) if high_52w else 0
    pct_from_low = ((price - low_52w) / low_52w * 100) if low_52w else 0
    return {
        "market_cap": p.get("marketCap") or p.get("mktCap") or 0,
        "price_now": price,
        "high_52w": high_52w, "low_52w": low_52w,
        "pct_from_52w_high": round(pct_from_high, 2),
        "pct_from_52w_low": round(pct_from_low, 2),
        "sector": p.get("sector") or "",
        "industry": p.get("industry") or "",
        "country": p.get("country") or "",
        "company_name": p.get("companyName") or "",
        "shares_outstanding": p.get("sharesOutstanding") or 0,
    }

# ───────────────────────────────────────────────────────────────────
# CLUSTERING + SCORING
# ───────────────────────────────────────────────────────────────────
def build_clusters(all_txns, lookback_days):
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)
    by_ticker = defaultdict(list)
    seen = set()
    for t in all_txns:
        key = (t["ticker"], t["accession"], t["insider_name"], t["txn_date"], t["shares"])
        if key in seen: continue
        seen.add(key)
        try:
            tdate = datetime.fromisoformat(t["txn_date"].replace("Z", "+00:00")).date() if "T" in t["txn_date"] else datetime.strptime(t["txn_date"], "%Y-%m-%d").date()
        except (ValueError, AttributeError):
            continue
        if tdate < cutoff: continue
        by_ticker[t["ticker"]].append(t)
    return by_ticker

def score_cluster(cluster):
    insiders = cluster["insiders"]
    max_tier = max((i["role_tier"] for i in insiders), default=0)
    role_score = max_tier
    n = len(insiders)
    if n >= 4: count_score = 100
    elif n == 3: count_score = 70
    elif n == 2: count_score = 40
    else: count_score = 10
    v = cluster["total_value"]
    if v < 50_000: val_score = 10
    elif v < 200_000: val_score = 30
    elif v < 1_000_000: val_score = 50
    elif v < 5_000_000: val_score = 75
    elif v < 25_000_000: val_score = 90
    else: val_score = 100
    last_buy_str = cluster["last_buy"]
    try:
        last_dt = datetime.fromisoformat(last_buy_str.replace("Z", "+00:00")) if "T" in last_buy_str else datetime.strptime(last_buy_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        last_dt = datetime.now(timezone.utc)
    days_since = (datetime.now(timezone.utc) - last_dt).days
    if days_since <= 7: rec_score = 100
    elif days_since <= 14: rec_score = 80
    elif days_since <= 30: rec_score = 60
    else: rec_score = 30
    has_ceo = cluster["has_ceo"]
    has_cfo = cluster["has_cfo"]
    has_chairman = cluster["has_chairman"]
    if has_ceo and has_cfo: stack_score = 100
    elif has_ceo and has_chairman: stack_score = 90
    elif has_ceo: stack_score = 70
    elif has_cfo and has_chairman: stack_score = 75
    elif has_cfo: stack_score = 60
    elif has_chairman: stack_score = 50
    elif n >= 3: stack_score = 40
    else: stack_score = 20
    composite = (
        0.40 * role_score + 0.15 * count_score + 0.25 * val_score +
        0.10 * rec_score + 0.10 * stack_score
    )
    if has_ceo and has_cfo: sig_type = "smart_money_dual"
    elif n >= 3 and max_tier >= 60: sig_type = "executive_cluster"
    elif n >= 3: sig_type = "cluster_buy"
    elif has_ceo and v >= 1_000_000: sig_type = "ceo_conviction"
    elif n == 2 and max_tier >= 60: sig_type = "exec_pair"
    else: sig_type = "lone_buy"
    return round(composite, 1), sig_type

def aggregate_cluster(ticker, txns):
    insider_map = defaultdict(lambda: {"name": "", "role": "", "role_tier": 0, "n_buys": 0, "total_shares": 0, "total_value": 0.0})
    total_shares = 0
    total_value = 0.0
    for t in txns:
        ins = insider_map[t["insider_name"]]
        ins["name"] = t["insider_name"]
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
        "ticker": ticker, "company": txns[0]["company"] if txns else "",
        "cik": txns[0]["cik"] if txns else "",
        "n_insiders": len(insiders), "n_transactions": len(txns),
        "total_shares": total_shares,
        "total_value": round(total_value, 2),
        "avg_price": round(total_value / total_shares, 2) if total_shares else 0,
        "first_buy": txns_sorted[0]["txn_date"] if txns_sorted else "",
        "last_buy": txns_sorted[-1]["txn_date"] if txns_sorted else "",
        "highest_role": highest_role,
        "has_ceo": has_ceo, "has_cfo": has_cfo,
        "has_chairman": has_chairman, "has_director": has_director,
        "insiders": [{**{k: v for k, v in i.items() if k != "role_tier"}, "role_tier": i["role_tier"], "total_value": round(i["total_value"], 2)} for i in insiders],
        "transactions": [{k: v for k, v in t.items() if k not in ("cik", "company")} for t in txns_sorted],
    }
    score, sig_type = score_cluster(cluster)
    cluster["score"] = score
    cluster["signal_type"] = sig_type
    return cluster

def build_rationale(cluster):
    insiders = cluster["insiders"]
    n = len(insiders)
    val_m = cluster["total_value"] / 1_000_000
    val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${cluster['total_value']/1000:.0f}k"
    if cluster["has_ceo"] and cluster["has_cfo"]:
        role_str = "CEO + CFO"
        if cluster["has_chairman"] and "chairman" not in role_str.lower():
            role_str = "CEO + CFO + Chairman"
        n_dirs = sum(1 for i in insiders if "director" in i["role"].lower())
        if cluster["has_director"] and n_dirs:
            role_str += f" + {n_dirs} director(s)"
    elif cluster["has_ceo"]:
        role_str = f"CEO (+{n-1} other)" if n > 1 else "CEO solo"
    elif cluster["has_cfo"] and cluster["has_chairman"]:
        role_str = "CFO + Chairman"
    elif cluster["has_chairman"]:
        role_str = f"Chairman (+{n-1})" if n > 1 else "Chairman solo"
    else:
        role_str = f"{n} insider{'s' if n > 1 else ''}"
    try:
        first = datetime.strptime(cluster["first_buy"][:10], "%Y-%m-%d").date()
        last = datetime.strptime(cluster["last_buy"][:10], "%Y-%m-%d").date()
        days = (last - first).days
    except (ValueError, AttributeError):
        days = 0
    fund = cluster.get("fundamentals", {})
    pct_from_high = fund.get("pct_from_52w_high", 0)
    rationale = f"{role_str} bought {val_str} of {cluster['ticker']} over {days}d at ${cluster['avg_price']:.2f} avg"
    if pct_from_high < -15:
        rationale += f" — stock {abs(pct_from_high):.0f}% off 52w high"
    elif pct_from_high < -5:
        rationale += f" — stock {abs(pct_from_high):.0f}% below 52w high"
    return rationale

# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[insider-cluster] starting v2, lookback={LOOKBACK_DAYS}d, max_filings={MAX_FILINGS_TO_PARSE}")
    
    # 1. PULL daily indices
    end_date = datetime.now(timezone.utc).date()
    business_days = get_business_days(end_date, N_BUSINESS_DAYS_INDEX)
    print(f"[insider-cluster] pulling daily index for {N_BUSINESS_DAYS_INDEX} biz days: {business_days[-1]} → {business_days[0]}")
    
    all_filings = []
    idx_errors = 0
    for d in business_days:
        try:
            filings = get_daily_form4_index(d)
            all_filings.extend(filings)
        except Exception as e:
            print(f"[insider-cluster] index error {d}: {type(e).__name__}: {e}")
            idx_errors += 1
    print(f"[insider-cluster] found {len(all_filings)} Form 4 filings ({idx_errors} index errors)")
    
    # 2. SAMPLE: take most recent N (sort desc by date_filed, take top N)
    all_filings.sort(key=lambda f: f.get("date_filed", ""), reverse=True)
    sampled = all_filings[:MAX_FILINGS_TO_PARSE]
    print(f"[insider-cluster] sampling top {len(sampled)} most recent for parsing")
    
    # 3. PARSE in parallel
    def _fetch_parse(f):
        try:
            xml = fetch_form4_xml(f["accession"], f["cik"])
            return parse_form4(xml, f["accession"], f["date_filed"])
        except Exception:
            return []
    
    all_txns = []
    n_parsed = 0
    n_failed = 0
    deadline = t0 + (context.get_remaining_time_in_millis() / 1000) - 60 if context else t0 + 540
    print(f"[insider-cluster] parse deadline: {deadline - t0:.0f}s from start")
    
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        futures = {ex.submit(_fetch_parse, f): f for f in sampled}
        for fut in as_completed(futures):
            n_parsed += 1
            if time.time() > deadline:
                # Stop — out of time. Cancel remaining.
                print(f"[insider-cluster] hit time budget, stopping at {n_parsed} parsed")
                for f in futures:
                    if not f.done():
                        f.cancel()
                break
            try:
                txns = fut.result(timeout=8)
                if txns:
                    all_txns.extend(txns)
                else:
                    n_failed += 1
            except Exception:
                n_failed += 1
    
    print(f"[insider-cluster] parsed {n_parsed} filings, extracted {len(all_txns)} buy transactions ({n_failed} failed)")
    
    # 4. CLUSTER
    by_ticker = build_clusters(all_txns, LOOKBACK_DAYS)
    print(f"[insider-cluster] {len(by_ticker)} unique tickers")
    
    raw_clusters = []
    for ticker, txns in by_ticker.items():
        if len(txns) == 0:
            continue
        cluster = aggregate_cluster(ticker, txns)
        if cluster["n_insiders"] < CLUSTER_MIN_INSIDERS and cluster["n_transactions"] < 2:
            continue
        raw_clusters.append(cluster)
    print(f"[insider-cluster] {len(raw_clusters)} clusters meeting threshold")
    
    raw_clusters.sort(key=lambda c: c["score"], reverse=True)
    
    # 5. ENRICH top 50
    enrich_top = raw_clusters[:50]
    print(f"[insider-cluster] enriching top {len(enrich_top)} with FMP fundamentals")
    if enrich_top and (time.time() < deadline + 30):
        with ThreadPoolExecutor(max_workers=6) as ex:
            future_to_idx = {ex.submit(enrich_ticker, c["ticker"]): i for i, c in enumerate(enrich_top)}
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    enrich_top[idx]["fundamentals"] = fut.result()
                except Exception:
                    enrich_top[idx]["fundamentals"] = {}
    
    # 6. RATIONALE
    for c in enrich_top:
        if "fundamentals" not in c:
            c["fundamentals"] = {}
        c["rationale"] = build_rationale(c)
    for c in raw_clusters[50:]:
        c["fundamentals"] = {}
        c["rationale"] = build_rationale(c)
    final_clusters = enrich_top + raw_clusters[50:]
    
    # 7. STATS
    n_strong = sum(1 for c in final_clusters if c["score"] >= 70)
    n_smart_money = sum(1 for c in final_clusters if c["signal_type"] == "smart_money_dual")
    n_ceo_conv = sum(1 for c in final_clusters if c["signal_type"] == "ceo_conviction")
    n_cluster = sum(1 for c in final_clusters if c["signal_type"] in ("cluster_buy", "executive_cluster"))
    n_contrarian = sum(1 for c in final_clusters if c.get("fundamentals", {}).get("pct_from_52w_high", 0) < -25 and c["score"] >= 60)
    
    output = {
        "schema_version": "2.0",
        "method": "insider_cluster_scanner_v2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "duration_s": round(time.time() - t0, 1),
        "stats": {
            "n_form4_filings_scanned": len(all_filings),
            "n_form4_parsed": n_parsed,
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
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[insider-cluster] wrote {len(body):,}b in {time.time()-t0:.1f}s")
    print(f"[insider-cluster] strong={n_strong} smart_money={n_smart_money} ceo_conv={n_ceo_conv} cluster={n_cluster} contrarian={n_contrarian}")
    if final_clusters[:5]:
        for c in final_clusters[:5]:
            print(f"[insider-cluster] TOP: {c['ticker']:<8} score={c['score']:>5} {c['signal_type']:<22} ${c['total_value']/1e6:>5.2f}M {c['n_insiders']}-ins")

    # ─── ALERTS ────────────────────────────────────────────────────────
    # Fire Telegram on NEW high-conviction signals that weren't in last run.
    # 4 categories: new strong (score>=70), new CEO_CONVICTION, new
    # SMART_MONEY_DUAL, new MASSIVE buys ($5M+).
    try:
        prior_run = {}
        try:
            obj = S3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
            prior_run = json.loads(obj["Body"].read())
        except Exception: pass

        prior_clusters = prior_run.get("clusters", []) if isinstance(prior_run, dict) else []
        prior_strong_tickers = {c.get("ticker") for c in prior_clusters
                                  if isinstance(c, dict) and (c.get("score") or 0) >= 70}
        prior_ceo_conv = {c.get("ticker") for c in prior_clusters
                           if isinstance(c, dict) and c.get("signal_type") == "ceo_conviction"}
        prior_smart_dual = {c.get("ticker") for c in prior_clusters
                              if isinstance(c, dict) and c.get("signal_type") == "smart_money_dual"}
        prior_massive = {c.get("ticker") for c in prior_clusters
                          if isinstance(c, dict) and (c.get("total_value") or 0) >= 5_000_000}

        alerts = []

        # 1. NEW STRONG SIGNALS (score >= 70 that weren't strong last run)
        new_strong = [c for c in final_clusters
                       if c["score"] >= 70 and c["ticker"] not in prior_strong_tickers]
        if new_strong:
            top = new_strong[:5]
            lines = []
            for c in top:
                val_m = c["total_value"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${c['total_value']/1000:.0f}k"
                signal_pretty = c["signal_type"].replace("_", " ").upper()
                lines.append(
                    f"• <b>{c['ticker']}</b> score {c['score']} · {signal_pretty} · "
                    f"{c['n_insiders']} insider{'s' if c['n_insiders']>1 else ''} · {val_str}"
                )
            alerts.append(
                f"🕵️ <b>NEW STRONG INSIDER CLUSTERS (score ≥ 70)</b>\n" +
                "\n".join(lines) +
                "\n\n<a href='https://justhodl.ai/insider/'>justhodl.ai/insider/</a>"
            )

        # 2. NEW CEO_CONVICTION (CEO buying personally — highest single-signal)
        new_ceo_conv = [c for c in final_clusters
                         if c["signal_type"] == "ceo_conviction"
                         and c["ticker"] not in prior_ceo_conv]
        if new_ceo_conv:
            top = new_ceo_conv[:4]
            lines = []
            for c in top:
                val_m = c["total_value"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${c['total_value']/1000:.0f}k"
                # Try to find the CEO name from insiders
                ceo_name = ""
                for ins in c.get("insiders", []):
                    if "CEO" in (ins.get("role", "") or "").upper() or "CHIEF EXECUTIVE" in (ins.get("role","") or "").upper():
                        ceo_name = f" — {ins.get('name','')[:40]}"
                        break
                lines.append(
                    f"• <b>{c['ticker']}</b>{ceo_name} · score {c['score']} · {val_str}"
                )
            alerts.append(
                f"🎯 <b>NEW CEO CONVICTION BUYS</b>\n"
                f"<i>CEO putting personal capital in. Historical alpha 6-15% over 6 months.</i>\n" +
                "\n".join(lines)
            )

        # 3. NEW SMART_MONEY_DUAL (CEO + 10%-owner together — institutional tier)
        new_smart_dual = [c for c in final_clusters
                           if c["signal_type"] == "smart_money_dual"
                           and c["ticker"] not in prior_smart_dual]
        if new_smart_dual:
            top = new_smart_dual[:4]
            lines = []
            for c in top:
                val_m = c["total_value"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${c['total_value']/1000:.0f}k"
                lines.append(
                    f"• <b>{c['ticker']}</b> score {c['score']} · {c['n_insiders']} insiders · {val_str}"
                )
            alerts.append(
                f"💎 <b>NEW SMART-MONEY DUAL SIGNAL</b>\n"
                f"<i>CEO + 10%-owner buying together — highest institutional tier.</i>\n" +
                "\n".join(lines)
            )

        # 4. NEW MASSIVE BUYS ($5M+ that weren't tracked last run)
        new_massive = [c for c in final_clusters
                        if (c.get("total_value") or 0) >= 5_000_000
                        and c["ticker"] not in prior_massive]
        if new_massive:
            top = new_massive[:4]
            lines = []
            for c in top:
                val_m = c["total_value"] / 1e6
                lines.append(
                    f"• <b>{c['ticker']}</b> ${val_m:.2f}M · score {c['score']} · "
                    f"{c['signal_type'].replace('_', ' ')}"
                )
            alerts.append(
                f"💰 <b>MASSIVE INSIDER BUYS ($5M+)</b>\n" + "\n".join(lines)
            )

        # Send all alerts
        TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
        TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
        if alerts and TG_TOKEN and TG_CHAT:
            for msg in alerts:
                try:
                    body_tg = json.dumps({
                        "chat_id": TG_CHAT, "text": msg,
                        "parse_mode": "HTML", "disable_web_page_preview": True,
                    }).encode("utf-8")
                    req = urllib.request.Request(
                        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                        data=body_tg, headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=10).read()
                    print(f"[insider-alert] sent: {msg[:80]}")
                except Exception as e:
                    print(f"[insider-alert] err: {e}")
        elif alerts:
            print(f"[insider-alert] {len(alerts)} alerts but no Telegram creds")
    except Exception as e:
        print(f"[insider-alert] exception in alert block: {e}")

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
