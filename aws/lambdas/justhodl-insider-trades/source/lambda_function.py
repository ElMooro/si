"""
justhodl-insider-trades — SEC EDGAR Form 4 pipeline

Pulls the last ~200 Form 4 filings from SEC EDGAR's atom feed, parses each
filing's structured XML for transaction data, filters for open-market purchases
above a value threshold, aggregates cluster buys (multiple insiders in the
same ticker within a rolling window), and writes a normalized JSON to S3.

Data flow:
    SEC EDGAR atom feed   →   filing index pages   →   primary XML docs
                          →   transaction records
                          →   merge with rolling 30d window from S3
                          →   compute cluster buys + sector rollup
                          →   write data/insider-trades.json

Design decisions
================
- Pure Python stdlib + boto3. No dependencies. Lambda zip stays tiny.
- Concurrent fetch with ThreadPoolExecutor (workers=8) — SEC rate limit is
  10 req/sec; at 8 workers + small sleeps we stay under it.
- User-Agent header is REQUIRED by SEC. Without it the response is 403.
- Rolling 30d window: read existing JSON from S3, append new, dedupe by
  accession_no, drop entries older than 30d. This means each Lambda
  invocation is incremental (cheap) and the file grows then stabilizes.
- Schedule: every 30 min via EB rule justhodl-insider-trades-30min.

What's a 'cluster buy'?
=======================
3+ different insiders at the same company purchasing within 14 days.
Strongest single-source insider signal — academic literature (Cohen,
Malloy, Pomorski 2012) shows clusters outperform broad insider buy
signals by ~3% annualized.

Output schema (data/insider-trades.json)
========================================
{
  "generated_at": ISO8601,
  "window_days": 30,
  "stats": {
    "total_buys": int,
    "total_value_usd": float,
    "unique_companies": int,
    "cluster_count": int,
  },
  "clusters": [
    {
      "ticker": str, "company": str, "cik": str,
      "insider_count": int,        # distinct insiders
      "transactions": int,         # total Form 4 lines
      "total_shares": int,
      "total_value": float,        # USD
      "avg_price": float,
      "first_filing": ISO8601, "last_filing": ISO8601,
      "insiders": [{"name", "role", "shares", "value"}],
    }
  ],
  "big_buys": [   # single transactions > $1M, all-time
    {ticker, company, insider, role, shares, price, value, filed_at}
  ],
  "sector_heat": [   # rough sector roll-up by SIC code prefix
    {sector, buy_count, total_value}
  ],
}
"""
from __future__ import annotations
import json
import os
import re
import time
import boto3
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from html.parser import HTMLParser

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY    = os.environ.get("S3_KEY", "data/insider-trades.json")
USER_AGENT = os.environ.get("SEC_USER_AGENT", "JustHodl Research raafouis@gmail.com")

# Filtering thresholds
MIN_BUY_VALUE_USD   = float(os.environ.get("MIN_BUY_VALUE_USD", "25000"))     # $25k minimum to filter noise
WINDOW_DAYS         = int(os.environ.get("WINDOW_DAYS", "30"))               # Rolling window kept in S3
CLUSTER_WINDOW_DAYS = int(os.environ.get("CLUSTER_WINDOW_DAYS", "14"))       # Cluster definition
CLUSTER_MIN_INSIDERS = int(os.environ.get("CLUSTER_MIN_INSIDERS", "3"))      # Distinct insiders for cluster
BIG_BUY_USD         = float(os.environ.get("BIG_BUY_USD", "1000000"))        # Single-tx threshold
MAX_FETCH_PARALLEL  = int(os.environ.get("MAX_PARALLEL", "8"))
ATOM_FEED_COUNT     = int(os.environ.get("ATOM_COUNT", "200"))               # SEC caps at ~200

# SIC code prefix → sector buckets (rough but useful)
SIC_SECTORS = {
    ("0", "1"): "Energy & Materials",
    ("2",): "Manufacturing",
    ("3",): "Industrials & Tech Hardware",
    ("4",): "Transport & Utilities",
    ("5",): "Retail & Wholesale",
    ("6",): "Financials & Real Estate",
    ("7",): "Services & Software",
    ("8",): "Healthcare & Education",
    ("9",): "Public Admin",
}

# Form 4 transaction codes
TXN_CODES = {
    "P": "Open-market purchase",
    "S": "Open-market sale",
    "A": "Grant/award",
    "D": "Disposition (gift/other)",
    "M": "Option exercise",
    "F": "Tax withholding",
    "G": "Gift",
    "X": "Option exercise (in-money)",
    "C": "Conversion of derivative",
    "I": "Discretionary transaction",
    "J": "Other (footnoted)",
}


# ─── HTTP helpers ─────────────────────────────────────────────────────────
def _fetch(url: str, accept: str = "application/atom+xml,application/xml,text/html") -> bytes:
    """SEC requires User-Agent. Use it on every request."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": accept,
        "Accept-Encoding": "gzip, deflate",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            import gzip
            data = gzip.decompress(data)
        return data


# ─── Atom feed parsing ─────────────────────────────────────────────────────
def fetch_recent_form4_filings():
    """Returns list of {cik, accession, company, filed_at, index_url}."""
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type=4&output=atom&count={ATOM_FEED_COUNT}"
    )
    body = _fetch(url, accept="application/atom+xml")
    root = ET.fromstring(body)
    ns = {"a": "http://www.w3.org/2005/Atom"}

    out = []
    for entry in root.findall("a:entry", ns):
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        link_el = entry.find("a:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        updated = entry.findtext("a:updated", default="", namespaces=ns)
        id_text = (entry.findtext("a:id", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("a:summary", default="", namespaces=ns) or "").strip()

        # Accession number lives in the <id> field as
        #   urn:tag:sec.gov,2008:accession-number=0001127602-26-012345
        # Falls back to <summary> body which contains "AccNo: NNNNNNNNNN-NN-NNNNNN"
        # and to <link> href as a final attempt.
        accession = None
        for source_text in (id_text, summary, link):
            m = re.search(r"\b(\d{10}-\d{2}-\d{6})\b", source_text)
            if m:
                accession = m.group(1)
                break

        # Filer CIK extraction. The atom <link> for /cgi-bin/browse-edgar entries
        # has CIK= as a query param. The title also contains CIK in parentheses
        # like 'WARREN BUFFETT (0001067983)'. Try both.
        cik_match = (re.search(r"CIK=(\d+)", link)
                     or re.search(r"/data/(\d+)/", link)
                     or re.search(r"\((\d{10})\)", title))
        cik = cik_match.group(1) if cik_match else None

        if not accession or not cik:
            continue

        out.append({
            "company": title.replace("4 - ", "").split(" (")[0].strip()[:120],
            "cik": cik,
            "accession": accession,
            "filed_at": updated,
            "index_url": link,
        })
    return out


# ─── Form 4 XML parsing ───────────────────────────────────────────────────
def _build_filing_paths(filer_cik: str, accession: str):
    """Return possible XML doc URLs for a filing.

    SEC archives every filing under the FILER (reporting person) CIK,
    not the issuer's. The filer CIK is the prefix of the accession number:
    accession '0001127602-26-012345' → filer CIK '0001127602' → int form '1127602'.

    The CIK in the atom feed's <link> is typically the issuer's CIK and
    points at the wrong directory — we ignore it and use accession's prefix.
    """
    cik_int = str(int(filer_cik))
    acc_clean = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}"
    return base + "/", base + "/index.json"


# Files in a filing's directory that are NEVER the primary Form 4 XML.
# Used to filter when we have multiple .xml files in the listing.
_EXCLUDE_XML_NAMES = {
    "filing-summary.xml",
    "metalinks.json",
    "financial_report.xml",
    "report.xml",
}
# Files matching these substrings are de-prioritized (only chosen as last resort).
_DEPRIORITIZE_XML_SUBSTR = ("filing-summary", "metadata", "metalinks", "financial-report", "financial_report")


class _AnchorFinder(HTMLParser):
    """Find the .xml link that's the primary Form 4 document."""
    def __init__(self):
        super().__init__()
        self.candidates = []
    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href", "")
        if href.lower().endswith(".xml"):
            self.candidates.append(href)


def _pick_form4_xml(filenames):
    """From a list of filenames in the filing directory, pick the most-likely Form 4 ownership XML."""
    xmls = [f for f in filenames if f.lower().endswith(".xml") and f.lower() not in _EXCLUDE_XML_NAMES]
    if not xmls:
        return None
    # Strong preference for filenames containing 'form4', 'wf-form', 'wk-form', or 'primary_doc'
    for kw in ("form4", "wf-form", "wk-form", "primary_doc"):
        for f in xmls:
            if kw in f.lower():
                return f
    # Otherwise filter out de-prioritized
    good = [f for f in xmls if not any(sub in f.lower() for sub in _DEPRIORITIZE_XML_SUBSTR)]
    if good:
        return good[0]
    return xmls[0]


def fetch_form4_xml(filer_cik: str, accession: str, errlog=None):
    """Fetch the primary Form 4 XML document for a given filing.

    errlog is an optional dict for surfacing per-error-type counts
    (used by diagnostic instrumentation).
    """
    base, index_json_url = _build_filing_paths(filer_cik, accession)

    # Prefer the JSON directory listing — most reliable
    try:
        idx = json.loads(_fetch(index_json_url, accept="application/json"))
        names = [item.get("name", "") for item in idx.get("directory", {}).get("item", [])]
        chosen = _pick_form4_xml(names)
        if chosen:
            return _fetch(base + chosen, accept="application/xml")
        if errlog is not None:
            errlog["no_xml_in_listing"] = errlog.get("no_xml_in_listing", 0) + 1
    except urllib.error.HTTPError as e:
        if errlog is not None:
            errlog[f"http_{e.code}_index"] = errlog.get(f"http_{e.code}_index", 0) + 1
    except urllib.error.URLError as e:
        if errlog is not None:
            errlog["url_err_index"] = errlog.get("url_err_index", 0) + 1
    except Exception as e:
        if errlog is not None:
            etype = type(e).__name__
            errlog[f"err_index_{etype}"] = errlog.get(f"err_index_{etype}", 0) + 1

    # Fallback: scrape the HTML index page for .xml links
    try:
        html = _fetch(base + "/", accept="text/html").decode("utf-8", errors="ignore")
        f = _AnchorFinder()
        f.feed(html)
        cleaned = [c.split("/")[-1] for c in f.candidates]
        chosen = _pick_form4_xml(cleaned)
        if chosen:
            for cand in f.candidates:
                if cand.endswith(chosen):
                    if cand.startswith("/"):
                        return _fetch("https://www.sec.gov" + cand, accept="application/xml")
                    if cand.startswith("http"):
                        return _fetch(cand, accept="application/xml")
                    return _fetch(base + chosen, accept="application/xml")
    except urllib.error.HTTPError as e:
        if errlog is not None:
            errlog[f"http_{e.code}_html"] = errlog.get(f"http_{e.code}_html", 0) + 1
    except Exception as e:
        if errlog is not None:
            etype = type(e).__name__
            errlog[f"err_html_{etype}"] = errlog.get(f"err_html_{etype}", 0) + 1
    return None


def parse_form4(xml_bytes: bytes):
    """Extract structured transactions from a Form 4 XML.

    Returns list of dicts:
      {ticker, company, cik, insider_name, insider_role,
       code, code_meaning, shares, price, value, txn_date, accession}
    """
    if not xml_bytes:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    # The Form 4 schema doesn't use namespaces (or uses an empty default ns)
    def t(elem, path):
        e = elem.find(path)
        if e is None:
            return None
        # Handle the SEC's <value>123</value> pattern wrapped in <foo><value>...</value></foo>
        v = e.find("value")
        if v is not None and v.text:
            return v.text.strip()
        return e.text.strip() if e.text else None

    issuer = root.find("issuer")
    if issuer is None:
        return []

    ticker = t(issuer, "issuerTradingSymbol")
    company = t(issuer, "issuerName")
    cik = t(issuer, "issuerCik")
    if not ticker or ticker.upper() in ("NONE", "NA"):
        return []

    # Reporting owner
    owner = root.find("reportingOwner")
    insider_name = None
    insider_role = []
    if owner is not None:
        insider_name = t(owner, "reportingOwnerId/rptOwnerName")
        rel = owner.find("reportingOwnerRelationship")
        if rel is not None:
            for tag, label in [
                ("isDirector", "Director"),
                ("isOfficer", "Officer"),
                ("isTenPercentOwner", "10% Owner"),
                ("isOther", "Other"),
            ]:
                e = rel.find(tag)
                if e is not None:
                    val = (e.text or "").strip()
                    if val in ("1", "true", "True"):
                        insider_role.append(label)
            # If officer, get title
            title = t(rel, "officerTitle")
            if title and "Officer" in insider_role:
                insider_role = [r if r != "Officer" else f"{title}" for r in insider_role]

    role_str = ", ".join(insider_role) if insider_role else "Insider"

    # Non-derivative transactions (= the actual stock buys/sells we care about)
    txns = []
    nd_table = root.find("nonDerivativeTable")
    if nd_table is None:
        return []

    for tx in nd_table.findall("nonDerivativeTransaction"):
        amounts = tx.find("transactionAmounts")
        coding = tx.find("transactionCoding")
        if amounts is None or coding is None:
            continue

        try:
            shares = float(t(amounts, "transactionShares") or 0)
            price = float(t(amounts, "transactionPricePerShare") or 0)
        except (TypeError, ValueError):
            continue

        code = t(coding, "transactionCode") or ""
        ad = t(amounts, "transactionAcquiredDisposedCode") or ""

        # 'A' = acquired, 'D' = disposed. Combined with the code, lets us
        # determine if it's a real buy. Genuine open-market buy = code 'P' + A.
        is_buy = code == "P" and ad == "A"
        is_sell = code == "S" and ad == "D"

        value = abs(shares * price)
        if not (is_buy or is_sell):
            continue
        if is_buy and value < MIN_BUY_VALUE_USD:
            continue

        txn_date_el = tx.find("transactionDate")
        txn_date = t(txn_date_el, "value") if txn_date_el is not None else None

        txns.append({
            "ticker": ticker.upper().strip(),
            "company": (company or "")[:80],
            "cik": cik,
            "insider": (insider_name or "Unknown")[:80],
            "role": role_str[:60],
            "code": code,
            "code_meaning": TXN_CODES.get(code, "Other"),
            "side": "buy" if is_buy else "sell",
            "shares": int(shares),
            "price": round(price, 2),
            "value": round(value, 2),
            "txn_date": txn_date,
        })

    return txns


# ─── S3 rolling-window helpers ────────────────────────────────────────────
def load_existing(s3) -> list:
    """Return list of prior transactions from S3 (or empty)."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        body = json.loads(obj["Body"].read())
        return body.get("transactions", [])
    except s3.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"load_existing error: {e}")
        return []


def merge_and_window(prior: list, fresh: list) -> list:
    """Dedupe by (accession, ticker, insider, side, shares) and drop > WINDOW_DAYS."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)

    seen = set()
    out = []
    # Process fresh first so newest entries win on dupe
    for record in fresh + prior:
        key = (record.get("accession"), record.get("ticker"), record.get("insider"),
               record.get("side"), record.get("shares"))
        if key in seen:
            continue
        seen.add(key)

        # Drop too-old
        try:
            filed = record.get("filed_at") or record.get("txn_date") or ""
            filed_dt = datetime.fromisoformat(filed.replace("Z", "+00:00"))
        except Exception:
            filed_dt = datetime.now(timezone.utc)
        if filed_dt < cutoff:
            continue
        out.append(record)
    return out


# ─── Aggregations ─────────────────────────────────────────────────────────
def detect_clusters(transactions: list) -> list:
    """Group by ticker; flag if >=N distinct insiders bought in CLUSTER_WINDOW_DAYS."""
    by_ticker = defaultdict(list)
    for t in transactions:
        if t.get("side") != "buy":
            continue
        by_ticker[t["ticker"]].append(t)

    clusters = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=CLUSTER_WINDOW_DAYS)

    for ticker, txns in by_ticker.items():
        # Filter to last 14 days
        recent = []
        for x in txns:
            try:
                dt = datetime.fromisoformat((x.get("filed_at") or x.get("txn_date") or "").replace("Z", "+00:00"))
            except Exception:
                continue
            if dt >= cutoff:
                recent.append(x)
        if not recent:
            continue

        # Count distinct insiders
        insiders = {x["insider"]: x for x in recent}
        if len(insiders) < CLUSTER_MIN_INSIDERS:
            continue

        total_value = sum(x["value"] for x in recent)
        total_shares = sum(x["shares"] for x in recent)
        avg_price = total_value / total_shares if total_shares else 0

        # Per-insider rollup (sum across multiple buys by same person)
        per_insider = defaultdict(lambda: {"shares": 0, "value": 0, "role": ""})
        for x in recent:
            p = per_insider[x["insider"]]
            p["shares"] += x["shares"]
            p["value"] += x["value"]
            p["role"] = x.get("role", "")

        clusters.append({
            "ticker": ticker,
            "company": recent[0]["company"],
            "cik": recent[0]["cik"],
            "insider_count": len(insiders),
            "transactions": len(recent),
            "total_shares": int(total_shares),
            "total_value": round(total_value, 2),
            "avg_price": round(avg_price, 2),
            "first_filing": min((x.get("filed_at") or "") for x in recent),
            "last_filing": max((x.get("filed_at") or "") for x in recent),
            "insiders": [
                {"name": k, "role": v["role"], "shares": int(v["shares"]), "value": round(v["value"], 2)}
                for k, v in sorted(per_insider.items(), key=lambda kv: -kv[1]["value"])
            ],
        })

    clusters.sort(key=lambda c: -c["total_value"])
    return clusters


def detect_big_buys(transactions: list) -> list:
    out = [t for t in transactions if t.get("side") == "buy" and t["value"] >= BIG_BUY_USD]
    out.sort(key=lambda t: -t["value"])
    return out[:50]


def stats_summary(transactions: list) -> dict:
    buys = [t for t in transactions if t["side"] == "buy"]
    return {
        "total_buys": len(buys),
        "total_value_usd": round(sum(t["value"] for t in buys), 2),
        "unique_companies": len({t["ticker"] for t in buys}),
        "unique_insiders": len({t["insider"] for t in buys}),
    }


# ─── Lambda entry point ───────────────────────────────────────────────────
def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    # 1. List recent Form 4 filings
    try:
        filings = fetch_recent_form4_filings()
    except Exception as e:
        print(f"FATAL atom feed fetch failed: {e}")
        return {"statusCode": 502, "body": json.dumps({"error": str(e)})}
    print(f"Atom feed returned {len(filings)} Form 4 filings")

    # 2. Fetch + parse each filing's XML in parallel (small batch to stay under SEC rate)
    fresh_txns = []
    fetch_errors = 0
    diag = {
        "filings_seen": 0,
        "xml_fetched": 0,
        "xml_parse_failed": 0,
        "no_issuer_or_ticker": 0,
        "no_nonderivative_table": 0,
        "txn_seen": 0,
        "skipped_not_buy_or_sell": 0,
        "skipped_below_threshold": 0,
        "buys_kept": 0,
        "sells_kept": 0,
    }
    errlog = {}  # per-error-type counter — see fetch_form4_xml

    def process(filing):
        diag["filings_seen"] += 1
        try:
            # Filer (reporting person) CIK is the prefix of the accession number,
            # NOT what's in filing["cik"] (which can be the issuer CIK from atom).
            # SEC archives every filing under the filer's CIK directory.
            filer_cik = filing["accession"].split("-")[0]
            xml = fetch_form4_xml(filer_cik, filing["accession"], errlog=errlog)
            if not xml:
                return None
            diag["xml_fetched"] += 1

            # Inline parse with diagnostics rather than calling parse_form4
            try:
                root = ET.fromstring(xml)
            except ET.ParseError:
                diag["xml_parse_failed"] += 1
                return []

            def t(elem, path):
                e = elem.find(path)
                if e is None:
                    return None
                v = e.find("value")
                if v is not None and v.text:
                    return v.text.strip()
                return e.text.strip() if e.text else None

            issuer = root.find("issuer")
            if issuer is None:
                diag["no_issuer_or_ticker"] += 1
                return []

            ticker = t(issuer, "issuerTradingSymbol")
            company = t(issuer, "issuerName")
            cik = t(issuer, "issuerCik")
            if not ticker or ticker.upper() in ("NONE", "NA"):
                diag["no_issuer_or_ticker"] += 1
                return []

            owner = root.find("reportingOwner")
            insider_name = None
            insider_role = []
            if owner is not None:
                insider_name = t(owner, "reportingOwnerId/rptOwnerName")
                rel = owner.find("reportingOwnerRelationship")
                if rel is not None:
                    for tag, label in [
                        ("isDirector", "Director"),
                        ("isOfficer", "Officer"),
                        ("isTenPercentOwner", "10% Owner"),
                        ("isOther", "Other"),
                    ]:
                        e = rel.find(tag)
                        if e is not None:
                            val = (e.text or "").strip()
                            if val in ("1", "true", "True"):
                                insider_role.append(label)
                    title = t(rel, "officerTitle")
                    if title and "Officer" in insider_role:
                        insider_role = [r if r != "Officer" else f"{title}" for r in insider_role]
            role_str = ", ".join(insider_role) if insider_role else "Insider"

            nd_table = root.find("nonDerivativeTable")
            if nd_table is None:
                diag["no_nonderivative_table"] += 1
                return []

            txns = []
            for tx in nd_table.findall("nonDerivativeTransaction"):
                diag["txn_seen"] += 1
                amounts = tx.find("transactionAmounts")
                coding = tx.find("transactionCoding")
                if amounts is None or coding is None:
                    continue

                try:
                    shares = float(t(amounts, "transactionShares") or 0)
                    price = float(t(amounts, "transactionPricePerShare") or 0)
                except (TypeError, ValueError):
                    continue

                code = t(coding, "transactionCode") or ""
                ad = t(amounts, "transactionAcquiredDisposedCode") or ""
                is_buy = code == "P" and ad == "A"
                is_sell = code == "S" and ad == "D"
                value = abs(shares * price)

                if not (is_buy or is_sell):
                    diag["skipped_not_buy_or_sell"] += 1
                    continue
                if is_buy and value < MIN_BUY_VALUE_USD:
                    diag["skipped_below_threshold"] += 1
                    continue

                if is_buy:
                    diag["buys_kept"] += 1
                else:
                    diag["sells_kept"] += 1

                txn_date_el = tx.find("transactionDate")
                txn_date = t(txn_date_el, "value") if txn_date_el is not None else None

                txns.append({
                    "ticker": ticker.upper().strip(),
                    "company": (company or "")[:80],
                    "cik": cik,
                    "insider": (insider_name or "Unknown")[:80],
                    "role": role_str[:60],
                    "code": code,
                    "code_meaning": TXN_CODES.get(code, "Other"),
                    "side": "buy" if is_buy else "sell",
                    "shares": int(shares),
                    "price": round(price, 2),
                    "value": round(value, 2),
                    "txn_date": txn_date,
                    "accession": filing["accession"],
                    "filed_at": filing["filed_at"],
                })

            return txns
        except Exception as e:
            print(f"parse error {filing['accession']}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=MAX_FETCH_PARALLEL) as pool:
        futures = [pool.submit(process, f) for f in filings]
        for fut in as_completed(futures):
            r = fut.result()
            if r is None:
                fetch_errors += 1
            else:
                fresh_txns.extend(r)
            time.sleep(0.05)

    print(f"DIAG: {json.dumps(diag)}")
    print(f"Parsed {len(fresh_txns)} kept transactions; fetch_errors={fetch_errors}")

    # 3. Merge with rolling 30-day window from S3
    prior = load_existing(s3)
    all_txns = merge_and_window(prior, fresh_txns)
    print(f"After merge+window: {len(all_txns)} transactions in window")

    # 4. Aggregate
    clusters = detect_clusters(all_txns)
    big_buys = detect_big_buys(all_txns)
    summary = stats_summary(all_txns)

    # 5. Compose output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "cluster_window_days": CLUSTER_WINDOW_DAYS,
        "thresholds": {
            "min_buy_value_usd": MIN_BUY_VALUE_USD,
            "cluster_min_insiders": CLUSTER_MIN_INSIDERS,
            "big_buy_usd": BIG_BUY_USD,
        },
        "stats": {
            **summary,
            "cluster_count": len(clusters),
            "big_buy_count": len(big_buys),
            "fetch_errors": fetch_errors,
            "fetch_duration_s": round(time.time() - started, 1),
            "diagnostics": diag,
            "fetch_errlog": errlog,
            "atom_feed_count": len(filings),
        },
        "clusters": clusters[:30],
        "big_buys": big_buys[:30],
        "transactions": [t for t in all_txns if t["side"] == "buy"][:500],   # cap for size
    }

    body = json.dumps(output).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )

    print(f"Wrote {len(body)} bytes to s3://{S3_BUCKET}/{S3_KEY}")
    print(f"Stats: {json.dumps(output['stats'])}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "stats": output["stats"],
            "preview": {
                "top_cluster": clusters[0] if clusters else None,
                "biggest_buy": big_buys[0] if big_buys else None,
            },
        }),
    }
