"""justhodl-executive-intel — Trump + executive-branch trade tracker (OGE 278-T)

Executive-branch officials (incl. the President & VP) must file Periodic
Transaction Reports (OGE Form 278-T) for securities transactions >$1,000,
within 30-45 days — the executive-branch analog of the STOCK Act.

These are PDFs published on whitehouse.gov/disclosures (Transaction Reports
section), in wp-content/uploads/YYYY/MM/. NO JSON API exists, so we:
  1. Crawl the disclosures index for PTR PDF links
  2. Download each PDF, extract text (vendored pypdf)
  3. Parse the transaction table → {filer, position, asset, ticker, side, date, amount}
  4. Map company names → tickers (curated + heuristic)
  5. Aggregate per-ticker conviction (same framework as congress, with an
     EXECUTIVE_PROXIMITY bonus — proximity to policy levers = informational edge)

Graceful: text-based PTRs parse cleanly; scanned/garbled ones are skipped.
Output: data/executive-intel.json  (merged into the political signal layer).
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import boto3

try:
    from pypdf import PdfReader
    import io as _io
    PYPDF_OK = True
except Exception as e:
    print(f"[exec-intel] pypdf import failed: {e}")
    PYPDF_OK = False

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/executive-intel.json"
DISCLOSURES_URL = "https://www.whitehouse.gov/disclosures/"
LOOKBACK_DAYS = 180
UA = "Mozilla/5.0 (compatible; JustHodlExecIntel/1.0)"

s3 = boto3.client("s3", region_name="us-east-1")

# Curated company-name → ticker map for common executive-disclosed names
NAME_TICKER = {
    "apple": "AAPL", "microsoft": "MSFT", "nvidia": "NVDA", "tesla": "TSLA",
    "amazon": "AMZN", "alphabet": "GOOGL", "google": "GOOGL", "meta": "META",
    "jpmorgan": "JPM", "goldman": "GS", "bank of america": "BAC", "wells fargo": "WFC",
    "exxon": "XOM", "chevron": "CVX", "lockheed": "LMT", "raytheon": "RTX",
    "rtx": "RTX", "northrop": "NOC", "general dynamics": "GD", "boeing": "BA",
    "palantir": "PLTR", "broadcom": "AVGO", "berkshire": "BRK.B", "visa": "V",
    "mastercard": "MA", "home depot": "HD", "caterpillar": "CAT", "deere": "DE",
    "coca-cola": "KO", "pepsi": "PEP", "pfizer": "PFE", "merck": "MRK",
    "johnson & johnson": "JNJ", "unitedhealth": "UNH", "eli lilly": "LLY",
    "trump media": "DJT", "tesla motors": "TSLA", "intel": "INTC",
    "amd": "AMD", "advanced micro": "AMD", "qualcomm": "QCOM", "oracle": "ORCL",
    "salesforce": "CRM", "netflix": "NFLX", "walmart": "WMT", "costco": "COST",
}


def _http_get_bytes(url, timeout=40):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as e:
        print(f"[exec-intel] fetch err {str(e)[:80]} {url[:90]}")
        return None


def _http_get_text(url, timeout=30):
    b = _http_get_bytes(url, timeout)
    return b.decode("utf-8", "replace") if b else None


def _read_s3_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def discover_ptr_pdfs():
    """Crawl the WH disclosures pages for Periodic Transaction Report PDF links."""
    urls = set()
    pages = [DISCLOSURES_URL,
             "https://www.whitehouse.gov/disclosures/transaction-reports/",
             "https://www.whitehouse.gov/disclosures/financial-disclosure-reports/"]
    for page in pages:
        html = _http_get_text(page)
        if not html:
            continue
        # Find PDF links that look like periodic transaction reports
        for m in re.finditer(r'href="([^"]+\.pdf)"', html, re.IGNORECASE):
            u = m.group(1)
            if not u.startswith("http"):
                u = "https://www.whitehouse.gov" + (u if u.startswith("/") else "/" + u)
            if "transaction" in u.lower() or "278" in u or "ptr" in u.lower():
                urls.add(u)
    return list(urls)


def parse_ptr_pdf(pdf_bytes, url):
    """Extract filer + transactions from a 278-T PDF text layer."""
    if not PYPDF_OK or not pdf_bytes:
        return None
    try:
        reader = PdfReader(_io.BytesIO(pdf_bytes))
        text = "\n".join((p.extract_text() or "") for p in reader.pages)
    except Exception as e:
        print(f"[exec-intel] pdf parse err {str(e)[:60]}")
        return None
    if not text or len(text) < 50:
        return None

    # Filer name + position
    filer = ""
    position = ""
    m = re.search(r"Filer'?s? Information\s*\n+\s*([A-Z][A-Za-z.\-' ]+,\s*[A-Za-z.\-' ]+)", text)
    if m:
        filer = m.group(1).strip()
    else:
        # Fallback: derive from filename
        fn = url.split("/")[-1].replace(".pdf", "")
        filer = re.sub(r"[-_]?Periodic.*$", "", fn).replace("-", " ").strip()
    pm = re.search(r"(President of the United States|Vice President|Secretary[^\n]{0,40}|Special Assistant[^\n]{0,40}|Counsel[^\n]{0,40}|Deputy[^\n]{0,40}|Director[^\n]{0,40}|Assistant to the President[^\n]{0,40})", text)
    if pm:
        position = pm.group(1).strip()[:60]

    # Transaction lines: "<Asset> <Purchase|Sale|Exchange> MM/DD/YYYY [No] $low - $high"
    transactions = []
    tx_re = re.compile(
        r"([A-Za-z0-9.,&'\-/() ]{2,60}?)\s+(Purchase|Sale|Sale \(partial\)|Sale \(full\)|Exchange)\s+(\d{2}/\d{2}/\d{4})\s+(?:No|Yes)?\s*\$([\d,]+)\s*-\s*\$([\d,]+)",
        re.IGNORECASE)
    for tm in tx_re.finditer(text):
        asset = tm.group(1).strip()
        side_raw = tm.group(2).lower()
        side = "buy" if "purchase" in side_raw else "sell"
        date = tm.group(3)
        low = tm.group(4)
        high = tm.group(5)
        # Skip excepted assets
        al = asset.lower()
        if any(k in al for k in ["bond", "treasury", "money market", "cd ", "mutual fund", "etf"]):
            continue
        ticker = extract_ticker(asset)
        transactions.append({
            "asset": asset[:60], "ticker": ticker, "side": side,
            "date": date, "amount": f"${low} - ${high}",
        })

    return {"filer": filer, "position": position, "url": url, "transactions": transactions}


def extract_ticker(asset):
    """Best-effort ticker from an asset description."""
    # Explicit ticker in parens or brackets, e.g. "Apple Inc (AAPL)"
    m = re.search(r"\(([A-Z]{1,5})\)", asset)
    if m:
        return m.group(1)
    al = asset.lower()
    for name, tk in NAME_TICKER.items():
        if name in al:
            return tk
    # Standalone uppercase token that looks like a ticker
    m2 = re.search(r"\b([A-Z]{2,5})\b", asset)
    if m2 and m2.group(1) not in ("LLC", "INC", "CORP", "ETF", "USA", "THE", "AND"):
        return m2.group(1)
    return ""


def lambda_handler(event, context):
    t0 = time.time()
    print("[exec-intel] starting")

    pdf_urls = discover_ptr_pdfs()
    print(f"[exec-intel] discovered {len(pdf_urls)} candidate PTR PDFs")

    filers = []
    all_tx = []
    parsed = 0
    for url in pdf_urls[:60]:
        b = _http_get_bytes(url)
        if not b:
            continue
        rec = parse_ptr_pdf(b, url)
        if rec and rec["transactions"]:
            filers.append(rec)
            parsed += 1
            for tx in rec["transactions"]:
                tx["filer"] = rec["filer"]
                tx["position"] = rec["position"]
                all_tx.append(tx)
        time.sleep(0.15)
    print(f"[exec-intel] parsed {parsed} PTRs with transactions, {len(all_tx)} total tx")

    # Aggregate per ticker (buys carry conviction; executive proximity = edge)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    by_ticker = defaultdict(lambda: {"ticker": "", "asset": "", "n_buys": 0, "n_sells": 0,
                                      "buyers": set(), "conviction": 0.0, "txs": [], "latest": None})
    for tx in all_tx:
        if not tx["ticker"]:
            continue
        try:
            d = datetime.strptime(tx["date"], "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except Exception:
            d = now
        if d < cutoff:
            continue
        rec = by_ticker[tx["ticker"]]
        rec["ticker"] = tx["ticker"]
        if not rec["asset"]:
            rec["asset"] = tx["asset"]
        rec["txs"].append(tx)
        if rec["latest"] is None or d > rec["latest"]:
            rec["latest"] = d
        # President / VP / senior WH proximity bonus
        pos = (tx.get("position") or "").lower()
        prox = 3.0 if ("president" in pos) else (2.0 if any(k in pos for k in ["secretary", "counsel", "director", "assistant to the president", "deputy"]) else 1.5)
        if tx["side"] == "buy":
            rec["n_buys"] += 1
            rec["buyers"].add(tx["filer"])
            age = (now - d).days
            recency = max(0.5, 1.0 - age / (LOOKBACK_DAYS * 2.0))
            rec["conviction"] += 12.0 * prox * recency
        else:
            rec["n_sells"] += 1

    results = []
    for tk, rec in by_ticker.items():
        n_buyers = len(rec["buyers"])
        cluster_mult = 1.0 + 0.5 * max(0, n_buyers - 1)
        conv = round(rec["conviction"] * cluster_mult, 1)
        if rec["n_sells"] > rec["n_buys"]:
            conv *= 0.4
        results.append({
            "ticker": tk, "asset": rec["asset"][:60],
            "conviction_score": round(conv, 1),
            "n_buyers": n_buyers, "n_buys": rec["n_buys"], "n_sells": rec["n_sells"],
            "buyers": list(rec["buyers"])[:6],
            "latest_tx_date": rec["latest"].strftime("%Y-%m-%d") if rec["latest"] else None,
            "transactions": rec["txs"][:8],
        })
    results.sort(key=lambda r: r["conviction_score"], reverse=True)
    top = [r for r in results if r["conviction_score"] > 0 and r["n_buys"] > r["n_sells"]][:30]

    # Keep last good snapshot if nothing parsed (WH layout may change)
    if not all_tx:
        prev = _read_s3_json(OUTPUT_KEY)
        if prev:
            prev["stale"] = True
            prev["generated_at"] = now.isoformat()
            s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=json.dumps(prev, default=str).encode(),
                          ContentType="application/json", CacheControl="public, max-age=600")
        print("[exec-intel] no parseable tx — kept snapshot")

    output = {
        "schema_version": "1.0",
        "engine": "executive-intel (OGE 278-T from whitehouse.gov)",
        "generated_at": now.isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "source": "whitehouse.gov/disclosures Transaction Reports (OGE Form 278-T PDFs)",
        "methodology": (
            "Crawl WH disclosures → download 278-T PDFs → pypdf text extract → "
            "parse transactions table → ticker-map → conviction = 12 × proximity "
            "(President 3x / senior 2x / staff 1.5x) × recency × cluster. Buys only. "
            "Excepted assets (bonds/treasuries/funds) filtered. Text PTRs parse; "
            "scanned ones skipped."
        ),
        "stats": {
            "candidate_pdfs": len(pdf_urls),
            "parsed_ptrs": parsed,
            "total_transactions": len(all_tx),
            "unique_tickers": len(results),
        },
        "filers": [{"filer": f["filer"], "position": f["position"],
                    "n_tx": len(f["transactions"]), "url": f["url"]} for f in filers],
        "top_conviction_buys": top,
        "by_ticker": {r["ticker"]: r for r in results[:100]},
        "all_transactions": all_tx[:200],
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[exec-intel] DONE {round(time.time()-t0,1)}s — {parsed} PTRs, {len(top)} conviction buys")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "parsed_ptrs": parsed,
                                                     "transactions": len(all_tx),
                                                     "top_conviction": len(top)})}
