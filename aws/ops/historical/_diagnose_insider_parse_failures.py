"""
Diagnose why 477/500 Form 4 filings fail to extract a buy transaction.
Pull a sample of 30 filings, parse with verbose counters at each filter,
report what's failing.
"""
import json, os, time, urllib.request, urllib.error
import xml.etree.ElementTree as ET
import boto3
from botocore.config import Config
from collections import Counter

REGION = "us-east-1"
LONG_CFG = Config(read_timeout=300, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=LONG_CFG)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

UA = "JustHodl Research raafouis@gmail.com"

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", "replace")
    except Exception as e:
        return None


def get_recent_form4_links(n=30):
    """Pull SEC daily index for today and yesterday, return Form 4 links."""
    from datetime import datetime, timedelta, timezone
    out = []
    today = datetime.now(timezone.utc).date()
    for offset in range(0, 5):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5: continue
        # SEC daily index path
        idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{d.year}/QTR{(d.month-1)//3+1}/form.{d.strftime('%Y%m%d')}.idx"
        idx = fetch(idx_url)
        if not idx: continue
        for line in idx.splitlines():
            if line.startswith("4 "):
                # Parse fixed-width record
                parts = line.split()
                if len(parts) >= 5:
                    accession_path = parts[-1]  # e.g. edgar/data/123/0001234567-25-001234.txt
                    # The XML is at /Archives/{accession_path with -index.htm}
                    out.append((d.isoformat(), accession_path))
                    if len(out) >= n: return out
        time.sleep(0.2)
    return out


def find_xml_url(filing_index_url):
    """Given the .txt filing-index URL, find the form4.xml within it."""
    txt = fetch(filing_index_url)
    if not txt: return None
    # In the .txt SGML, look for <FILENAME>foo.xml lines or .xml URLs
    import re
    for m in re.finditer(r'<FILENAME>(.+?\.xml)', txt, re.IGNORECASE):
        fname = m.group(1).strip()
        # Build URL from the filing-index URL base
        base = filing_index_url.rsplit("/", 1)[0]
        # Strip "0001..." accession prefix
        accession = filing_index_url.rsplit("/", 1)[-1].replace(".txt", "").replace("-", "")
        # No need - the base path already has the dir
        return f"{base}/{fname}"
    return None


def diagnose_one(xml_text):
    """Return dict explaining why parse failed/succeeded."""
    diag = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return {"fail": "xml_parse_error", "msg": str(e)[:80]}

    # Issuer ticker
    issuer = root.find("issuer")
    if issuer is None:
        return {"fail": "no_issuer_block"}
    ticker_el = issuer.find("issuerTradingSymbol")
    if ticker_el is None or not (ticker_el.text or "").strip():
        return {"fail": "no_ticker", "issuer_name": (issuer.findtext("issuerName") or "")[:40]}
    ticker = ticker_el.text.strip().upper()
    if ticker == "N/A" or len(ticker) > 6:
        return {"fail": "ticker_invalid", "ticker": ticker}
    diag["ticker"] = ticker

    # Owner
    owner = root.find("reportingOwner")
    if owner is None:
        return {"fail": "no_owner_block", "ticker": ticker}

    # nonDerivativeTable
    nd = root.find("nonDerivativeTable")
    if nd is None:
        # Could be derivative-only filing (option exercise)
        return {"fail": "no_nonderivative_table", "ticker": ticker, "has_derivative": root.find("derivativeTable") is not None}

    txns = nd.findall("nonDerivativeTransaction")
    if not txns:
        # Holdings-only filing (no transactions, just snapshot)
        holdings = nd.findall("nonDerivativeHolding")
        return {"fail": "no_nd_transactions", "ticker": ticker, "n_holdings": len(holdings)}

    diag["n_txns"] = len(txns)
    txn_codes = []
    ad_codes = []
    has_buy = False
    for t in txns:
        coding = t.find("transactionCoding")
        if coding is None:
            txn_codes.append("NO_CODING")
            continue
        tcode = (coding.findtext("transactionCode") or "").strip()
        txn_codes.append(tcode)
        amounts = t.find("transactionAmounts")
        if amounts is None:
            continue
        ad = (amounts.findtext("transactionAcquiredDisposedCode/value") or "").strip()
        ad_codes.append(f"{tcode}/{ad}")
        if tcode == "P" and ad == "A":
            shares = float(amounts.findtext("transactionShares/value") or 0)
            price = float(amounts.findtext("transactionPricePerShare/value") or 0)
            if shares > 0 and price > 0:
                has_buy = True

    if has_buy:
        return {"ok": True, "ticker": ticker, "txn_codes": txn_codes, "ad_codes": ad_codes}
    return {"fail": "no_open_market_buy", "ticker": ticker, "txn_codes": txn_codes, "ad_codes": ad_codes}


def main():
    section("1) Fetch 30 recent Form 4 filings")
    links = get_recent_form4_links(30)
    log(f"  found {len(links)} Form 4 links to test")

    section("2) Parse each and tally failure reasons")
    fail_counter = Counter()
    txn_code_counter = Counter()
    ad_code_counter = Counter()
    samples_by_fail = {}
    n_ok = 0
    n_total = 0
    for d, accession_path in links[:30]:
        n_total += 1
        url = f"https://www.sec.gov/Archives/{accession_path}"
        xml_url = find_xml_url(url)
        if not xml_url:
            fail_counter["no_xml_in_index"] += 1
            continue
        xml_text = fetch(xml_url)
        if not xml_text:
            fail_counter["xml_fetch_failed"] += 1
            continue
        time.sleep(0.15)
        diag = diagnose_one(xml_text)
        if diag.get("ok"):
            n_ok += 1
            for c in diag.get("txn_codes", []):
                txn_code_counter[c] += 1
            for c in diag.get("ad_codes", []):
                ad_code_counter[c] += 1
        else:
            r = diag.get("fail", "unknown")
            fail_counter[r] += 1
            for c in diag.get("txn_codes", []):
                txn_code_counter[c] += 1
            for c in diag.get("ad_codes", []):
                ad_code_counter[c] += 1
            if r not in samples_by_fail and len(samples_by_fail) < 8:
                samples_by_fail[r] = diag

    log(f"  total: {n_total}  buys extracted: {n_ok}  failures: {sum(fail_counter.values())}")
    log("")
    log("  ── failure breakdown ──")
    for r, n in fail_counter.most_common():
        log(f"    {r:<30} {n}")
    log("")
    log("  ── transactionCode tally (raw) ──")
    for c, n in txn_code_counter.most_common(15):
        log(f"    {c:<10} {n}")
    log("")
    log("  ── code/AD pair tally ──")
    for c, n in ad_code_counter.most_common(15):
        log(f"    {c:<10} {n}")
    log("")
    log("  ── sample failures ──")
    for r, d in samples_by_fail.items():
        log(f"    {r}: {json.dumps(d)[:200]}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "diagnose_insider_parse.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
