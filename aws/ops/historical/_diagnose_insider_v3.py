"""
Diagnose Form 4 parse failures using the same logic as the production Lambda.
Use Lambda's own daily-index pull to find a fresh sample, then parse 50
filings with verbose per-reject-reason counters.
"""
import json, os, time, re, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from collections import Counter
import threading
import boto3

REGION = "us-east-1"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

UA = "JustHodl Research raafouis@gmail.com"
SEC_MIN_INTERVAL = 0.12
_sec_lock = threading.Lock()
_last_sec_call = 0.0

def sec_get(url, timeout=12):
    global _last_sec_call
    with _sec_lock:
        elapsed = time.time() - _last_sec_call
        if elapsed < SEC_MIN_INTERVAL:
            time.sleep(SEC_MIN_INTERVAL - elapsed)
        _last_sec_call = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def get_daily_form4_filings(date_obj):
    yyyy = date_obj.year
    qtr = (date_obj.month - 1) // 3 + 1
    yyyymmdd = date_obj.strftime("%Y%m%d")
    url = f"https://www.sec.gov/Archives/edgar/daily-index/{yyyy}/QTR{qtr}/form.{yyyymmdd}.idx"
    try:
        text = sec_get(url, timeout=20)
    except urllib.error.HTTPError as e:
        return []
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
        cik = line[74:86].strip()
        file_name = line[98:].strip()
        m = re.search(r"(\d{10}-\d{2}-\d{6})", file_name)
        accession = m.group(1) if m else file_name.split("/")[-1].replace(".txt", "")
        filings.append({"cik": cik, "accession": accession})
    return filings


def fetch_form4_xml(accession, cik):
    if not cik or not accession:
        return None, "no_cik_or_acc"
    accession_clean = accession.replace("-", "")
    try:
        cik_int = int(cik)
    except ValueError:
        return None, "bad_cik"
    folder = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_clean}/"
    try:
        idx_text = sec_get(folder + "index.json", timeout=12)
        idx = json.loads(idx_text)
        items = idx.get("directory", {}).get("item", [])
        xml_name = None
        for item in items:
            n = item.get("name", "")
            if n.endswith(".xml") and ("form4" in n.lower() or "primary_doc" in n.lower() or "edgar.xml" in n.lower()):
                xml_name = n; break
        if not xml_name:
            for item in items:
                n = item.get("name", "")
                if n.endswith(".xml") and "metadata" not in n.lower() and "wf-form" not in n.lower():
                    xml_name = n; break
        if not xml_name:
            return None, f"no_xml_in_dir (items: {[i.get('name','') for i in items[:6]]})"
        return sec_get(folder + xml_name, timeout=12), "ok"
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}"
    except Exception as e:
        return None, f"err: {type(e).__name__}: {str(e)[:60]}"


def deep_parse(xml_text):
    """Return (success, reason, info)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return False, "xml_parse_error", str(e)[:80]

    issuer = root.find("issuer")
    if issuer is None:
        return False, "no_issuer", ""
    ticker_el = issuer.find("issuerTradingSymbol")
    if ticker_el is None or not (ticker_el.text or "").strip():
        return False, "no_ticker", (issuer.findtext("issuerName") or "")[:40]
    ticker = ticker_el.text.strip().upper()
    if ticker == "N/A":
        return False, "ticker_NA", ""
    if len(ticker) > 6:
        return False, "ticker_too_long", ticker

    nd = root.find("nonDerivativeTable")
    if nd is None:
        has_d = root.find("derivativeTable") is not None
        return False, ("derivative_only" if has_d else "no_tables"), ticker

    txns = nd.findall("nonDerivativeTransaction")
    if not txns:
        holdings = nd.findall("nonDerivativeHolding")
        return False, "holdings_only", f"{ticker} ({len(holdings)} holdings)"

    codes = []
    has_buy = False
    buy_value = 0
    for t in txns:
        coding = t.find("transactionCoding")
        if coding is None:
            codes.append("NO_CODING"); continue
        tcode = (coding.findtext("transactionCode") or "").strip()
        amounts = t.find("transactionAmounts")
        if amounts is None:
            codes.append(f"{tcode}/NO_AMT"); continue
        ad = (amounts.findtext("transactionAcquiredDisposedCode/value") or "").strip()
        codes.append(f"{tcode}/{ad}")
        if tcode == "P" and ad == "A":
            try:
                shares = float(amounts.findtext("transactionShares/value") or 0)
                price  = float(amounts.findtext("transactionPricePerShare/value") or 0)
                if shares > 0 and price > 0:
                    has_buy = True
                    buy_value += shares * price
            except (ValueError, TypeError):
                pass

    if has_buy:
        return True, "ok", f"{ticker}: ${buy_value:,.0f} ({codes})"
    return False, "no_open_market_buy", f"{ticker}: {codes}"


def main():
    section("1) Pull SEC daily index (today + last 2 biz days)")
    today = datetime.now(timezone.utc).date()
    filings = []
    for offset in range(0, 5):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5: continue
        try:
            day_filings = get_daily_form4_filings(d)
            log(f"  {d}: {len(day_filings)} Form 4 filings")
            filings.extend(day_filings)
        except Exception as e:
            log(f"  {d}: FAILED {e}")
        if len(filings) > 200: break

    log(f"  TOTAL: {len(filings)} filings")
    sample = filings[:50]

    section("2) Fetch + parse 50 — verbose per-reject reasons")
    fail_codes = Counter()
    fetch_codes = Counter()
    txn_pairs = Counter()
    samples_by_reason = {}
    n_ok = 0
    log(f"  fetching/parsing {len(sample)} samples (~{len(sample)*2*SEC_MIN_INTERVAL:.0f}s minimum)…")
    for i, f in enumerate(sample):
        xml_text, fetch_reason = fetch_form4_xml(f["accession"], f["cik"])
        if not xml_text:
            fetch_codes[fetch_reason] += 1
            if fetch_reason not in samples_by_reason and len(samples_by_reason) < 12:
                samples_by_reason[f"fetch:{fetch_reason}"] = f
            continue
        success, reason, info = deep_parse(xml_text)
        if success:
            n_ok += 1
            # Tally codes
            m = re.search(r"\[(.*?)\]", info)
            if m:
                for c in re.findall(r"[A-Z]/[A-Z]", m.group(1)):
                    txn_pairs[c] += 1
            samples_by_reason.setdefault(f"OK:{reason}", info)
        else:
            fail_codes[reason] += 1
            if f"PARSE:{reason}" not in samples_by_reason and len(samples_by_reason) < 12:
                samples_by_reason[f"PARSE:{reason}"] = info or "?"

    log("")
    log(f"  TOTAL: {len(sample)}  OK_BUYS: {n_ok}")
    log("")
    log("  ── fetch failures ──")
    for r, n in fetch_codes.most_common():
        log(f"    {r:<35} {n}")
    log("")
    log("  ── parse rejections ──")
    for r, n in fail_codes.most_common():
        log(f"    {r:<35} {n}")
    log("")
    log("  ── samples (one per reason) ──")
    for r, s in samples_by_reason.items():
        log(f"    {r:<32} {str(s)[:120]}")

    section("3) Implications")
    pct_ok = 100*n_ok/len(sample) if sample else 0
    log(f"  Buy-extraction rate: {pct_ok:.1f}% — {n_ok} of {len(sample)}")
    log("  ")
    log("  Most Form 4 filings are NOT open-market buys. They include:")
    log("  - Tax withholdings on RSU vest (code F)")
    log("  - Restricted stock grants (code A, but ad=A and price=$0)")
    log("  - Option exercises (code M)")
    log("  - Sells (code S, ad=D)")
    log("  - Holdings updates (no transactions)")
    log("  - Derivative-only filings")
    log("  ")
    log("  A 5-15% true-buy rate is NORMAL. To get more buys to score, increase MAX_FILINGS_TO_PARSE.")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "diagnose_insider_v3.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
