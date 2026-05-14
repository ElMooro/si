#!/usr/bin/env python3
"""542 — Probe SEC Form 4 XML for AAPL/META/GOOGL to debug BUILD 12 parsing."""
import io, json, os, re, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

REPORT = "aws/ops/reports/542_form4_xml_probe.json"

HEADERS = {
    "User-Agent": "JustHodl.AI raafouis@gmail.com",
    "Accept": "application/json",
    "Host-Hint": "data.sec.gov",
}

CIKS = [
    {"ticker": "AAPL", "cik": "320193"},
    {"ticker": "META", "cik": "1326801"},
    {"ticker": "GOOGL", "cik": "1652044"},
    {"ticker": "WMT", "cik": "104169"},
]


def http_get(url, json_mode=False):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl.AI raafouis@gmail.com",
        "Accept": "application/json" if json_mode else "*/*",
        "Accept-Encoding": "gzip,deflate",
        "Host": "data.sec.gov" if "data.sec.gov" in url else "www.sec.gov",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read()
        # gzip?
        if r.info().get("Content-Encoding") == "gzip":
            import gzip
            body = gzip.decompress(body)
        body = body.decode("utf-8", "replace")
        return json.loads(body) if json_mode else body


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "by_ticker": {}}
    cutoff_30d = datetime.now(timezone.utc).date() - timedelta(days=30)

    for entry in CIKS:
        tkr = entry["ticker"]
        cik = entry["cik"].zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        tinfo = {"ticker": tkr, "cik": cik}
        try:
            sub = http_get(url, json_mode=True)
            recent = (sub.get("filings") or {}).get("recent") or {}
            forms = recent.get("form") or []
            dates = recent.get("filingDate") or []
            accs = recent.get("accessionNumber") or []
            primaries = recent.get("primaryDocument") or []

            f4_indices = [i for i, f in enumerate(forms) if f == "4"]
            tinfo["n_total_filings"] = len(forms)
            tinfo["n_form4_total"] = len(f4_indices)

            recent_meta = []
            for i in f4_indices[:8]:
                try:
                    d = datetime.strptime(dates[i], "%Y-%m-%d").date()
                    recent_meta.append({
                        "date": dates[i],
                        "accession": accs[i],
                        "primary": primaries[i] if i < len(primaries) else None,
                    })
                except Exception: pass
            tinfo["recent_form4_filings"] = recent_meta

            # Probe FIRST form 4 to see actual XML
            if recent_meta:
                f0 = recent_meta[0]
                acc_nodash = f0["accession"].replace("-", "")
                xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{f0.get('primary') or 'form4.xml'}"
                try:
                    xml = http_get(xml_url, json_mode=False)
                    tinfo["xml_url"] = xml_url
                    tinfo["xml_size"] = len(xml)
                    tinfo["xml_first_3kb"] = xml[:3000]

                    # Test the regex pattern from the lambda
                    blocks = re.findall(
                        r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>',
                        xml, re.DOTALL)
                    tinfo["n_nonDerivative_blocks"] = len(blocks)
                    # Also derivative
                    dblocks = re.findall(
                        r'<derivativeTransaction>(.*?)</derivativeTransaction>',
                        xml, re.DOTALL)
                    tinfo["n_derivative_blocks"] = len(dblocks)
                    # holdings (no transactions, just snapshots)
                    hblocks = re.findall(
                        r'<nonDerivativeHolding>(.*?)</nonDerivativeHolding>',
                        xml, re.DOTALL)
                    tinfo["n_nonDerivative_holdings"] = len(hblocks)

                    # Parse codes
                    codes = []
                    for b in blocks:
                        cm = re.search(r'<transactionCode>([^<]+)</transactionCode>', b)
                        sm = re.search(r'<transactionShares>\s*<value>([\d.]+)</value>', b)
                        pm = re.search(r'<transactionPricePerShare>\s*<value>([\d.]+)</value>', b)
                        codes.append({
                            "code": cm.group(1).strip() if cm else None,
                            "shares": float(sm.group(1)) if sm else None,
                            "price": float(pm.group(1)) if pm else None,
                        })
                    tinfo["parsed_codes"] = codes
                    # Insider info
                    rn = re.search(r'<rptOwnerName>([^<]+)</rptOwnerName>', xml)
                    rt = re.search(r'<officerTitle>([^<]+)</officerTitle>', xml)
                    tinfo["insider"] = rn.group(1).strip() if rn else None
                    tinfo["officer_title"] = rt.group(1).strip() if rt else None
                except Exception as e:
                    tinfo["xml_err"] = str(e)[:200]
        except Exception as e:
            tinfo["err"] = str(e)[:200]
        out["by_ticker"][tkr] = tinfo

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
