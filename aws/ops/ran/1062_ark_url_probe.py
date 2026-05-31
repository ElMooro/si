#!/usr/bin/env python3
"""1062 — probe ARK URL variants to find the actually-working format.

Tests 6 URL variants for each ETF to find what works post-2025 CMS path change:
  - New path + URL-encoded ampersand (%26)
  - New path + raw ampersand
  - New path + no_ampersand_in_filename (different naming convention)
  - Direct fund page scrape to extract "Fund Holdings CSV" link
"""
import json, os, pathlib, time, urllib.request, urllib.error, re
from datetime import datetime, timezone

REPORT = "aws/ops/reports/1062_ark_url_probe.json"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

# Multiple URL variants to test
NEW_BASE = "https://ark-funds.com/wp-content/fundsiteliterature/csv/"
OLD_BASE = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/"

VARIANTS = {
    "ARKK": [
        f"{NEW_BASE}ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
        f"{NEW_BASE}ARKK_HOLDINGS.csv",  # short form
        f"{NEW_BASE}ARK%20INNOVATION%20ETF%20ARKK%20HOLDINGS.csv",
    ],
    "ARKQ": [
        f"{NEW_BASE}ARK_AUTONOMOUS_TECH_%26_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
        f"{NEW_BASE}ARK_AUTONOMOUS_TECH_AND_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
        f"{NEW_BASE}ARKQ_HOLDINGS.csv",
    ],
}


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"status": r.status, "size": len(body),
                     "content_type": r.headers.get("Content-Type", ""),
                     "preview": body[:300].decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:120]}"}


def scrape_fund_page(symbol):
    """Look at the actual fund page HTML for a 'Fund Holdings CSV' link."""
    url = f"https://www.ark-funds.com/funds/{symbol.lower()}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
            # Look for CSV links
            csv_links = re.findall(r'href=["\']([^"\']*\.csv[^"\']*)["\']', html, re.IGNORECASE)
            # Look for holdings keywords near links
            return {
                "page_size": len(html),
                "n_csv_links": len(csv_links),
                "csv_links": list(set(csv_links))[:10],
                "first_500": html[:500],
            }
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:
        return {"err": str(e)[:120]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    print("[1062] phase 1: test URL variants for ARKK + ARKQ…")
    out["variants"] = {}
    for fund, urls in VARIANTS.items():
        out["variants"][fund] = []
        for url in urls:
            r = fetch(url)
            r["url"] = url
            out["variants"][fund].append(r)
            time.sleep(0.4)
    
    print("[1062] phase 2: scrape ARK fund pages for CSV links…")
    out["scrapes"] = {}
    for sym in ["arkk", "arkq", "arkw", "arkf", "arkg", "arkx"]:
        out["scrapes"][sym] = scrape_fund_page(sym)
        time.sleep(0.5)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1062] DONE → {REPORT}")


if __name__ == "__main__":
    main()
