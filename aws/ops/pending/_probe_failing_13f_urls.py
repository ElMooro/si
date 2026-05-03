"""
Probe failing 13F filings to find the URL pattern.

For each failing fund (AQR, PERSHING, SCION, etc.), check:
  1. Does the WATCHLIST_CIK/ACCESSION/ URL exist?
  2. Does the FILER_CIK_FROM_ACCESSION/ACCESSION/ URL exist?
  3. What .xml files are present?
  4. Try fetching infotable.xml directly
"""
import urllib.request
import json

from ops_report import report

USER_AGENT = "JustHodl Research raafouis@gmail.com"


# (watchlist_cik, accession, fund_name)
FAILING = [
    ("0001167557", "0001085146-26-000240", "AQR"),
    ("0001336528", "0001172661-26-001091", "PERSHING"),
    ("0001423053", "0001104659-26-016408", "CITADEL"),
    ("0001273087", "0001273087-26-000002", "MILLENNIUM"),
    ("0001167483", "0000919574-26-001143", "TIGER_GLOBAL"),
    ("0001135730", "0000919574-26-001239", "COATUE"),
    ("0001061165", "0000902664-26-001084", "BAUPOST"),
    ("0001286922", "0001214659-26-XXXXX", "ELLIOTT"),  # may not have accession
    ("0001649339", "0001649339-26-000001", "SCION"),
    ("0001603466", "0001603466-26-000001", "POINT72"),
    ("0001029160", "0000902664-26-000998", "SOROS"),
    ("0001079114", "0001172661-24-001512", "GREENLIGHT"),
]


def _fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("probe_failing_13f_urls") as r:
        r.heading("Probe URL patterns for failing 13F funds")

        for watchlist_cik, accession, fund_name in FAILING:
            r.section(f"── {fund_name} ──")
            cik_int = str(int(watchlist_cik))
            acc_clean = accession.replace("-", "")
            filer_cik = accession.split("-")[0]
            filer_int = str(int(filer_cik))

            r.log(f"  watchlist CIK: {cik_int}")
            r.log(f"  accession: {accession}")
            r.log(f"  filer-prefix-CIK from accession: {filer_int}")

            # Try with WATCHLIST CIK
            url1 = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/index.json"
            try:
                raw = _fetch(url1)
                idx = json.loads(raw)
                items = idx.get("directory", {}).get("item", [])
                xmls = [i["name"] for i in items if i["name"].endswith(".xml")]
                r.log(f"  ✓ watchlist-CIK URL works → {len(xmls)} xml files: {xmls[:5]}")
                continue
            except Exception as e:
                r.log(f"  ✗ watchlist-CIK URL fails: {type(e).__name__}: {str(e)[:80]}")

            # Try with FILER prefix CIK
            url2 = f"https://www.sec.gov/Archives/edgar/data/{filer_int}/{acc_clean}/index.json"
            try:
                raw = _fetch(url2)
                idx = json.loads(raw)
                items = idx.get("directory", {}).get("item", [])
                xmls = [i["name"] for i in items if i["name"].endswith(".xml")]
                r.log(f"  ✓ filer-prefix-CIK URL works → {len(xmls)} xml files: {xmls[:5]}")
            except Exception as e:
                r.log(f"  ✗ filer-prefix-CIK URL fails: {type(e).__name__}: {str(e)[:80]}")


if __name__ == "__main__":
    main()
