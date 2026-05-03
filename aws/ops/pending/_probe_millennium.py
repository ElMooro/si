"""Probe MILLENNIUM 13F filing to find why parse_returned_empty / infotable_not_found."""
import urllib.request
import json
import re

from ops_report import report

USER_AGENT = "JustHodl Research raafouis@gmail.com"


def _fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json,application/xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("probe_millennium_13f") as r:
        r.heading("Probe MILLENNIUM 13F filing")

        # MILLENNIUM CIK=1273087, accession=0001273087-26-000002
        filing_dir = "https://www.sec.gov/Archives/edgar/data/1273087/000127308726000002/"

        r.section("1. Index")
        try:
            raw = _fetch(filing_dir + "index.json")
            idx = json.loads(raw.decode("utf-8"))
            for i in idx.get("directory", {}).get("item", []):
                r.log(f"  {i.get('name'):50s} {i.get('size'):>15} bytes")
        except Exception as e:
            r.fail(f"  fetch index fail: {e}")
            return

        r.section("2. Probe MLP_Filing_20251231_v1.xml")
        url = filing_dir + "MLP_Filing_20251231_v1.xml"
        try:
            text = _fetch(url, timeout=15).decode("utf-8", errors="ignore")
            r.log(f"  size: {len(text)} chars")
            r.log(f"\n  first 600 chars:")
            for line in text[:600].split("\n")[:15]:
                r.log(f"    {line[:130]}")
            r.log(f"\n  Has '<infoTable': {('<infoTable' in text)}")
            r.log(f"  Has 'infoTable' anywhere: {('infoTable' in text)}")
            r.log(f"  Has '<infotable': {('<infotable' in text.lower())}")
            r.log(f"  Has '<information': {('<information' in text)}")
            # Actual unique tags
            tags = set(re.findall(r"<(\w+(?::\w+)?)\b", text))
            r.log(f"  unique tags: {sorted(tags)[:25]}")
        except Exception as e:
            r.fail(f"  fetch xml fail: {e}")

        r.section("3. Probe primary_doc.xml")
        url = filing_dir + "primary_doc.xml"
        try:
            text = _fetch(url, timeout=15).decode("utf-8", errors="ignore")
            r.log(f"  size: {len(text)} chars")
            r.log(f"  Has '<infoTable': {('<infoTable' in text)}")
            r.log(f"\n  first 400 chars:")
            for line in text[:400].split("\n")[:10]:
                r.log(f"    {line[:130]}")
        except Exception as e:
            r.fail(f"  fetch primary_doc fail: {e}")


if __name__ == "__main__":
    main()
