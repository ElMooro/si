"""
Probe v2: Fetch filing index via data.sec.gov filing-index JSON endpoint
which is more reliable than the cgi-bin browse interface.

The standard SEC archive filing has these files:
  - primary_doc.xml         — coverpage (filer name, period, etc.)
  - infotable.xml           — actual holdings (CUSIP, name, value, shares)
    OR sometimes form13fInfoTable.xml
"""
import json
import re
import urllib.request

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
USER_AGENT = "JustHodl Research raafouis@gmail.com"

s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("probe_13f_v2") as r:
        r.heading("Probe 13F infotable XML format (v2)")

        # Berkshire's most recent filing details from prior probe
        cik_int = "1067983"
        accession = "0001193125-26-054580"
        acc_clean = accession.replace("-", "")
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"
        r.log(f"  filing dir: {base_url}")

        r.section("1. Try filing-index JSON")
        # SEC publishes a JSON manifest at /index.json
        index_json_url = f"{base_url}index.json"
        try:
            data = json.loads(fetch(index_json_url).decode("utf-8"))
            items = data.get("directory", {}).get("item", [])
            r.log(f"  files in filing:")
            for item in items:
                name = item.get("name", "")
                size = item.get("size", "")
                r.log(f"    {name}  ({size} bytes)")
        except Exception as e:
            r.log(f"  index.json failed: {e}")

        r.section("2. Attempt to fetch infotable directly")
        # Common naming patterns
        candidates = [
            "infotable.xml",
            "form13fInfoTable.xml",
            "informationTable.xml",
            "primary_doc.xml",
        ]
        # Find via index first
        try:
            data = json.loads(fetch(index_json_url).decode("utf-8"))
            items = data.get("directory", {}).get("item", [])
            xml_files = [i["name"] for i in items if i.get("name", "").endswith(".xml")]
            r.log(f"  xml files in filing: {xml_files}")

            # Try each xml — find which is the holdings table
            for xml_name in xml_files:
                xml_url = base_url + xml_name
                try:
                    xml_text = fetch(xml_url).decode("utf-8", errors="ignore")
                    # Look for known infotable elements
                    if "<infoTable>" in xml_text or "<ns1:infoTable" in xml_text or "<infoTable " in xml_text:
                        r.log(f"  ✓ infotable found: {xml_name}")
                        # Show first 2 records
                        # Extract first 2000 chars after first <infoTable>
                        idx = xml_text.find("<infoTable")
                        if idx == -1:
                            idx = xml_text.find("<ns1:infoTable")
                        snippet = xml_text[idx:idx + 3000]
                        r.log(f"  first 2 records:")
                        r.log(f"    {snippet[:2500]}")
                        break
                    elif "<periodOfReport>" in xml_text:
                        r.log(f"  - {xml_name}: cover page (not holdings)")
                except Exception as e:
                    r.log(f"  fetch {xml_name}: {e}")
        except Exception as e:
            r.log(f"  v2 probe failed: {e}")


if __name__ == "__main__":
    main()

