"""Probe a failing fund's actual XML — why parse fails."""
import json
import urllib.request
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
USER_AGENT = "JustHodl Research raafouis@gmail.com"
s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("probe_failing_fund_xml") as r:
        r.heading("Probe XML structure of failing funds")

        idx = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-positions.json")["Body"].read())

        # Test 4 still-failing funds
        for fkey in ["AQR", "PERSHING", "CITADEL", "SCION"]:
            r.section(f"── {fkey} ──")
            meta = idx.get("by_fund", {}).get(fkey, {})
            latest = meta.get("latest_filing", {})
            acc = latest.get("accession", "")
            cik = meta.get("cik", "")
            cik_int = str(int(cik))
            acc_clean = acc.replace("-", "")
            base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"

            try:
                idx_json = json.loads(fetch(base + "index.json").decode("utf-8"))
                items = idx_json.get("directory", {}).get("item", [])
                xml_files = [i for i in items if i.get("name", "").endswith(".xml")]

                # Find the largest non-primary xml (the infotable)
                non_primary = [i for i in xml_files if i.get("name") != "primary_doc.xml"]
                if not non_primary:
                    r.log(f"  no non-primary XML found")
                    continue

                target = non_primary[0]
                xml_url = base + target["name"]
                r.log(f"  fetching {target['name']} ({target.get('size','?')} bytes)…")
                body = fetch(xml_url, timeout=20).decode("utf-8", errors="ignore")

                # First 800 chars
                r.log(f"  first 800 chars:")
                r.log(f"    {body[:800].replace(chr(10), ' ')[:780]}")

                # Search for key markers
                r.log(f"\n  marker check:")
                r.log(f"    has '<infoTable':         {body.count('<infoTable')}")
                r.log(f"    has '<ns1:infoTable':     {body.count('<ns1:infoTable')}")
                r.log(f"    has '<ns2:infoTable':     {body.count('<ns2:infoTable')}")
                r.log(f"    has '<n:infoTable':       {body.count('<n:infoTable')}")
                r.log(f"    has '<nameOfIssuer':      {body.count('<nameOfIssuer')}")
                r.log(f"    has 'xmlns=':             {body.count('xmlns=')}")
                r.log(f"    has 'cusip':              {body.count('<cusip')}")

                # Try the actual fix code
                import re
                cleaned = re.sub(r"<(/?)\w+:", r"\g<1>", body)
                cleaned = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", cleaned)
                r.log(f"\n  after cleaning: '<infoTable' count = {cleaned.count('<infoTable')}")
                r.log(f"  after cleaning first 400:")
                r.log(f"    {cleaned[:400].replace(chr(10),' ')}")

                import xml.etree.ElementTree as ET
                try:
                    root = ET.fromstring(cleaned)
                    info_tables = list(root.iter("infoTable"))
                    r.log(f"\n  ET.iter('infoTable'): {len(info_tables)} matches")
                    if info_tables:
                        first = info_tables[0]
                        name = first.findtext("nameOfIssuer")
                        cusip = first.findtext("cusip")
                        value = first.findtext("value")
                        r.log(f"  first record: name='{name}' cusip='{cusip}' value='{value}'")
                except ET.ParseError as e:
                    r.log(f"  ET parse error: {e}")
            except Exception as e:
                r.log(f"  {fkey}: probe error {e}")


if __name__ == "__main__":
    main()
