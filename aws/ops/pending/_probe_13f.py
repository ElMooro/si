"""
Probe: what's in data/institutional-positions.json right now,
and what does an actual 13F infotable XML look like?

This is fact-finding before building the position-extracting Lambda.
"""
import json
import urllib.request

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
USER_AGENT = "JustHodl Research raafouis@gmail.com"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("probe_13f_state") as r:
        r.heading("Probe 13F state + SEC infotable format")

        r.section("1. Current data/institutional-positions.json")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/institutional-positions.json")
            data = json.loads(obj["Body"].read())
        except Exception as e:
            r.fail(f"  {e}")
            return

        r.log(f"  generated_at: {data.get('generated_at')}")
        r.log(f"  tracked_funds: {data.get('tracked_funds')}")
        r.log(f"  filings_seen: {data.get('filings_seen')}")
        r.log(f"  new_filings: {len(data.get('new_filings', []))}")
        r.log(f"  by_fund keys: {list(data.get('by_fund', {}).keys())}")

        # Show one fund example
        fund = data.get("by_fund", {}).get("BERKSHIRE", {})
        r.log(f"\n  Sample (Berkshire):")
        r.log(f"    {json.dumps(fund, indent=2, default=str)[:600]}")

        # Sample new filings
        if data.get("new_filings"):
            r.log(f"\n  Sample new filing:")
            r.log(f"    {json.dumps(data['new_filings'][0], indent=2, default=str)[:400]}")

        r.section("2. Fetch one real 13F infotable XML to learn schema")
        # Use Berkshire's most recent filing
        latest = (data.get("by_fund", {}).get("BERKSHIRE", {}).get("latest_filing", {}))
        accession = latest.get("accession")
        if not accession:
            r.fail("  no Berkshire accession found")
            return

        # Berkshire CIK = 0001067983
        cik_int = "1067983"
        acc_clean = accession.replace("-", "")
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"
        r.log(f"  filing index URL: {idx_url}")

        req = urllib.request.Request(
            f"https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&CIK={cik_int}&type=13F-HR&dateb=&owner=include&count=5",
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                browse_html = resp.read().decode("utf-8", errors="ignore")
            # Find the latest accession's full index URL
            import re
            m = re.search(r'href="(/Archives/edgar/data/\d+/\d+/\d+-index\.htm)"', browse_html)
            if m:
                index_url = "https://www.sec.gov" + m.group(1)
                r.log(f"  resolved index: {index_url}")

                # Fetch the index page to find the infotable.xml
                req2 = urllib.request.Request(index_url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req2, timeout=15) as resp:
                    index_html = resp.read().decode("utf-8", errors="ignore")

                # Find any .xml link that's an infotable (not the primary doc)
                xml_matches = re.findall(r'href="([^"]+\.xml)"', index_html)
                r.log(f"  xml files found: {xml_matches[:8]}")

                # Try to fetch the infotable (usually has 'infotable' or similar in name)
                infotable_url = None
                for x in xml_matches:
                    name = x.split("/")[-1].lower()
                    if "infotable" in name or "form13fInfoTable" in name.lower():
                        infotable_url = x if x.startswith("http") else "https://www.sec.gov" + x
                        break
                # Fallback: try the largest .xml
                if not infotable_url and xml_matches:
                    infotable_url = (xml_matches[0] if xml_matches[0].startswith("http")
                                     else "https://www.sec.gov" + xml_matches[0])
                r.log(f"  infotable url: {infotable_url}")

                if infotable_url:
                    req3 = urllib.request.Request(infotable_url, headers={"User-Agent": USER_AGENT})
                    with urllib.request.urlopen(req3, timeout=20) as resp:
                        infotable_xml = resp.read().decode("utf-8", errors="ignore")
                    r.log(f"  infotable size: {len(infotable_xml)} bytes")
                    # Show first 1500 chars
                    r.log(f"  infotable preview:")
                    r.log(f"    {infotable_xml[:1500]}")
        except Exception as e:
            r.log(f"  fetch error: {e}")


if __name__ == "__main__":
    main()
