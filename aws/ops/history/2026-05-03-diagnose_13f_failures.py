"""
Why did 14 of 18 funds fail to parse in the first 13F-positions run?

Pull the actual S3 output and check fund_errors.
"""
import json
import urllib.request

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
USER_AGENT = "JustHodl Research raafouis@gmail.com"

s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("diagnose_13f_failures") as r:
        r.heading("Why did 14 of 18 funds fail?")

        try:
            data = json.loads(s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")["Body"].read())
        except Exception as e:
            r.fail(f"  read failed: {e}")
            return

        r.section("Output summary")
        r.log(f"  generated_at: {data.get('generated_at')}")
        r.log(f"  funds_parsed: {data.get('funds_parsed')}")
        r.log(f"  funds_failed: {data.get('funds_failed')}")
        r.log(f"  funds OK: {list(data.get('by_fund', {}).keys())}")

        r.section("Fund errors (per-fund)")
        errors = data.get("fund_errors", [])
        for e in errors:
            r.log(f"  {e.get('fund_key', '?')}: {e.get('error', '?')}")

        r.section("Sample one failing fund's filing")
        # Pick one error with infotable_not_found, fetch its filing index
        idx_data = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-positions.json")["Body"].read())

        for err in errors[:5]:
            fkey = err.get("fund_key")
            meta = idx_data.get("by_fund", {}).get(fkey, {})
            latest = meta.get("latest_filing", {})
            acc = latest.get("accession", "")
            cik = meta.get("cik", "")
            if not acc:
                continue
            cik_int = str(int(cik))
            acc_clean = acc.replace("-", "")
            base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"
            r.log(f"\n  ── {fkey} ({err.get('error', '?')}) ──")
            r.log(f"  filing dir: {base}")
            try:
                idx_json = json.loads(fetch(base + "index.json").decode("utf-8"))
                items = idx_json.get("directory", {}).get("item", [])
                xml_files = [i for i in items if i.get("name", "").endswith(".xml")]
                r.log(f"  xml files: {[i['name'] for i in xml_files]}")

                # For each, fetch first 800 chars to see structure
                for x in xml_files[:3]:
                    try:
                        body = fetch(base + x["name"], timeout=12).decode("utf-8", errors="ignore")
                        # Show what kind of XML this is
                        first_500 = body[:500].replace("\n", " ")
                        has_info = "<infoTable" in body or "<ns1:infoTable" in body
                        r.log(f"    {x['name']}: has_infoTable={has_info}, size={len(body)}")
                        if not has_info:
                            r.log(f"      preview: {first_500[:300]}")
                    except Exception as e:
                        r.log(f"    {x['name']}: fetch fail {e}")
            except Exception as e:
                r.log(f"  index fetch fail: {e}")


if __name__ == "__main__":
    main()
