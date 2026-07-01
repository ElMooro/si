"""ops 2680 — check the deployed buildout list directly for genuine duplicates
(same company + same date + same form = true dup; different date/form = legitimate
separate filings)."""
import boto3, json
from collections import Counter
s3 = boto3.client("s3", region_name="us-east-1")
j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/structural-pre-signals.json")["Body"].read())
items = j.get("buildout", {}).get("items", [])
print(f"total buildout items: {len(items)}")
keys = Counter((r.get("ticker"), r.get("company"), r.get("file_date"), r.get("form")) for r in items)
dupes = {k: v for k, v in keys.items() if v > 1}
print(f"exact-duplicate (ticker,company,date,form) tuples: {len(dupes)}")
for k, v in dupes.items():
    print(f"  {k} x{v}")
    for r in items:
        if (r.get("ticker"), r.get("company"), r.get("file_date"), r.get("form")) == k:
            print(f"    _url={r.get('filing_url')}")
print("DONE 2680")
