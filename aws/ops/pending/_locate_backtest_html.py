"""Verify backtest.html is published, locate it, and inspect existing sections."""
import boto3
from ops_report import report

REGION = "us-east-1"
S3 = boto3.client("s3", region_name=REGION)


def main():
    with report("locate_backtest_html") as r:
        r.heading("Searching for backtest.html in S3 bucket")
        # List all keys matching backtest*
        try:
            paginator = S3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
                for obj in page.get("Contents", []):
                    if "backtest" in obj["Key"].lower() and obj["Key"].endswith(".html"):
                        keys.append((obj["Key"], obj["Size"], obj["LastModified"]))
            r.log(f"  Found {len(keys)} backtest*.html keys:")
            for k, s, m in keys:
                r.log(f"    {k}  {s:,}b  {m}")

            # Just look for the canonical backtest.html
            try:
                obj = S3.head_object(Bucket="justhodl-dashboard-live", Key="backtest.html")
                r.log(f"\n  ✓ backtest.html EXISTS: {obj['ContentLength']:,}b mod={obj['LastModified']}")
                # Read it
                content = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest.html")["Body"].read().decode()
                r.log(f"\n  Inspecting structure:")
                r.log(f"    has loadResults: {'loadResults' in content}")
                r.log(f"    has loadCallsBacktest: {'loadCallsBacktest' in content}")
                r.log(f"    has horizon attribution: {'horizon' in content.lower()}")
                r.log(f"    has nav_curve: {'nav_curve' in content}")
                r.log(f"    line count: {content.count(chr(10))}")
                # Find anchor points where we'd inject new section
                for marker in ["<h2", "<section", "</body>", "id=\"results\"", "id=\"chart\""]:
                    cnt = content.count(marker)
                    r.log(f"    occurrences of '{marker}': {cnt}")
            except Exception as e:
                r.log(f"  ✗ HEAD backtest.html: {e}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Also check S3 for backtest data
        r.heading("Backtest data files in S3")
        try:
            for k in ["backtest/results.json", "backtest/summary.json"]:
                try:
                    o = S3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                    r.log(f"  ✓ {k}  {o['ContentLength']:,}b mod={o['LastModified']}")
                except Exception as e:
                    r.log(f"  ✗ {k}: {e}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Decisive-call ledger
        r.heading("Decisive-call ledger (input for #3)")
        try:
            import json
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            d = json.loads(obj["Body"].read())
            calls = d if isinstance(d, list) else (d.get("calls") or d.get("history") or [])
            r.log(f"  total calls in ledger: {len(calls)}")
            r.log(f"  first call timestamp: {calls[0].get('as_of') if calls else '—'}")
            r.log(f"  last call timestamp: {calls[-1].get('as_of') if calls else '—'}")
            r.log(f"  call structure (last call keys): {list(calls[-1].keys()) if calls else '—'}")
            r.log("")
            r.log("  Recent verbs:")
            for c in calls[-10:]:
                r.log(f"    {c.get('as_of')}  verb={c.get('verb') or c.get('call')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
