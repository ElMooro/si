"""Inspect inputs for Wave 3 candidates — options data, news sources, screener tickers."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_wave3_inputs") as r:
        r.heading("Inspect Wave 3 inputs")

        # 1. Options/skew - check if options-flow Lambda exists
        r.heading("1. Options/skew availability")
        for key in ["data/options-flow.json", "data/flow-data.json", "flow-data.json", "data/iv-surface.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.ok(f"  ✓ {key}  {obj['ContentLength']:,}b")
            except Exception:
                r.log(f"  ✗ {key} missing")

        # 2. News sources
        r.heading("2. News sources")
        for key in ["data/news-feed.json", "news.json", "data/morning-intelligence.json", "data/intel.json", "intel.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.ok(f"  ✓ {key}  {obj['ContentLength']:,}b  modified={obj['LastModified']}")
            except Exception:
                r.log(f"  ✗ {key} missing")

        # 3. Screener
        r.heading("3. Screener tickers (S&P 500)")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
            d = json.loads(obj["Body"].read())
            n = len(d.get("stocks", []) or d.get("results", []) or d)
            r.ok(f"  ✓ screener/data.json — {n} entries")
            sample = d.get("stocks") or d.get("results") or []
            if sample:
                first = sample[0]
                r.log(f"  sample keys: {list(first.keys())[:15]}")
        except Exception as e:
            r.log(f"  ✗ screener: {e}")

        # 4. VIX/options-flow rich data
        r.heading("4. Polygon options entitlement check (test direct snapshot)")
        import urllib.request, urllib.parse
        for url in [
            "https://api.polygon.io/v3/snapshot/options/SPY?apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d&limit=5",
        ]:
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    body = resp.read().decode()[:300]
                    r.log(f"  ✓ {url[:80]}: {body[:200]}")
            except Exception as e:
                r.log(f"  ✗ {url[:80]}: {e}")

        # 5. Insider trades + earnings (for news.html)
        r.heading("5. Other Wave 3 inputs")
        for key in ["data/insider-trades.json", "data/13f-changes.json", "data/earnings-tracker.json", "data/whats-changed.json", "data/morning-brief.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.ok(f"  ✓ {key}  {obj['ContentLength']:,}b")
            except Exception:
                r.log(f"  ✗ {key} missing")


if __name__ == "__main__":
    main()
