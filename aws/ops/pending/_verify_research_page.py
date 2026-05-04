# rerun-marker: 1777919736
"""Verify research.html is live on GH Pages, and verify the data sources it depends on."""
import urllib.request
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("verify_research_page") as r:
        r.heading("research.html on GH Pages")
        try:
            req = urllib.request.Request("https://justhodl.ai/research.html", headers={"User-Agent": "audit/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                size = len(resp.read())
                r.ok(f"  ✓ status={resp.status}  size={size:,}b")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("Data sources the page reads")
        sources = [
            "screener/data.json",
            "data/momentum-scanner.json",
            "data/sector-rotation.json",
            "data/earnings-tracker.json",
            "data/short-interest.json",
            "data/insider-trades.json",
            "data/calibration-snapshot.json",
        ]
        for s_key in sources:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=s_key)
                r.ok(f"  ✓ {s_key:40s} {obj['ContentLength']:>10,}b  {obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {s_key}: {e}")

        # Test specific tickers exist
        r.heading("Spot-check NVDA, INTC, AAPL, BRK.B in screener data")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")["Body"].read())
        for t in ["NVDA", "INTC", "AAPL", "BRK.B", "SNDK", "META"]:
            stock = next((s for s in (d.get("stocks") or []) if s.get("symbol") == t), None)
            if stock:
                r.log(f"  ✓ {t:7s} ${stock.get('price','—')}  PE={stock.get('peRatio','—')}  ROE={stock.get('roe','—')}  3m={stock.get('chg3m','—')}%")
            else:
                r.log(f"  ✗ {t}: not in screener")


if __name__ == "__main__":
    main()
