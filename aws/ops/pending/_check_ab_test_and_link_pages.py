"""Check ab-test S3 file at correct path, and confirm ticker.html / feedback.html are GH-Pages-deployed."""
import time
import urllib.request
import boto3
from ops_report import report

REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("check_ab_test_and_link_pages") as r:
        r.heading("Final wrap-up — verify ab-test path + GH Pages")

        # ab-test correct path
        try:
            h = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/ab-test-results.json")
            age_min = (time.time() - h["LastModified"].timestamp()) / 60
            r.ok(f"  ✓ data/ab-test-results.json: {h['ContentLength']:,}b age={age_min:.1f}min")
        except Exception as e:
            r.log(f"  ✗ data/ab-test-results.json: {e}")

        # GitHub Pages
        for f in ["ticker.html", "feedback.html"]:
            url = f"https://justhodl.ai/{f}"
            try:
                req = urllib.request.Request(url, method="HEAD")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    r.ok(f"  ✓ GET {url} → {resp.status}")
            except Exception as e:
                r.log(f"  ✗ {url}: {e}")

        # The Function URL
        try:
            req = urllib.request.Request("https://vmzexqk56frz3dvpo6nioe5ylm0kijlj.lambda-url.us-east-1.on.aws/signals?limit=3")
            with urllib.request.urlopen(req, timeout=12) as resp:
                body = resp.read().decode()[:300]
            r.ok(f"  ✓ feedback URL signals → {len(body)}b")
            r.log(f"    {body}")
        except Exception as e:
            r.log(f"  ✗ feedback URL: {e}")

        # Sample whats-changed payload
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/whats-changed.json")
            import json
            d = json.loads(obj["Body"].read())
            r.ok(f"  ✓ whats-changed: as_of={d.get('as_of')} n={d.get('n_changes')}")
        except Exception as e:
            r.log(f"  ✗ whats-changed: {e}")


if __name__ == "__main__":
    main()
