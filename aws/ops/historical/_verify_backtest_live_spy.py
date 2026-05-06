"""Wait for cache TTL then verify backtest.html has SPY integration live."""
import json
import time
import urllib.request

import boto3
from ops_report import report

REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)


def fetch(url, no_cache=True):
    headers = {"User-Agent": "verify/2.0"}
    if no_cache:
        headers["Cache-Control"] = "no-cache"
        headers["Pragma"] = "no-cache"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None, str(e)


def main():
    with report("verify_backtest_live_spy") as r:
        # Step 1: wait for cache propagation
        r.heading("1) Wait 30s for cache propagation, then fetch with cache-bust")
        time.sleep(30)
        cb = int(time.time())
        code, body = fetch(f"https://justhodl.ai/backtest.html?cb={cb}")
        r.log(f"  cache-busted fetch: {code}, {len(body):,}b")

        for label, sub in [
            ("5 KPI grid", "grid-template-columns:repeat(5,1fr)"),
            ("Alpha vs SPY KPI", "Alpha vs SPY"),
            ("SPY Buy & Hold KPI", "SPY Buy &amp; Hold"),
            ("hasSpy chart logic", "hasSpy"),
            ("Strategy NAV legend", "Strategy NAV"),
        ]:
            r.log(f"    {'✓' if sub in body else '✗'} {label}")

        # Step 2: Bucket inspection — was HTML actually deployed to S3?
        r.heading("2) Check if backtest.html exists in S3")
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key="backtest.html")
            r.log(f"  ✓ s3://justhodl-dashboard-live/backtest.html: {head['ContentLength']:,}b mod={head['LastModified'].isoformat()}")
        except Exception as e:
            r.log(f"  ✗ Not in S3 bucket: {e}")
            r.log(f"  → backtest.html is served from GitHub Pages, not S3")

        # Step 3: confirm the data flowed end-to-end
        r.heading("3) Confirm backtest data has SPY benchmark in S3")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  ✓ backtest/results.json: {head.get('ContentLength', '?')}b")
            r.log(f"  Strategy: ${summ.get('final_nav')}  ({summ.get('total_return_pct'):+.2f}%)")
            r.log(f"  SPY:      ${summ.get('spy_final_nav')}  ({summ.get('spy_return_pct'):+.2f}%)")
            r.log(f"  Alpha:    {summ.get('alpha_vs_spy_pct'):+.2f}%")
            r.log(f"  Sharpe:   {summ.get('sharpe_proxy')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
