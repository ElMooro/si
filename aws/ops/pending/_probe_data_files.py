"""Probe what S3 data files exist + their freshness for the morning-intel rewrite."""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

KEYS_TO_CHECK = [
    # Existing wired-in
    "data/report.json", "intelligence-report.json", "crypto-intel.json",
    "edge-data.json", "repo-data.json", "flow-data.json",
    "screener/data.json", "predictions.json", "valuations-data.json",
    "regime/current.json", "divergence/current.json",
    # NEW — Tier S+A
    "data/gdelt-sentiment.json", "data/aaii-sentiment.json",
    "data/sec-insider-clusters.json", "data/sec-13f-filings.json",
    "data/13f-positions.json", "data/sec-10kq.json",
    "data/sec-8k-redflags.json", "data/red-flags.json",
    "data/options-gamma.json", "data/onchain-ratios.json",
    "data/labor-leading.json", "data/oecd-cli.json",
    "data/nyfed-dealer-survey.json", "data/price-redundancy.json",
    # NEW — Tier 1-3
    "data/liquidity-flow.json", "data/exchange-flows.json",
    "data/vix-curve.json",
    # NEW — Phase 9-11
    "data/auction-crisis.json", "data/eurodollar-stress.json",
    "data/asymmetric-setups.json", "data/cot-extremes.json",
    "data/asymmetric-current.json", "data/cot/extremes.json",
    "data/risk-sized.json",
]


def main():
    with report("probe_data_files") as r:
        r.heading("Probe S3 data files for morning-intel rewrite")
        now = datetime.now(timezone.utc)
        for k in KEYS_TO_CHECK:
            try:
                head = s3.head_object(Bucket=BUCKET, Key=k)
                age_h = (now - head["LastModified"]).total_seconds() / 3600
                size = head["ContentLength"]
                r.log(f"  ✓ {k:50s} {size:>10,}b  age={age_h:5.1f}h")
            except Exception as e:
                code = e.response['Error']['Code'] if hasattr(e, 'response') else type(e).__name__
                r.log(f"  ✗ {k:50s} {code}")


if __name__ == "__main__":
    main()
