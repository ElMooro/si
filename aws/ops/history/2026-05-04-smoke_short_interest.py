"""Smoke test the freshly-created justhodl-short-interest Lambda."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/short-interest.json"
LAMBDA_NAME = "justhodl-short-interest"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("smoke_short_interest") as r:
        r.heading("Smoke test justhodl-short-interest")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']}")
            r.log(f"  duration: {time.time()-t0:.1f}s")
            r.log(f"  response: {payload[:400]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ function error: {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=KEY)
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            r.log(f"  duration_s: {data.get('duration_s')}")
            r.log(f"  n_tickers_with_data: {data.get('n_tickers_with_data')}")
            r.log(f"  n_tickers_finra: {data.get('n_tickers_finra')}")
            r.log(f"  n_tickers_polygon: {data.get('n_tickers_polygon')}")

            r.section("Top crowded shorts")
            for x in (data.get("top_crowded_shorts") or [])[:8]:
                r.log(f"    {x['ticker']:7s} short_pct={x.get('latest_short_pct'):.1f}% trend={x.get('trend_pct')}% signal={x.get('signal')}")

            r.section("Top squeeze risk")
            for x in (data.get("top_squeeze_risk") or [])[:5]:
                r.log(f"    {x['ticker']:7s} dtc={x.get('days_to_cover')} trend={x.get('trend_pct')}% si_chg={x.get('si_change_pct')}%")

            r.section("Top high days-to-cover")
            for x in (data.get("top_high_dtc") or [])[:5]:
                r.log(f"    {x['ticker']:7s} dtc={x.get('days_to_cover')} si={x.get('short_interest')}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
