"""Re-smoke after FINRA float-parse fix."""
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
    with report("resmoke_short_interest") as r:
        r.heading("Re-smoke after float-parse fix")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']}")
            r.log(f"  duration: {time.time()-t0:.1f}s")
            r.log(f"  response: {payload[:400]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        try:
            obj = s3.get_object(Bucket=BUCKET, Key=KEY)
            data = json.loads(obj["Body"].read())
            r.log(f"  n_finra: {data.get('n_tickers_finra')}")
            r.log(f"  n_polygon: {data.get('n_tickers_polygon')}")
            r.log(f"  n_squeeze_risk: {len(data.get('top_squeeze_risk') or [])}")
            r.log(f"  n_crowded: {len(data.get('top_crowded_shorts') or [])}")
            r.log(f"  n_covering: {len(data.get('top_covering') or [])}")

            r.section("Top crowded shorts (rising trend)")
            for x in (data.get("top_crowded_shorts") or [])[:8]:
                r.log(f"    {x['ticker']:7s} short_pct={x.get('latest_short_pct')} trend={x.get('trend_pct')}% signal={x.get('signal')}")

            r.section("Top squeeze risk")
            for x in (data.get("top_squeeze_risk") or [])[:8]:
                r.log(f"    {x['ticker']:7s} dtc={x.get('days_to_cover')} trend={x.get('trend_pct')}% signal={x.get('signal')}")

            r.section("Top high days-to-cover")
            for x in (data.get("top_high_dtc") or [])[:5]:
                r.log(f"    {x['ticker']:7s} dtc={x.get('days_to_cover')} short_pct={x.get('latest_short_pct')}")

            r.section("Top covering")
            for x in (data.get("top_covering") or [])[:5]:
                r.log(f"    {x['ticker']:7s} si_chg={x.get('si_change_pct')}% trend={x.get('trend_pct')}%")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
