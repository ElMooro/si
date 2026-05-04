"""Show top short interest signals."""
import json
import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/short-interest.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("show_short_signals") as r:
        r.heading("Top short interest signals")
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        data = json.loads(obj["Body"].read())
        r.log(f"  n_finra: {data.get('n_tickers_finra')}")
        r.log(f"  n_polygon: {data.get('n_tickers_polygon')}")

        r.section("🚨 Top SQUEEZE RISK (high days-to-cover + falling short volume)")
        for x in (data.get("top_squeeze_risk") or []):
            r.log(f"  {x['ticker']:7s} dtc={x.get('days_to_cover'):>5} short_pct={x.get('latest_short_pct'):>5}% trend={x.get('trend_pct'):>+6}% si_chg={x.get('si_change_pct')}")

        r.section("📈 Top CROWDED SHORT / DISTRIBUTION (rising short volume)")
        for x in (data.get("top_crowded_shorts") or [])[:15]:
            r.log(f"  {x['ticker']:7s} short_pct={x.get('latest_short_pct'):>5}% trend={x.get('trend_pct'):>+6}% signal={x.get('signal')}")

        r.section("⏰ Top HIGH DAYS-TO-COVER")
        for x in (data.get("top_high_dtc") or [])[:10]:
            r.log(f"  {x['ticker']:7s} dtc={x.get('days_to_cover'):>5} short_pct={x.get('latest_short_pct')}%")

        r.section("📉 Top COVERING (shorts unwinding)")
        for x in (data.get("top_covering") or [])[:10]:
            r.log(f"  {x['ticker']:7s} si_chg={x.get('si_change_pct')}% trend={x.get('trend_pct')}%")


if __name__ == "__main__":
    main()
