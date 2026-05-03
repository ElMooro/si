"""Probe FMP earnings calendar to see why n_upcoming=0."""
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def main():
    with report("probe_earnings_fmp") as r:
        r.heading("Probe FMP earnings calendar API")
        today = datetime.now(timezone.utc).date()
        to_date = (today + timedelta(days=14)).isoformat()
        url = (f"https://financialmodelingprep.com/api/v3/earning_calendar"
               f"?from={today.isoformat()}&to={to_date}&apikey={FMP_KEY}")
        r.log(f"  GET {url[:80]}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research raafouis@gmail.com"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            r.log(f"  type: {type(data).__name__}")
            if isinstance(data, list):
                r.log(f"  n_events: {len(data)}")
                if data:
                    r.log(f"  first event: {json.dumps(data[0])[:300]}")
                    r.log(f"  last event: {json.dumps(data[-1])[:300]}")
                    # Filter for our watchlist
                    watchlist_set = {"AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","SPY","QQQ"}
                    matches = [e for e in data if e.get("symbol") in watchlist_set]
                    r.log(f"  matches in test set: {len(matches)}")
                    for m in matches[:5]:
                        r.log(f"    {m.get('symbol')} on {m.get('date')} ({m.get('time')}) EPS:{m.get('epsEstimated')}")
            else:
                r.log(f"  response: {str(data)[:500]}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # Test recent
        r.section("Recent earnings (past 30 days)")
        from_date = (today - timedelta(days=30)).isoformat()
        url2 = (f"https://financialmodelingprep.com/api/v3/earning_calendar"
                f"?from={from_date}&to={today.isoformat()}&apikey={FMP_KEY}")
        try:
            req = urllib.request.Request(url2, headers={"User-Agent": "JustHodl"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data2 = json.loads(resp.read())
            if isinstance(data2, list):
                r.log(f"  n_events: {len(data2)}")
                with_eps = [e for e in data2 if e.get("eps") is not None]
                r.log(f"  with eps_actual: {len(with_eps)}")
                if with_eps:
                    r.log(f"  sample with eps: {json.dumps(with_eps[0])[:400]}")
        except Exception as e:
            r.fail(f"  err: {e}")

        # Check actual S3 output
        r.section("Current S3 output")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-tracker.json")
            output = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {output.get('generated_at')}")
            r.log(f"  n_upcoming: {len(output.get('upcoming_14d') or [])}")
            r.log(f"  n_recent: {len(output.get('recent_results_30d') or [])}")
            r.log(f"  n_pead: {len(output.get('pead_signals') or [])}")
            r.log(f"  duration_s: {output.get('duration_s')}")
        except Exception as e:
            r.log(f"  no S3 output yet: {e}")


if __name__ == "__main__":
    main()
