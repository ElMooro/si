"""Find the S&P 500 ticker list location."""
import boto3, json
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("find_sp500_list") as r:
        keys_to_check = [
            "screener/data.json",
            "data/sp500-tickers.json",
            "data/sp500.json",
            "screener/sp500.json",
        ]
        for k in keys_to_check:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.ok(f"  ✓ {k}  size={obj['ContentLength']:,}b")
            except Exception:
                r.log(f"  ✗ {k}  not found")
        # Look in screener/data.json for the universe
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
            d = json.loads(obj["Body"].read())
            r.heading("screener/data.json shape")
            if isinstance(d, dict):
                r.log(f"  keys: {list(d.keys())[:15]}")
                stocks = d.get("stocks") or d.get("results") or d.get("data") or []
                r.log(f"  stock list len: {len(stocks)}")
                if stocks and isinstance(stocks[0], dict):
                    r.log(f"  first stock keys: {list(stocks[0].keys())[:15]}")
                    r.log(f"  first stock: {json.dumps(stocks[0], default=str)[:300]}")
            elif isinstance(d, list):
                r.log(f"  list len: {len(d)}")
                if d: r.log(f"  first: {json.dumps(d[0], default=str)[:300]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
