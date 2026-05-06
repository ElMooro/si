"""Inspect existing flow-data.json + screener data shape."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_flow_data") as r:
        r.heading("flow-data.json contents")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="flow-data.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top-level keys: {list(d.keys())[:15]}")
            for k in list(d.keys())[:10]:
                v = d[k]
                if isinstance(v, dict):
                    r.log(f"  {k}: {list(v.keys())[:8]}")
                elif isinstance(v, list):
                    r.log(f"  {k}: list len={len(v)}, sample[0]={str(v[0])[:120] if v else 'empty'}")
                else:
                    r.log(f"  {k}: {str(v)[:80]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("screener/data.json sample")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
            d = json.loads(obj["Body"].read())
            stocks = d.get("stocks", []) or d.get("results", []) or d
            if isinstance(stocks, list) and stocks:
                first = stocks[0]
                r.log(f"  total: {len(stocks)} stocks")
                r.log(f"  full first sample:")
                for k, v in first.items():
                    r.log(f"    {k}: {str(v)[:100]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("insider-trades.json shape")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-trades.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top-level keys: {list(d.keys())[:10]}")
            trades = d.get("trades", []) or d.get("recent_buys", []) or d.get("recent_sells", []) or []
            for k, v in d.items():
                if isinstance(v, list) and v:
                    r.log(f"  {k}: list len={len(v)}")
                    r.log(f"    sample keys: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0])}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
