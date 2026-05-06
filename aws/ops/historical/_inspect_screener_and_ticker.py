"""Inspect screener/data.json fundamentals fields + ticker.html scope."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_screener_and_ticker") as r:
        r.heading("screener/data.json — fields per stock")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")["Body"].read())
        r.log(f"  top-level keys: {list(d.keys())[:20]}")
        # stocks list
        for k in ["stocks", "results", "data", "items", "tickers"]:
            v = d.get(k)
            if isinstance(v, list) and v:
                r.log(f"  found list under '{k}' n={len(v)}")
                sample = v[0]
                if isinstance(sample, dict):
                    r.log(f"  fields: {list(sample.keys())}")
                    r.log(f"  sample (truncated): {json.dumps(sample, default=str, indent=2)[:1500]}")
                break
        else:
            # maybe top-level is dict-of-tickers
            for k, v in list(d.items())[:3]:
                r.log(f"  '{k}' (type {type(v).__name__}) preview: {str(v)[:200]}")


if __name__ == "__main__":
    main()
