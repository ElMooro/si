"""Drill into flow-data.json's skew/put_call/vix_complex blocks."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_skew_block") as r:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="flow-data.json")
            d = json.loads(obj["Body"].read())
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        for k in ["vix_complex", "skew", "put_call", "gamma_exposure", "sentiment", "trading_signals", "market_internals"]:
            r.heading(k)
            v = d.get("data", {}).get(k)
            if v is None:
                r.log("  (missing)")
                continue
            r.log(json.dumps(v, default=str, indent=2)[:1200])


if __name__ == "__main__":
    main()
