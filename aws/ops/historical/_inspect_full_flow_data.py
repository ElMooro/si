"""Get full sentiment/internals/trading_signals blocks for vol.html design."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_full_flow_data") as r:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="flow-data.json")
            d = json.loads(obj["Body"].read())
            data = d.get("data", {})
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        for k in ["sentiment", "trading_signals", "market_internals", "fund_flows", "unusual_activity"]:
            r.heading(k)
            v = data.get(k)
            if v is None:
                r.log("  (missing)")
                continue
            r.log(json.dumps(v, default=str, indent=2)[:1500])


if __name__ == "__main__":
    main()
