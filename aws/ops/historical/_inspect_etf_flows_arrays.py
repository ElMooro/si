"""Inspect etf-flows heavy_inflow/heavy_outflow + by_etf structure for the logger."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_etf_flows_arrays") as r:
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/etf-flows.json")["Body"].read())
        for k in ("heavy_inflow", "heavy_outflow", "rotation_in", "rotation_out", "unusual_vol"):
            v = d.get(k)
            if isinstance(v, list):
                r.log(f"  {k} (len={len(v)}):")
                for item in v[:3]:
                    if isinstance(item, dict):
                        r.log(f"    {json.dumps(item, default=str)[:300]}")
                    else:
                        r.log(f"    {item}")
        # by_etf shape
        be = d.get("by_etf")
        if isinstance(be, list) and be:
            r.log(f"\n  by_etf (len={len(be)}), first 3:")
            for item in be[:3]:
                r.log(f"    {json.dumps(item, default=str)[:300]}")


if __name__ == "__main__":
    main()
