"""Debug the still-zero signal types: momentum_top_pick + earnings_pead."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("debug_remaining_zeros") as r:
        r.heading("momentum-scanner top_50_composite shape")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        top = d.get("top_50_composite", [])
        r.log(f"  top_50_composite type: {type(top).__name__} len={len(top)}")
        if top:
            r.log(f"  first 3 items keys: {list(top[0].keys()) if isinstance(top[0], dict) else 'not-dict'}")
            for item in top[:5]:
                if isinstance(item, dict):
                    r.log(f"    {json.dumps(item, default=str)[:300]}")

        r.heading("earnings-tracker pead_signals item shape")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-tracker.json")["Body"].read())
        peads = d.get("pead_signals", [])
        r.log(f"  pead_signals len={len(peads)}")
        for p in peads[:5]:
            r.log(f"    keys: {list(p.keys())[:15] if isinstance(p, dict) else 'not-dict'}")
            r.log(f"    {json.dumps(p, default=str)[:400]}")


if __name__ == "__main__":
    main()
