"""Check momentum scanner output."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("check_momentum_today") as r:
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        r.heading("Momentum scanner state")
        r.log(f"  generated_at: {d.get('generated_at')}")
        r.log(f"  keys: {list(d.keys())[:15]}")
        for k in ["top_50_composite", "top_composite", "ranked", "top_picks"]:
            v = d.get(k)
            if v is not None:
                r.log(f"  {k}: type={type(v).__name__} len={len(v) if hasattr(v,'__len__') else '?'}")
                if isinstance(v, list) and v:
                    r.log(f"    first item keys: {list(v[0].keys())[:10] if isinstance(v[0], dict) else '?'}")
                    r.log(f"    first item: {str(v[0])[:300]}")


if __name__ == "__main__":
    main()
