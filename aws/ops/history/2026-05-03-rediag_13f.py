"""Re-diagnose: after the namespace fix, why are 11 still failing?"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("rediagnose_13f") as r:
        r.heading("Re-diagnose 13F failures after namespace fix")
        try:
            data = json.loads(s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")["Body"].read())
        except Exception as e:
            r.fail(f"  read: {e}")
            return
        r.log(f"  funds_parsed: {data.get('funds_parsed')}")
        r.log(f"  funds_failed: {data.get('funds_failed')}")
        r.log(f"  funds OK: {sorted(data.get('by_fund', {}).keys())}")
        r.log(f"  fund errors:")
        for e in data.get("fund_errors", []):
            r.log(f"    {e.get('fund_key', '?')}: {e.get('error', '?')}")

        # Position counts per parsed fund
        r.section("Position counts per parsed fund")
        for fk, f in data.get("by_fund", {}).items():
            r.log(f"  {fk}: {f.get('n_positions', 0)} positions, "
                  f"AUM ${f.get('total_value_usd', 0)/1e9:.1f}B")


if __name__ == "__main__":
    main()
