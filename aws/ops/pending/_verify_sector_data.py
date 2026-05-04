"""Verify sector-rotation actually has return data."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("verify_sector_data") as r:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sector-rotation.json")
        d = json.loads(obj["Body"].read())
        r.heading("Sector returns dump")
        r.log(f"  spy_returns: {d.get('spy_returns')}")
        r.log(f"  market_breadth: {d.get('market_breadth')}")
        for s in d.get("sectors", []):
            r.log(f"  {s['ticker']} {s['name']}")
            r.log(f"    returns: {s.get('returns')}")
            r.log(f"    rs_vs_spy: {s.get('rs_vs_spy')}")
            r.log(f"    regime: {s.get('regime')}")
            r.log(f"    momentum_quintile: {s.get('momentum_quintile')}")


if __name__ == "__main__":
    main()
