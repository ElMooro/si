"""Check exact keys on top composite item."""
import json, boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("check_momentum_first_keys") as r:
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        first = d["rankings"]["composite_top_50"][0]
        r.log(f"  all keys on first item: {sorted(first.keys())}")
        for k in sorted(first.keys()):
            r.log(f"    {k}: {first[k]}")


if __name__ == "__main__":
    main()
