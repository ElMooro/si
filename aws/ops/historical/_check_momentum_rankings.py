"""Inspect rankings shape."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("check_momentum_rankings") as r:
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")["Body"].read())
        rankings = d.get("rankings", {})
        if isinstance(rankings, dict):
            r.log(f"  rankings keys: {list(rankings.keys())[:15]}")
            for view_key, view_val in rankings.items():
                if isinstance(view_val, list):
                    r.log(f"  {view_key}: list[{len(view_val)}]")
                    if view_val:
                        r.log(f"    [0] = {str(view_val[0])[:300]}")
                else:
                    r.log(f"  {view_key}: {type(view_val).__name__}")
        elif isinstance(rankings, list):
            r.log(f"  rankings: list[{len(rankings)}]")
            for x in rankings[:3]:
                r.log(f"    {str(x)[:200]}")
        # also summary
        r.log(f"  summary: {json.dumps(d.get('summary'), default=str)[:500]}")


if __name__ == "__main__":
    main()
