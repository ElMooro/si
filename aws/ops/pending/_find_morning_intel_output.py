"""Find morning-intelligence's actual output location."""
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("find_morning_intel_output") as r:
        r.heading("Bucket roots")
        # List top-level prefixes
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Delimiter="/")
        for p in resp.get("CommonPrefixes", []):
            r.log(f"  prefix: {p['Prefix']}")
        for o in resp.get("Contents", []):
            r.log(f"  object: {o['Key']}  ({o['Size']:,}b)")

        r.heading("Look for any 'morning' or 'brief' or 'intelligence' or 'narrative' keys")
        paginator = s3.get_paginator("list_objects_v2")
        for prefix in ["morning", "brief", "intelligence", "narrative", "report", "ai", "claude"]:
            for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Size"] > 100:
                        r.log(f"  /{prefix} → {obj['Key']}  {obj['Size']:,}b  {obj['LastModified'].isoformat()}")


if __name__ == "__main__":
    main()
