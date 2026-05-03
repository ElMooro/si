"""Find actual S3 paths for output files of various Lambdas."""
import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("probe_data_paths") as r:
        r.heading("Find correct S3 paths for missing data")

        # List a few directories
        for prefix in ["", "data/", "asymmetric/", "cot/", "risk/", "stress/", "redflags/", "regime/", "divergence/"]:
            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20, Delimiter="/")
                files = [o["Key"] for o in resp.get("Contents", []) if not o["Key"].endswith("/")]
                if files:
                    r.log(f"\n  Prefix '{prefix}':")
                    for f in files[:15]:
                        r.log(f"    {f}")
            except Exception as e:
                r.log(f"  {prefix} err: {e}")

        # Also look at top-level files matching known names
        r.section("Top-level files matching common names")
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=100, Delimiter="/")
            files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
            for f in sorted(files):
                r.log(f"    {f}")
        except Exception as e:
            r.log(f"  err: {e}")


if __name__ == "__main__":
    main()
