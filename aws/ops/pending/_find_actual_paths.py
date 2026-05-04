"""Find actual S3 paths for divergence, COT, eurodollar data."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

BUCKET = "justhodl-dashboard-live"


def main():
    with report("find_actual_paths") as r:
        # 1. Divergence schema
        r.heading("divergence/current.json full content")
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key="divergence/current.json")["Body"].read())
            r.log(json.dumps(d, default=str, indent=2)[:3000])
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 2. List all 'cot' or 'extreme' related S3 keys
        r.heading("S3 keys matching 'cot' or 'extreme'")
        for prefix in ["cot/", "data/cot", "data/extreme"]:
            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
                for obj in resp.get("Contents", []):
                    r.log(f"  {obj['Key']:50s} {obj['Size']:>10,}b  mod={obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  prefix={prefix}: {e}")

        # 3. List eurodollar
        r.heading("S3 keys matching 'eurodollar'")
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/eurodollar", MaxKeys=20)
            for obj in resp.get("Contents", []):
                r.log(f"  {obj['Key']:50s} {obj['Size']:>10,}b  mod={obj['LastModified'].isoformat()}")
        except Exception as e:
            r.log(f"  {e}")

        # 4. cot-extremes-scanner Lambda — look at where it writes
        r.heading("justhodl-cot-extremes-scanner code excerpt")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-cot-extremes-scanner")
            r.log(f"  state={cfg['State']} mod={cfg['LastModified']}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. eurodollar-stress Lambda
        r.heading("justhodl-eurodollar-stress Lambda")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-eurodollar-stress")
            r.log(f"  state={cfg['State']} mod={cfg['LastModified']}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
