"""Find Crisis KB structure."""
import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("probe_crisis_kb") as r:
        r.heading("Find Crisis KB structure")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/crisis-knowledge-base.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  size: {len(json.dumps(data))//1024}KB")
            r.log(f"  top-level keys: {list(data.keys())[:20]}")
            if "frameworks" in data:
                r.log(f"  n frameworks: {len(data['frameworks'])}")
                for k, v in list(data['frameworks'].items())[:5]:
                    r.log(f"    {k}: {len(v) if isinstance(v, (list,dict)) else type(v).__name__}")
            if "rules" in data:
                r.log(f"  n rules: {len(data['rules'])}")
                if data['rules']:
                    sample = data['rules'][0] if isinstance(data['rules'], list) else list(data['rules'].values())[0]
                    r.log(f"  sample rule structure: {json.dumps(sample, default=str)[:300]}")
            r.log(f"  Top-level: {json.dumps({k: type(v).__name__ for k, v in data.items()}, indent=2)[:600]}")
        except Exception as e:
            r.fail(f"  err: {e}")


if __name__ == "__main__":
    main()
