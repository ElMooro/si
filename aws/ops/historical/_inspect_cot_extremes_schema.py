"""Inspect cot/extremes/current.json full schema."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_cot_extremes_schema") as r:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="cot/extremes/current.json")
            d = json.loads(obj["Body"].read())
        except Exception as e:
            r.log(f"  ✗ {e}")
            return
        r.log(f"  top keys: {list(d.keys())}")
        r.log(f"  size: {len(json.dumps(d))} chars")
        for k, v in d.items():
            if isinstance(v, (str, int, float, bool)):
                r.log(f"  {k}: {v}")
        # Find array(s)
        for k, v in d.items():
            if isinstance(v, list):
                r.log(f"  {k} (list, n={len(v)})")
                if v and isinstance(v[0], dict):
                    r.log(f"    [0] keys: {list(v[0].keys())}")
                    for i in range(min(5, len(v))):
                        rec = v[i]
                        r.log(f"    [{i}] {dict((kk, str(vv)[:40]) for kk,vv in rec.items() if not isinstance(vv,(list,dict)))}")


if __name__ == "__main__":
    main()
