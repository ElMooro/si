"""Probe the real outputs of asymmetric-scorer and risk-sizer at their actual S3 paths."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("probe_real_output_schemas") as r:
        for key in ["opportunities/asymmetric-equity.json", "risk/recommendations.json",
                    "data/asymmetric-scorer.json", "data/risk-sizer.json"]:
            r.heading(f"=== {key} ===")
            try:
                head = s3.head_object(Bucket=BUCKET, Key=key)
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                d = json.loads(obj["Body"].read())
                r.log(f"  size: {head['ContentLength']:,}b   modified: {head['LastModified'].isoformat()}")
                r.log(f"  top keys: {list(d.keys())}")
                r.log("")
                for k in list(d.keys())[:30]:
                    v = d.get(k)
                    if isinstance(v, (str, int, float, bool)) or v is None:
                        r.log(f"    {k:35s} = {str(v)[:80]}")
                    elif isinstance(v, list):
                        r.log(f"    {k:35s} = list (n={len(v)})")
                        if v and isinstance(v[0], dict):
                            r.log(f"      [0] keys: {list(v[0].keys())}")
                            r.log(f"      [0] sample: { {kk: str(vv)[:50] for kk,vv in v[0].items() if not isinstance(vv,(list,dict))} }")
                    elif isinstance(v, dict):
                        r.log(f"    {k:35s} = dict (keys: {list(v.keys())[:10]})")
                        for kk, vv in list(v.items())[:6]:
                            if isinstance(vv, (str, int, float, bool)):
                                r.log(f"      .{kk:30s} = {str(vv)[:50]}")
            except Exception as e:
                r.log(f"  ✗ {e}")
            r.log("")


if __name__ == "__main__":
    main()
