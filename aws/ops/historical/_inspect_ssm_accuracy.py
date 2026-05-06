"""Inspect SSM accuracy parameter structure."""
import json
import boto3
from ops_report import report

ssm = boto3.client("ssm", region_name="us-east-1")


def main():
    with report("inspect_ssm_accuracy") as r:
        for name in ["/justhodl/calibration/accuracy", "/justhodl/calibration/weights"]:
            r.heading(f"=== {name} ===")
            try:
                p = ssm.get_parameter(Name=name)["Parameter"]
                d = json.loads(p["Value"])
                r.log(f"  type: {type(d).__name__}, n_keys: {len(d)}")
                for k, v in list(d.items())[:8]:
                    r.log(f"    {k:32s} → {type(v).__name__}: {str(v)[:100]}")
            except Exception as e:
                r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
