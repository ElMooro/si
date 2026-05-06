"""Session resume — what's the current state of every recent build?"""
import boto3
import json
from datetime import datetime
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


def main():
    with report("session_resume_state") as r:
        r.heading("Recent Lambdas — last invoke timestamps")
        recent = [
            "justhodl-calibration-snapshot",
            "justhodl-sector-rotation",
            "justhodl-alert-router",
            "justhodl-momentum-scanner",
            "justhodl-wave-signal-logger",
            "justhodl-allocator",
            "justhodl-bond-regime-detector",
            "justhodl-divergence-scanner",
            "justhodl-cot-extremes-scanner",
            "justhodl-asymmetric-scorer",
            "justhodl-risk-sizer",
            "justhodl-auction-crisis-detector",
            "justhodl-eurodollar-stress",
        ]
        for n in recent:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                r.ok(f"  ✓ {n:38s} mem={cfg['MemorySize']}MB  modified={cfg['LastModified'][:19]}")
            except Exception as e:
                r.log(f"  ✗ {n}: doesn't exist")

        r.heading("Recent S3 outputs (modified in last 24h)")
        recent_keys = [
            "data/calibration-snapshot.json",
            "data/sector-rotation.json",
            "data/alert-history.json",
            "data/momentum-scanner.json",
            "data/wave-signals-logged.json",
            "data/allocator-snapshot.json",
            "data/cross-asset-relative-value.json",
            "regime/current.json",
            "divergence/current.json",
            "data/cot-extremes.json",
            "data/asymmetric-scorer.json",
            "data/risk-sizer.json",
            "data/auction-crisis.json",
            "data/eurodollar-stress.json",
        ]
        for key in recent_keys:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                age_h = (datetime.now(obj["LastModified"].tzinfo) - obj["LastModified"]).total_seconds() / 3600
                age_str = f"{age_h:.1f}h ago" if age_h < 48 else f"{age_h/24:.1f}d ago"
                size = obj["ContentLength"]
                r.ok(f"  ✓ {key:48s} {size:>9,}b  {age_str}")
            except Exception:
                r.log(f"  ✗ {key} missing")


if __name__ == "__main__":
    main()
