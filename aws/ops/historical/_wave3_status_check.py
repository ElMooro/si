"""Check Wave 3 systems status: allocator, momentum, vol, news, research."""
import json
import boto3
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("wave3_status_check") as r:
        r.heading("Wave 3 Lambdas + S3 outputs")

        wave3_lambdas = [
            "justhodl-allocator",
            "justhodl-momentum-scanner",
            "justhodl-wave-signal-logger",
        ]
        for n in wave3_lambdas:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                last = cfg.get("LastModified", "?")
                r.ok(f"  ✓ {n:38s} state={cfg['State']:8s} mod={last}")
            except Exception as e:
                r.log(f"  ✗ {n}: {e}")

        # Schedules
        r.heading("Schedules")
        rules = ["justhodl-allocator-6h", "justhodl-momentum-scanner-daily",
                 "justhodl-wave-signal-logger-6h"]
        for rule in rules:
            try:
                d = events.describe_rule(Name=rule)
                r.ok(f"  ✓ {rule:42s} {d.get('ScheduleExpression', '?'):25s} state={d.get('State')}")
            except Exception as e:
                # try alternate names
                r.log(f"  ✗ {rule}: {e}")

        # Wave 3 S3 outputs
        r.heading("S3 outputs")
        keys = [
            "data/allocator.json",
            "data/momentum-scanner.json",
            "data/sector-rotation.json",
            "data/calibration-snapshot.json",
            "data/alert-history.json",
            "data/flow-data.json",
            "data/vix-curve.json",
            "data/insider-trades.json",
            "data/earnings-tracker.json",
            "data/macro-surprise.json",
            "divergence/current.json",
            "data/correlation-surface.json",
            "data/auction-crisis.json",
            "data/whats-changed.json",
        ]
        for k in keys:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.ok(f"  ✓ {k:42s} {obj['ContentLength']:>10,}b  mod={obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {k}: {str(e)[:80]}")


if __name__ == "__main__":
    main()
