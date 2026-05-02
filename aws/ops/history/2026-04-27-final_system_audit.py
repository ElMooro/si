"""Final verification — what's the system status post-all-fixes."""
from pathlib import Path
import sys

sys.path.insert(0, "aws/ops")
from ops_report import report

import boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def now():
    return datetime.now(timezone.utc)


def load_expectations():
    src = Path("aws/lambdas/justhodl-health-monitor/source/expectations.py").read_text()
    ns = {}
    exec(src, ns)
    return ns["EXPECTATIONS"]


def main():
    with report("final_system_audit") as r:
        r.heading("Final system audit — post all fixes")

        EXPECTATIONS = load_expectations()
        s3_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "s3_file"]
        lambda_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "lambda"]

        r.section("S3 freshness")
        s3_issues = 0
        for spec in s3_specs:
            try:
                head = s3.head_object(Bucket=BUCKET, Key=spec["key"])
                size = head["ContentLength"]
                last_mod = head["LastModified"]
                age_h = (now() - last_mod).total_seconds() / 3600
                exp_size = spec.get("expected_size", 0)
                fresh_max = spec.get("fresh_max") or 86400
                fresh_max_h = fresh_max / 3600

                problem = None
                if size < exp_size * 0.5 and exp_size > 0:
                    problem = f"size {size} < {exp_size//2} (expected {exp_size})"
                elif age_h > fresh_max_h * 2:
                    problem = f"age {age_h:.1f}h > 2x fresh_max {fresh_max_h:.1f}h"
                if problem:
                    s3_issues += 1
                    r.log(f"  ✗ {spec['key']}: {problem}")
            except ClientError as e:
                s3_issues += 1
                if e.response["Error"]["Code"] == "404":
                    r.log(f"  ✗ {spec['key']}: 404 missing")
                else:
                    r.log(f"  ✗ {spec['key']}: {e}")
        r.log(f"  S3 issues: {s3_issues} of {len(s3_specs)}")

        r.section("Lambda errors")
        lam_issues = 0
        for spec in lambda_specs:
            name = spec["name"]
            try:
                end = now()
                start = end - timedelta(hours=24)
                inv = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Invocations",
                    Dimensions=[{"Name": "FunctionName", "Value": name}],
                    StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
                )
                err = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Errors",
                    Dimensions=[{"Name": "FunctionName", "Value": name}],
                    StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
                )
                total_inv = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
                total_err = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
                rate = total_err / max(total_inv, 1)
                max_err = spec.get("max_error_rate", 0.20)
                full_min = spec.get("min_invocations_24h", 0)
                if total_inv == 0 and full_min > 0:
                    lam_issues += 1
                    r.log(f"  ✗ {name}: 0 invocations / 24h (min ≥{full_min})")
                elif rate > max_err:
                    lam_issues += 1
                    r.log(f"  ✗ {name}: error rate {rate*100:.1f}% > {max_err*100:.0f}%")
            except Exception as e:
                lam_issues += 1
                r.log(f"  ✗ {name}: {type(e).__name__}: {e}")
        r.log(f"  Lambda issues: {lam_issues} of {len(lambda_specs)}")

        r.section("Final tally")
        total = s3_issues + lam_issues
        r.log(f"  S3:     {s3_issues} issues / {len(s3_specs)}")
        r.log(f"  Lambda: {lam_issues} issues / {len(lambda_specs)}")
        r.log(f"  TOTAL:  {total}")
        if total == 0:
            r.ok("  ✅ ALL GREEN")


if __name__ == "__main__":
    main()
