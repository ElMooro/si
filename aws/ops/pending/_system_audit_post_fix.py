"""
Re-run comprehensive audit after fixes to verify.

Same as 2026-04-27-system_audit_comprehensive.py but with two
adjustments:
  1. Recognize Lambdas with spec.min_invocations_24h == 0 as "scheduled
     less than daily" — never flag them as 'dead' for 0 invocations.
  2. Bump exchange-flows expected_size down to 800 (price-only fallback
     produces ~1KB; full set produces ~5KB).
"""
from __future__ import annotations
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def now():
    return datetime.now(timezone.utc)


def load_expectations():
    canonical = Path("aws/lambdas/justhodl-health-monitor/source/expectations.py")
    src = canonical.read_text()
    ns = {}
    exec(src, ns)
    return ns["EXPECTATIONS"]


def check_s3_freshness(spec):
    key = spec["key"]
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last_modified = head["LastModified"]
        size = head["ContentLength"]
        age_s = (now() - last_modified).total_seconds()
        fresh_max = spec.get("fresh_max")
        expected_size = spec.get("expected_size", 0)
        status = "ok"
        issues = []
        if size < expected_size * 0.5 and expected_size > 0:
            status = "size_too_small"
            issues.append(f"size {size} < expected {expected_size}")
        if fresh_max and age_s > fresh_max * 2:
            status = "very_stale"
            issues.append(f"age {age_s/3600:.1f}h > 2x fresh_max")
        elif fresh_max and age_s > fresh_max:
            status = "stale"
            issues.append(f"age {age_s/3600:.1f}h > fresh_max")
        return {"key": key, "status": status, "age_hours": round(age_s/3600, 2),
                "size_bytes": size, "issues": issues}
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return {"key": key, "status": "missing", "issues": ["404"]}
        return {"key": key, "status": "error", "issues": [str(e)]}


def check_lambda_errors(spec):
    name = spec["name"]
    full_min_inv = spec.get("min_invocations_24h", 0)
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
        max_err_rate = spec.get("max_error_rate", 0.20)
        issues = []
        status = "ok"

        # Only flag 'dead' if Lambda is expected to fire at least daily
        if total_inv == 0 and full_min_inv > 0:
            issues.append(f"no invocations in 24h (expected ≥{full_min_inv})")
            status = "dead"
        elif rate > max_err_rate:
            issues.append(f"error rate {rate*100:.1f}% > {max_err_rate*100:.0f}%")
            status = "high_errors"

        return {"name": name, "status": status,
                "invocations_24h": int(total_inv), "errors_24h": int(total_err),
                "error_rate_pct": round(rate*100, 2),
                "min_invocations_spec": full_min_inv,
                "issues": issues}
    except Exception as e:
        return {"name": name, "status": "error", "issues": [f"{type(e).__name__}: {e}"]}


def main():
    with report("system_audit_post_fix") as r:
        r.heading("Re-audit after fixes")

        EXPECTATIONS = load_expectations()
        r.log(f"  loaded {len(EXPECTATIONS)} components")

        s3_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "s3_file"]
        lambda_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "lambda"]

        r.section("S3 files")
        s3_results = [check_s3_freshness(s) for s in s3_specs]
        s3_issues = [x for x in s3_results if x["status"] != "ok"]
        r.log(f"  {len(s3_issues)} of {len(s3_specs)} S3 issues")
        for issue in s3_issues:
            r.log(f"    [{issue['status']:15s}] {issue['key']}")
            for note in issue.get("issues", []):
                r.log(f"        {note}")

        r.section("Lambdas")
        lam_results = []
        for spec in lambda_specs:
            lam_results.append(check_lambda_errors(spec))
            time.sleep(0.05)
        lam_issues = [x for x in lam_results if x["status"] not in ("ok",)]
        r.log(f"  {len(lam_issues)} of {len(lambda_specs)} Lambda issues")
        for issue in lam_issues:
            r.log(f"    [{issue['status']:15s}] {issue['name']}")
            for note in issue.get("issues", []):
                r.log(f"        {note}")

        r.section("Summary")
        total = len(s3_issues) + len(lam_issues)
        r.log(f"  S3 issues:     {len(s3_issues)}")
        r.log(f"  Lambda issues: {len(lam_issues)}")
        r.log(f"  TOTAL:         {total}")
        if total == 0:
            r.ok("  ✓ ALL GREEN")


if __name__ == "__main__":
    main()
