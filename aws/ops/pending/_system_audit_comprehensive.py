"""
Comprehensive audit of what's not working right now.

Checks:
  1. Health monitor's current dashboard.json — what does it report?
  2. Every S3 data file in expectations.py — when was it last modified?
  3. Every Lambda in expectations.py — error rate over last 24h
  4. List Lambdas that haven't been invoked in their expected interval
  5. List S3 files older than 2× their fresh_max
  6. Look for HTML pages that reference S3 paths that don't exist

Output: aws/ops/reports/latest/system_audit_*.md (verbose) +
        aws/ops/audit/system_issues_*.json (structured)
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
    """Load expectations dict from canonical file."""
    canonical = Path("aws/lambdas/justhodl-health-monitor/source/expectations.py")
    src = canonical.read_text()
    ns = {}
    exec(src, ns)
    return ns["EXPECTATIONS"]


def check_s3_freshness(spec):
    """How stale is a single S3 file?"""
    key = spec["key"]
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last_modified = head["LastModified"]
        size = head["ContentLength"]
        age_s = (now() - last_modified).total_seconds()
        fresh_max = spec.get("fresh_max")
        warn_max = spec.get("warn_max")
        expected_size = spec.get("expected_size", 0)

        status = "ok"
        issues = []
        if size < expected_size * 0.5 and expected_size > 0:
            status = "size_too_small"
            issues.append(f"size {size} < expected {expected_size}")
        if fresh_max and age_s > fresh_max * 2:
            status = "very_stale"
            issues.append(f"age {age_s/3600:.1f}h > 2x fresh_max ({fresh_max/3600:.1f}h)")
        elif fresh_max and age_s > fresh_max:
            status = "stale"
            issues.append(f"age {age_s/3600:.1f}h > fresh_max ({fresh_max/3600:.1f}h)")
        return {
            "key": key,
            "status": status,
            "age_hours": round(age_s / 3600, 2),
            "size_bytes": size,
            "issues": issues,
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return {"key": key, "status": "missing", "issues": ["404 not found"]}
        return {"key": key, "status": "error", "issues": [str(e)]}


def check_lambda_errors(spec):
    """24h error rate for a Lambda."""
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
        durs = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average", "Maximum"],
        )
        total_inv = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        total_err = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        avg_dur = (sum(p.get("Average", 0) for p in durs.get("Datapoints", []))
                   / max(len(durs.get("Datapoints", [])), 1))
        max_dur = max((p.get("Maximum", 0) for p in durs.get("Datapoints", [])), default=0)

        # Get last invocation timestamp
        last_invocation = None
        if inv.get("Datapoints"):
            datapoints = sorted(inv["Datapoints"], key=lambda p: p["Timestamp"])
            for p in reversed(datapoints):
                if p.get("Sum", 0) > 0:
                    last_invocation = p["Timestamp"]
                    break

        max_err_rate = spec.get("max_error_rate", 0.20)
        rate = total_err / max(total_inv, 1)

        # Get Lambda LastModified
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            last_modified = cfg.get("LastModified", "")
        except Exception:
            last_modified = "unknown"

        issues = []
        status = "ok"
        if total_inv == 0:
            issues.append("no invocations in 24h")
            status = "dead"
        elif rate > max_err_rate:
            issues.append(f"error rate {rate*100:.1f}% > {max_err_rate*100:.0f}%")
            status = "high_errors"
        elif rate > max_err_rate * 0.5:
            issues.append(f"error rate {rate*100:.1f}% elevated")
            status = "elevated_errors"
        if max_dur > 25_000:
            issues.append(f"max duration {max_dur/1000:.1f}s — near timeout?")
        return {
            "name": name,
            "status": status,
            "invocations_24h": int(total_inv),
            "errors_24h": int(total_err),
            "error_rate_pct": round(rate * 100, 2),
            "avg_duration_ms": round(avg_dur, 0),
            "max_duration_ms": round(max_dur, 0),
            "last_invocation": last_invocation.isoformat() if last_invocation else None,
            "last_modified": last_modified,
            "issues": issues,
        }
    except Exception as e:
        return {"name": name, "status": "error", "issues": [f"{type(e).__name__}: {e}"]}


def check_html_pages(audit_results):
    """Scan root HTML files for references to S3 data files that don't exist."""
    issues = []
    s3_data_paths = []
    # Build a set of S3 keys we know exist
    existing_keys = set()
    for r in audit_results.get("s3", []):
        if r.get("status") not in ("missing", "error"):
            existing_keys.add(r["key"])

    html_files = sorted(Path(".").glob("*.html"))
    for html in html_files:
        try:
            src = html.read_text(errors="ignore")
        except Exception:
            continue
        # Extract S3 references
        import re
        # Look for fetch('/data/...') or similar
        matches = re.findall(r"['\"`]/?(data/[a-zA-Z0-9_\-/]+\.json)['\"`]", src)
        matches += re.findall(r"['\"`]/?(opportunities/[a-zA-Z0-9_\-/]+\.json)['\"`]", src)
        for m in set(matches):
            if m not in existing_keys:
                # Check if it actually exists in S3 (might not be in expectations)
                try:
                    s3.head_object(Bucket=BUCKET, Key=m)
                    # Exists but not tracked in expectations — info only
                    pass
                except ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        issues.append({"page": html.name, "missing_key": m})
    return issues


def main():
    with report("system_audit_comprehensive") as r:
        r.heading("Comprehensive system audit")

        r.section("1. Load expectations")
        try:
            EXPECTATIONS = load_expectations()
            r.log(f"  loaded {len(EXPECTATIONS)} components")
        except Exception as e:
            r.fail(f"  failed to load: {e}")
            return

        s3_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "s3_file"]
        lambda_specs = [s for s in EXPECTATIONS.values() if s.get("type") == "lambda"]
        r.log(f"    {len(s3_specs)} S3 specs, {len(lambda_specs)} Lambda specs")

        r.section("2. S3 file freshness")
        s3_results = []
        for spec in s3_specs:
            res = check_s3_freshness(spec)
            s3_results.append(res)

        # Sort by status severity
        order = {"missing": 0, "very_stale": 1, "size_too_small": 2,
                 "stale": 3, "error": 4, "ok": 5}
        s3_results.sort(key=lambda x: order.get(x.get("status"), 99))

        s3_issues = [r for r in s3_results if r["status"] != "ok"]
        r.log(f"  S3 issues: {len(s3_issues)} of {len(s3_results)}")
        for issue in s3_issues:
            r.log(f"    [{issue['status']:15s}] {issue['key']}")
            for note in issue.get("issues", []):
                r.log(f"        {note}")

        r.section("3. Lambda errors and idleness")
        lam_results = []
        for spec in lambda_specs:
            res = check_lambda_errors(spec)
            lam_results.append(res)
            time.sleep(0.05)   # gentle on CW

        lam_issues = [r for r in lam_results if r["status"] not in ("ok",)]
        r.log(f"  Lambda issues: {len(lam_issues)} of {len(lam_results)}")
        for issue in lam_issues:
            r.log(f"    [{issue['status']:18s}] {issue['name']}")
            for note in issue.get("issues", []):
                r.log(f"        {note}")
            r.log(f"        24h: {issue.get('invocations_24h', 0)} inv | "
                  f"{issue.get('errors_24h', 0)} err | "
                  f"avg {issue.get('avg_duration_ms', 0):.0f}ms | "
                  f"max {issue.get('max_duration_ms', 0):.0f}ms")

        r.section("4. HTML page broken references")
        page_issues = check_html_pages({"s3": s3_results})
        if page_issues:
            r.log(f"  {len(page_issues)} broken S3 references in HTML")
            for issue in page_issues[:30]:
                r.log(f"    {issue['page']}  →  {issue['missing_key']}")
        else:
            r.log(f"  no broken S3 refs detected")

        r.section("5. Summary")
        r.log(f"  S3 files:        {len(s3_specs)} tracked, {len(s3_issues)} issues")
        r.log(f"  Lambdas:         {len(lambda_specs)} tracked, {len(lam_issues)} issues")
        r.log(f"  HTML refs broken: {len(page_issues)}")
        r.log(f"  TOTAL ISSUES:    {len(s3_issues) + len(lam_issues) + len(page_issues)}")

        # Write structured artifact
        Path("aws/ops/audit").mkdir(parents=True, exist_ok=True)
        ts = now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(f"aws/ops/audit/system_issues_{ts}.json")
        out_path.write_text(json.dumps({
            "generated_at": now().isoformat(),
            "s3_results": s3_results,
            "lambda_results": lam_results,
            "html_broken_refs": page_issues,
            "summary": {
                "s3_total": len(s3_specs),
                "s3_with_issues": len(s3_issues),
                "lambda_total": len(lambda_specs),
                "lambda_with_issues": len(lam_issues),
                "html_broken_refs": len(page_issues),
            },
        }, indent=2, default=str))
        r.log(f"\n  ✓ structured artifact: {out_path}")


if __name__ == "__main__":
    main()
