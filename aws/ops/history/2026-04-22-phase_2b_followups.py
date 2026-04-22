#!/usr/bin/env python3
"""
Three parallel follow-ups:

A. DELETE the 2 truly-dead Lambdas:
     - nyfed-cmdi-fetcher     (1 inv / 90d, no callers, no schedule)
     - nyfed-main-aggregator  (1 inv / 90d, no callers, no schedule)
   Also deletes their CloudWatch log groups.

B. INVESTIGATE enhanced-openbb-handler + its 2 warmers:
     - Read handler code: what does it do? S3 writes? downstream pings?
     - Read each warmer's code + schedule: are they pure warmers, or do
       they actually call the handler for real work?
     - Estimate monthly cost based on avg duration × invocation rate

C. INSPECT justhodl-email-reports-v2 vs justhodl-daily-report-v3:
     - Read v2 code
     - Compare to daily-report-v3 email logic
     - Check what SES sender + subject + content each produces
     - Recommend whether v2 is redundant

Actions are independent. All findings reported in one structured doc.
"""

import io
import json
import os
import re
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

lam  = boto3.client("lambda", region_name=REGION)
cw   = boto3.client("cloudwatch", region_name=REGION)
ev   = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def download_code(fn_name: str) -> dict:
    """Returns {filename: source_text} for every .py/.js file in the zip."""
    try:
        code_url = lam.get_function(FunctionName=fn_name)["Code"]["Location"]
    except ClientError:
        return {}
    try:
        with urllib.request.urlopen(code_url, timeout=20) as resp:
            zbytes = resp.read()
    except Exception:
        return {}
    files = {}
    try:
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            for entry in zf.namelist():
                if entry.endswith((".py", ".js", ".mjs")) and "/site-packages/" not in entry and "/dist-info/" not in entry:
                    try:
                        files[entry] = zf.read(entry).decode("utf-8", errors="ignore")
                    except Exception:
                        pass
    except zipfile.BadZipFile:
        pass
    return files


def get_avg_duration_ms(fn_name: str, days=30) -> float:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Average"],
        )
        points = resp.get("Datapoints", [])
        if not points:
            return 0.0
        return sum(p["Average"] for p in points) / len(points)
    except ClientError:
        return 0.0


def get_invocations_30d(fn_name: str) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Sum"],
        )
        return int(sum(p["Sum"] for p in resp.get("Datapoints", [])))
    except ClientError:
        return 0


with report("phase_2b_followups") as r:
    r.heading("Phase 2b follow-ups: deletes + investigation + email dedup")

    # ─────────────────────────────────────────────────
    # A. DELETE nyfed-cmdi-fetcher and nyfed-main-aggregator
    # ─────────────────────────────────────────────────
    r.section("A. Delete 2 truly-dead Lambdas")

    for fn_name in ["nyfed-cmdi-fetcher", "nyfed-main-aggregator"]:
        try:
            lam.delete_function(FunctionName=fn_name)
            r.ok(f"  Lambda {fn_name} deleted")
            deleted = True
        except lam.exceptions.ResourceNotFoundException:
            r.warn(f"  Lambda {fn_name} not found (already deleted?)")
            deleted = False
        except ClientError as e:
            r.fail(f"  Lambda {fn_name} delete failed: {e}")
            deleted = False

        # Delete log group too
        log_group = f"/aws/lambda/{fn_name}"
        try:
            logs.delete_log_group(logGroupName=log_group)
            r.log(f"    Log group {log_group} deleted")
        except logs.exceptions.ResourceNotFoundException:
            r.log(f"    Log group {log_group} didn't exist")
        except ClientError as e:
            r.warn(f"    Log group delete warning: {e}")

        r.kv(action="delete", target=fn_name, status="deleted" if deleted else "not-found")

    # ─────────────────────────────────────────────────
    # B. INVESTIGATE enhanced-openbb-handler + warmers
    # ─────────────────────────────────────────────────
    r.section("B. Investigate enhanced-openbb-handler + its warmers")

    # Find warmer Lambdas
    handler_code = download_code("enhanced-openbb-handler")
    r.log(f"  enhanced-openbb-handler source files: {len(handler_code)}")

    # Show what the handler does (first 30 lines of main file)
    if handler_code:
        main_file = sorted(handler_code.keys(), key=lambda f: (len(handler_code[f]), f))[-1]
        r.log(f"  Main file: {main_file} ({len(handler_code[main_file])} bytes)")
        r.log(f"  First 30 lines of {main_file}:")
        for line in handler_code[main_file].splitlines()[:30]:
            r.log(f"    {line[:180]}")

        # Detect signals: does it write to S3? invoke other Lambdas? return data?
        full_code = "\n".join(handler_code.values())
        signals = {
            "writes_s3":       bool(re.search(r"put_object|s3\.upload", full_code)),
            "reads_s3":        bool(re.search(r"get_object|s3\.download", full_code)),
            "invokes_other":   bool(re.search(r"\.invoke\s*\(", full_code)),
            "http_responses":  bool(re.search(r"statusCode\s*:\s*200", full_code)),
            "openbb_calls":    bool(re.search(r"openbb", full_code, re.I)),
            "polygon_calls":   bool(re.search(r"polygon", full_code, re.I)),
            "writes_dynamo":   bool(re.search(r"put_item|dynamodb", full_code, re.I)),
        }
        r.log(f"  Handler behavior:")
        for k, v in signals.items():
            r.log(f"    {k}: {v}")

    # Inspect the warmers
    r.log("")
    r.log("  Warmer rules:")
    for rule_name in ["lambda-warmer-system3", "lambda-warmer-system3-frequent"]:
        try:
            rule = ev.describe_rule(Name=rule_name)
            r.log(f"    {rule_name}: State={rule.get('State')} Schedule={rule.get('ScheduleExpression','—')}")
            # Targets
            targets = ev.list_targets_by_rule(Rule=rule_name).get("Targets", [])
            for t in targets:
                tgt_arn = t.get("Arn", "")
                tgt_name = tgt_arn.rsplit(":", 1)[-1]
                input_payload = t.get("Input", "")
                r.log(f"      → target: {tgt_name}")
                if input_payload:
                    r.log(f"        payload: {input_payload[:200]}")
        except ClientError as e:
            r.warn(f"    {rule_name}: {e}")

    # Cost estimate
    dur = get_avg_duration_ms("enhanced-openbb-handler")
    inv_30d = get_invocations_30d("enhanced-openbb-handler")
    # Lambda pricing: $0.0000166667 per GB-second. Function memory unknown — try 256MB default.
    try:
        cfg = lam.get_function_configuration(FunctionName="enhanced-openbb-handler")
        memory_mb = cfg.get("MemorySize", 256)
    except ClientError:
        memory_mb = 256
    gb_sec_per_inv = (dur / 1000) * (memory_mb / 1024)
    compute_cost_30d = inv_30d * gb_sec_per_inv * 0.0000166667
    request_cost_30d = inv_30d * 0.0000002  # $0.20 per 1M requests
    total_cost_30d = compute_cost_30d + request_cost_30d

    r.log("")
    r.log(f"  Cost estimate (last 30 days):")
    r.log(f"    Invocations: {inv_30d:,}")
    r.log(f"    Avg duration: {dur:.0f} ms")
    r.log(f"    Memory: {memory_mb} MB")
    r.log(f"    Compute: ${compute_cost_30d:.4f}")
    r.log(f"    Requests: ${request_cost_30d:.4f}")
    r.log(f"    Total: ${total_cost_30d:.4f} / month")
    r.kv(action="cost-estimate", target="enhanced-openbb-handler",
         inv_30d=inv_30d, avg_ms=round(dur), memory_mb=memory_mb,
         monthly_cost_usd=f"{total_cost_30d:.4f}")

    # ─────────────────────────────────────────────────
    # C. INSPECT justhodl-email-reports-v2 vs justhodl-daily-report-v3
    # ─────────────────────────────────────────────────
    r.section("C. Compare email-reports-v2 vs daily-report-v3 email behavior")

    v2_code = download_code("justhodl-email-reports-v2")
    v3_code = download_code("justhodl-daily-report-v3")

    # Helper to extract email signals
    def extract_email_info(code_files):
        full = "\n".join(code_files.values())
        return {
            "ses_usage":      bool(re.search(r"\.send_email\(|\.send_raw_email", full)),
            "ses_from":       re.search(r"['\"](raafouis[^'\"]*@[^'\"]+)['\"]", full),
            "ses_from_generic": re.search(r"['\"]([a-z0-9_\.+\-]+@[a-z0-9\.\-]+)['\"]", full, re.I),
            "subject_lines":  re.findall(r"['\"]?[Ss]ubject['\"]?\s*[:=]\s*['\"]([^'\"]{5,80})['\"]", full),
            "recipient":      re.search(r"ToAddresses\s*['\"]?[:=]\s*\[\s*['\"]([^'\"]+)['\"]", full),
            "html_template":  bool(re.search(r"<html|<body|<table", full, re.I)),
            "file_bytes":     sum(len(v) for v in code_files.values()),
        }

    v2_info = extract_email_info(v2_code)
    v3_info = extract_email_info(v3_code)

    r.log(f"  justhodl-email-reports-v2:")
    r.log(f"    Code size: {v2_info['file_bytes']} bytes, {len(v2_code)} file(s)")
    r.log(f"    Uses SES send_email: {v2_info['ses_usage']}")
    r.log(f"    HTML email: {v2_info['html_template']}")
    if v2_info['ses_from']:
        r.log(f"    From: {v2_info['ses_from'].group(1)}")
    if v2_info['subject_lines']:
        r.log(f"    Subjects found: {v2_info['subject_lines'][:3]}")
    if v2_info['recipient']:
        r.log(f"    Recipient: {v2_info['recipient'].group(1)}")

    r.log("")
    r.log(f"  justhodl-daily-report-v3:")
    r.log(f"    Code size: {v3_info['file_bytes']} bytes, {len(v3_code)} file(s)")
    r.log(f"    Uses SES send_email: {v3_info['ses_usage']}")
    r.log(f"    HTML email: {v3_info['html_template']}")
    if v3_info['subject_lines']:
        r.log(f"    Subjects found: {v3_info['subject_lines'][:3]}")

    # EB rules for email-reports-v2
    v2_rules = []
    try:
        arn = lam.get_function_configuration(FunctionName="justhodl-email-reports-v2")["FunctionArn"]
        v2_rules = ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    except ClientError:
        pass

    r.log("")
    r.log(f"  v2 schedule: {v2_rules}")
    for rn in v2_rules:
        try:
            rule = ev.describe_rule(Name=rn)
            r.log(f"    {rn}: {rule.get('ScheduleExpression')} · State={rule.get('State')}")
        except ClientError:
            pass

    # Verdict
    r.log("")
    if v2_info["ses_usage"] and not v3_info["ses_usage"]:
        verdict = "v2 is the ACTIVE email sender; v3 doesn't send email. v2 is needed."
    elif v2_info["ses_usage"] and v3_info["ses_usage"]:
        verdict = "BOTH v2 and v3 send email. v2 likely a legacy duplicate — inspect manually."
    elif not v2_info["ses_usage"]:
        verdict = "v2 does NOT send email. Possibly redundant or broken."
    else:
        verdict = "Unclear — manual inspection recommended."
    r.log(f"  VERDICT: {verdict}")
    r.kv(action="email-dedup", v2_sends_email=v2_info["ses_usage"],
         v3_sends_email=v3_info["ses_usage"], verdict=verdict[:100])

    r.log("Done")
