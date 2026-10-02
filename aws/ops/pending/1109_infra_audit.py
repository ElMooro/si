"""Ops 1109: AWS infrastructure utilization audit.
Checks Lambda configs, S3 settings, CloudWatch alarms, X-Ray.
Writes: aws/ops/reports/1109_infra_audit.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1109_infra_audit.json")
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

KEY_LAMBDAS = [
    "justhodl-ticker-360", "justhodl-master-ranker",
    "justhodl-feed-heartbeat", "justhodl-flow-confluence",
]

def audit_lambda(fn):
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        # Check provisioned concurrency
        try:
            pc = lam.get_provisioned_concurrency_config(
                FunctionName=fn, Qualifier=cfg.get("Version", "$LATEST"))
            pc_count = pc.get("RequestedProvisionedConcurrentExecutions")
        except ClientError:
            pc_count = 0
        # Check X-Ray tracing
        tracing = cfg.get("TracingConfig", {}).get("Mode", "PassThrough")
        # Check reserved concurrency
        try:
            rc = lam.get_function_concurrency(FunctionName=fn)
            reserved = rc.get("ReservedConcurrentExecutions")
        except ClientError:
            reserved = None
        return {
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "runtime": cfg.get("Runtime"),
            "provisioned_concurrency": pc_count,
            "xray_tracing": tracing,
            "reserved_concurrency": reserved,
            "ephemeral_storage": cfg.get("EphemeralStorage", {}).get("Size"),
        }
    except Exception as e:
        return {"error": str(e)[:80]}

def audit_s3():
    try:
        bucket = "justhodl-dashboard-live"
        # Check versioning
        ver = s3.get_bucket_versioning(Bucket=bucket)
        # Check acceleration
        try:
            acc = s3.get_bucket_accelerate_configuration(Bucket=bucket)
            accel = acc.get("Status")
        except ClientError:
            accel = "unknown"
        # Check lifecycle (intelligent tiering)
        try:
            lc = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
            rules = len(lc.get("Rules", []))
        except ClientError:
            rules = 0
        # Check encryption
        try:
            enc = s3.get_bucket_encryption(Bucket=bucket)
            encrypted = True
        except ClientError:
            encrypted = False
        return {"versioning": ver.get("Status", "Suspended"),
                "transfer_acceleration": accel,
                "lifecycle_rules": rules,
                "default_encryption": encrypted}
    except Exception as e:
        return {"error": str(e)[:80]}

def audit_cloudwatch():
    try:
        # Count alarms
        alarms = cw.describe_alarms(MaxRecords=100)
        n_alarms = len(alarms.get("MetricAlarms", []))
        # Check for Lambda-specific alarms
        lambda_alarms = [a for a in alarms.get("MetricAlarms", [])
                         if "Lambda" in str(a.get("Namespace", ""))]
        return {"total_alarms": n_alarms,
                "lambda_alarms": len(lambda_alarms)}
    except Exception as e:
        return {"error": str(e)[:80]}

def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "lambdas": {}, "s3": {}, "cloudwatch": {}}
    for fn in KEY_LAMBDAS:
        rep["lambdas"][fn] = audit_lambda(fn)
    rep["s3"] = audit_s3()
    rep["cloudwatch"] = audit_cloudwatch()
    # Recommendations
    recs = []
    for fn, cfg in rep["lambdas"].items():
        if cfg.get("provisioned_concurrency", 0) == 0:
            recs.append(f"{fn}: no provisioned concurrency (cold starts on every invocation)")
        if cfg.get("xray_tracing") == "PassThrough":
            recs.append(f"{fn}: X-Ray tracing disabled (no distributed tracing)")
    if rep["s3"].get("transfer_acceleration") != "Enabled":
        recs.append("S3: transfer acceleration disabled")
    if rep["s3"].get("lifecycle_rules", 0) == 0:
        recs.append("S3: no lifecycle rules (no intelligent tiering/cost optimization)")
    if rep["cloudwatch"].get("lambda_alarms", 0) == 0:
        recs.append("CloudWatch: no Lambda alarms (no automated alerting on failures)")
    rep["recommendations"] = recs
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT, "w").write(json.dumps(rep, indent=2))
    print(json.dumps({"recommendations": len(recs)}, indent=2))
    for r in recs:
        print(" -", r)

if __name__ == "__main__":
    main()
