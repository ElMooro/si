"""
Shared helpers for ops scripts that create/update Lambdas + EB rules.

Eliminates ~150 lines of boilerplate per ops script by centralizing the
create-or-update + EB rule + smoke-test pattern.

Usage from an ops script in aws/ops/pending/:
    from _lambda_deploy_helpers import deploy_lambda

    deploy_lambda(
        report=r,
        function_name="justhodl-gdelt-sentiment",
        source_dir=Path("aws/lambdas/justhodl-gdelt-sentiment/source"),
        env_vars={"S3_BUCKET": "justhodl-dashboard-live"},
        eb_rule_name="justhodl-gdelt-sentiment-30min",
        eb_schedule="rate(30 minutes)",
        timeout=180,
        memory=512,
        reserved_concurrency=1,
        smoke_test=True,
    )

Behaviors:
- Idempotent: creates Lambda if missing, updates if present
- Retries on ResourceConflictException (concurrent deploy race)
- Always re-applies env vars + timeout + memory on update
- Always re-asserts EB rule schedule + target + invoke permission
- Optional smoke test invocation with structured stat extraction
"""
from __future__ import annotations
import io
import json
import time
import zipfile
from pathlib import Path
from typing import Optional, Dict, List

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

_lam = boto3.client("lambda", region_name=REGION)
_eb = boto3.client("events", region_name=REGION)


def build_zip(source_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in source_dir.rglob("*"):
            if f.is_file() and "__pycache__" not in str(f) and not f.name.endswith(".pyc"):
                z.write(f, str(f.relative_to(source_dir)))
    return buf.getvalue()


def function_exists(name: str) -> bool:
    try:
        _lam.get_function(FunctionName=name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def _retry_on_conflict(call, *args, max_attempts=6, **kwargs):
    """Exponential backoff retry on ResourceConflictException."""
    for attempt in range(max_attempts):
        try:
            return call(*args, **kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException" and attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def create_or_update_lambda(
    *, report, function_name: str, zip_bytes: bytes,
    env_vars: Dict[str, str], timeout: int, memory: int,
    description: str, reserved_concurrency: Optional[int],
    create_function_url: bool,
):
    if function_exists(function_name):
        report.log(f"  Lambda exists — updating")
        _retry_on_conflict(_lam.update_function_code,
                           FunctionName=function_name, ZipFile=zip_bytes)
        _lam.get_waiter("function_updated").wait(
            FunctionName=function_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30},
        )
        _retry_on_conflict(_lam.update_function_configuration,
                           FunctionName=function_name,
                           Environment={"Variables": env_vars},
                           Timeout=timeout,
                           MemorySize=memory,
                           Description=description)
        report.ok(f"  ✓ updated {function_name}")
    else:
        report.log(f"  Lambda missing — creating")
        _lam.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description=description,
            Timeout=timeout,
            MemorySize=memory,
            Architectures=["x86_64"],
            Environment={"Variables": env_vars},
        )
        _lam.get_waiter("function_active").wait(
            FunctionName=function_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30},
        )
        report.ok(f"  ✓ created {function_name}")

    if reserved_concurrency is not None:
        try:
            _lam.put_function_concurrency(
                FunctionName=function_name,
                ReservedConcurrentExecutions=reserved_concurrency,
            )
            report.ok(f"  ✓ reserved concurrency = {reserved_concurrency}")
        except ClientError as e:
            report.warn(f"  reserved concurrency: {e}")

    if create_function_url:
        try:
            url_resp = _lam.create_function_url_config(
                FunctionName=function_name,
                AuthType="NONE",
                Cors={
                    "AllowCredentials": False,
                    "AllowHeaders": ["content-type"],
                    "AllowMethods": ["*"],
                    "AllowOrigins": ["*"],
                    "MaxAge": 86400,
                },
            )
            report.ok(f"  ✓ Function URL: {url_resp['FunctionUrl']}")
        except ClientError as e:
            if "ResourceConflictException" not in str(e):
                report.warn(f"  Function URL: {e}")
        try:
            _lam.add_permission(
                FunctionName=function_name,
                StatementId="FunctionUrlAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except ClientError as e:
            if "ResourceConflictException" not in str(e):
                report.warn(f"  add public permission: {e}")


def ensure_eb_rule(*, report, rule_name: str, schedule: str, function_name: str):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{function_name}"
    try:
        existing = _eb.describe_rule(Name=rule_name)
        if existing["State"] == "DISABLED":
            _eb.enable_rule(Name=rule_name)
            report.ok(f"  ✓ enabled disabled rule {rule_name}")
        elif existing.get("ScheduleExpression") != schedule:
            _eb.put_rule(Name=rule_name, ScheduleExpression=schedule, State="ENABLED")
            report.ok(f"  ✓ updated schedule on {rule_name}")
        else:
            report.log(f"  rule already correct: {rule_name} ({schedule})")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            _eb.put_rule(Name=rule_name, ScheduleExpression=schedule, State="ENABLED",
                         Description=f"Trigger {function_name}")
            report.ok(f"  ✓ created rule {rule_name}")
        else:
            raise

    _eb.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
    report.ok(f"  ✓ target → {function_name}")

    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}"
    sid = f"AllowEB-{rule_name}-{int(time.time())}"[:64]
    try:
        _lam.add_permission(
            FunctionName=function_name,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        report.ok(f"  ✓ added invoke permission")
    except ClientError as e:
        if "ResourceConflictException" not in str(e):
            report.warn(f"  add_permission: {e}")


def smoke_test(*, report, function_name: str) -> Optional[dict]:
    report.log(f"  invoking {function_name}…")
    try:
        resp = _lam.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=b'{"source":"deploy.smoke_test"}',
        )
    except ClientError as e:
        report.fail(f"  ✗ invoke: {e}")
        return None

    if "FunctionError" in resp:
        body = resp["Payload"].read().decode("utf-8")[:500]
        report.fail(f"  ✗ FunctionError: {resp['FunctionError']}")
        report.log(f"  body: {body}")
        return None

    body = json.loads(resp["Payload"].read())
    if isinstance(body, dict) and "body" in body:
        try:
            inner = json.loads(body["body"])
            stats = inner.get("stats", inner)
            report.ok(f"  ✓ smoke test passed")
            for k, v in (stats.items() if isinstance(stats, dict) else []):
                if isinstance(v, (str, int, float)):
                    report.log(f"    {k:24s} {v}")
            return inner
        except Exception:
            report.log(f"  body (raw): {body.get('body', '')[:200]}")
    return body


def deploy_lambda(
    *, report, function_name: str, source_dir: Path,
    env_vars: Dict[str, str],
    eb_rule_name: Optional[str] = None,
    eb_schedule: Optional[str] = None,
    timeout: int = 180,
    memory: int = 512,
    description: str = "",
    reserved_concurrency: Optional[int] = None,
    create_function_url: bool = True,
    smoke: bool = True,
):
    """One-call deployment: zip source, create/update, EB rule, smoke test."""
    zip_bytes = build_zip(source_dir)
    report.log(f"  zip: {len(zip_bytes)} bytes")

    report.section("1. Lambda")
    create_or_update_lambda(
        report=report, function_name=function_name, zip_bytes=zip_bytes,
        env_vars=env_vars, timeout=timeout, memory=memory,
        description=description, reserved_concurrency=reserved_concurrency,
        create_function_url=create_function_url,
    )

    if eb_rule_name and eb_schedule:
        report.section("2. EB rule + permissions")
        ensure_eb_rule(
            report=report, rule_name=eb_rule_name,
            schedule=eb_schedule, function_name=function_name,
        )

    if smoke:
        report.section("3. Smoke test")
        return smoke_test(report=report, function_name=function_name)
    return None
