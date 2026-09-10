#!/usr/bin/env python3
"""Resumable, fail-closed JustHodl AI production rollout.

The default mode is a read-only plan.  Live changes require both ``--apply``
and the exact approval phrase.  The script is designed for
``.github/workflows/run-ops.yml`` and deliberately does not deploy the
Cloudflare Worker.
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import io
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

import boto3
from botocore.exceptions import ClientError, WaiterError


ACCOUNT = "857687956942"
REGION = "us-east-1"
APPROVAL_PHRASE = "I APPROVE JUSTHODL AI PRODUCTION ROLLOUT"
FUNCTION_NAME = "justhodl-ai"
BASE_STACK = "justhodl-ai-production-gates"
CONSUMER_STACK = "justhodl-ai-consumers"
DASHBOARD_BUCKET = "justhodl-dashboard-live"
PRIVATE_ARTIFACT_BUCKET = f"justhodl-ai-{ACCOUNT}"
ALARM_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT}:jh-ops-alerts"
BRAIN_KEY = "data/brain.json"
REPORT_SCHEMA = "JustHodlAiLiveRolloutReport/v1"
STATE_SCHEMA = "JustHodlAiLiveRolloutState/v1"
REPORT_NAME = "justhodl_ai_live_rollout.json"
STATE_NAME = "justhodl_ai_live_rollout_state.json"
RULE_NAMES = (
    "justhodl-ai-signal-envelope-v1-prod",
    "justhodl-ai-prediction-v2-prod",
    "justhodl-ai-model-package-state-prod",
)
CONSUMER_FUNCTIONS = (
    "justhodl-ai-signal-feature-consumer-prod",
    "justhodl-ai-prediction-ledger-consumer-prod",
    "justhodl-ai-model-governance-consumer-prod",
)
QUEUE_RESOURCES = {
    "SignalQueueArn": "SignalValidationQueue",
    "PredictionQueueArn": "PredictionLedgerQueue",
    "ModelGovernanceQueueArn": "ModelGovernanceQueue",
}
REPO_ROOT = Path(__file__).resolve().parents[3]
AI_ROOT = REPO_ROOT / "aws" / "lambdas" / "justhodl-ai"
BASE_TEMPLATE = AI_ROOT / "iam" / "production-gates.template.json"
CONSUMER_TEMPLATE = AI_ROOT / "consumer-functions.template.json"
DENY_POLICY = AI_ROOT / "iam" / "dashboard-brain-deny-policy.json"
CONFIG_PATH = AI_ROOT / "config.json"
REQUIREMENTS_PATH = AI_ROOT / "requirements-governance.txt"
REPORT_DIR = REPO_ROOT / "aws" / "ops" / "reports" / "latest"
DEFAULT_CANARY_INPUT = REPO_ROOT / "aws" / "ops" / "pending" / "justhodl_ai_canary_input.json"


class RolloutError(RuntimeError):
    """A production safety gate failed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def b64sha(value: bytes) -> str:
    return base64.b64encode(hashlib.sha256(value).digest()).decode("ascii")


def error_code(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Code", ""))
    return ""


def error_message(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Message", ""))
    return str(exc)


def secret_safe(value: Any, key: str = "") -> Any:
    """Redact likely credentials before data reaches a report or stdout."""
    marker = key.lower()
    if any(word in marker for word in ("secret", "token", "password", "credential")):
        return "<redacted>"
    if isinstance(value, Mapping):
        return {str(k): secret_safe(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [secret_safe(item, key) for item in value]
    if isinstance(value, tuple):
        return [secret_safe(item, key) for item in value]
    return value


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(json.dumps(secret_safe(value), indent=2, default=str).encode() + b"\n")
    temp.replace(path)


def deterministic_zip(entries: Mapping[str, bytes]) -> bytes:
    """Build a byte-for-byte stable Lambda zip."""
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(entries):
            info = zipfile.ZipInfo(name, (2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, entries[name])
    return output.getvalue()


def source_entries(source: Path, *, include_shared: bool = True) -> Dict[str, bytes]:
    entries: Dict[str, bytes] = {}
    local_names = {
        item.name
        for item in source.rglob("*.py")
        if item.is_file() and "__pycache__" not in item.parts
    }
    if include_shared:
        for item in sorted((REPO_ROOT / "aws" / "shared").glob("*.py")):
            if item.name not in local_names:
                entries[item.name] = item.read_bytes()
    for item in sorted(source.rglob("*")):
        if not item.is_file() or "__pycache__" in item.parts or item.suffix == ".pyc":
            continue
        entries[item.relative_to(source).as_posix()] = item.read_bytes()
    return entries


def package_consumers() -> bytes:
    return deterministic_zip(source_entries(AI_ROOT / "source"))


def package_main_lambda() -> bytes:
    """Package source/shared code plus the pinned governance dependency tree."""
    entries = source_entries(AI_ROOT / "source")
    with tempfile.TemporaryDirectory(prefix="justhodl-ai-governance-") as tmp:
        target = Path(tmp)
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-compile",
            "--requirement",
            str(REQUIREMENTS_PATH),
            "--target",
            str(target),
        ]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
        for item in sorted(target.rglob("*")):
            if not item.is_file() or "__pycache__" in item.parts or item.suffix == ".pyc":
                continue
            relative = item.relative_to(target).as_posix()
            if relative not in entries:
                entries[relative] = item.read_bytes()
    expanded_bytes = sum(len(value) for value in entries.values())
    if expanded_bytes > 240 * 1024 * 1024:
        raise RolloutError(
            f"governed Lambda package expands to {expanded_bytes} bytes; refusing Lambda limit risk"
        )
    return deterministic_zip(entries)


def merge_policy_documents(
    current: Optional[Mapping[str, Any]], required: Mapping[str, Any]
) -> Dict[str, Any]:
    """Merge reviewed Sids while retaining every unrelated live statement."""
    merged = copy.deepcopy(
        current or {"Version": required.get("Version", "2012-10-17"), "Statement": []}
    )
    statements = merged.get("Statement", [])
    if isinstance(statements, Mapping):
        statements = [statements]
    if not isinstance(statements, list):
        raise RolloutError("live dashboard bucket policy has an invalid Statement")
    reviewed = required.get("Statement", [])
    if not isinstance(reviewed, list) or not reviewed:
        raise RolloutError("reviewed dashboard deny policy has no statements")
    by_sid = {
        item.get("Sid"): index
        for index, item in enumerate(statements)
        if isinstance(item, Mapping) and item.get("Sid")
    }
    for statement in reviewed:
        if not isinstance(statement, Mapping) or not statement.get("Sid"):
            raise RolloutError("every reviewed deny statement must have a Sid")
        sid = statement["Sid"]
        if sid in by_sid:
            statements[by_sid[sid]] = copy.deepcopy(statement)
        else:
            by_sid[sid] = len(statements)
            statements.append(copy.deepcopy(statement))
    merged["Statement"] = statements
    merged.setdefault("Version", "2012-10-17")
    return merged


def validate_local_artifacts() -> None:
    """Reject reviewed artifacts that no longer stage every consumer disabled."""
    base = json.loads(BASE_TEMPLATE.read_text(encoding="utf-8"))
    consumers = json.loads(CONSUMER_TEMPLATE.read_text(encoding="utf-8"))
    base_rules = [
        item
        for item in base.get("Resources", {}).values()
        if item.get("Type") == "AWS::Events::Rule"
    ]
    mappings = [
        item
        for item in consumers.get("Resources", {}).values()
        if item.get("Type") == "AWS::Lambda::EventSourceMapping"
    ]
    if len(base_rules) != 3 or any(
        item.get("Properties", {}).get("State") != "DISABLED" for item in base_rules
    ):
        raise RolloutError("base template must stage exactly three disabled EventBridge rules")
    if len(mappings) != 3 or any(
        item.get("Properties", {}).get("Enabled") is not False for item in mappings
    ):
        raise RolloutError("consumer template must stage exactly three disabled mappings")
    rules = base.get("Rules", {})
    serialized_rules = json.dumps(rules, separators=(",", ":"))
    if ACCOUNT not in serialized_rules or REGION not in serialized_rules:
        raise RolloutError("base template is not pinned to the approved account and region")


class Reporter:
    def __init__(self, mode: str, run_id: str) -> None:
        self.path = REPORT_DIR / REPORT_NAME
        self.state_path = REPORT_DIR / STATE_NAME
        self._secret_values: set[str] = set()
        self.data: Dict[str, Any] = {
            "schema": REPORT_SCHEMA,
            "run_id": run_id,
            "mode": mode,
            "started_at": utc_now(),
            "target": {"account": ACCOUNT, "region": REGION},
            "status": "RUNNING",
            "steps": [],
            "artifacts": {},
            "change_sets": [],
            "checks": [],
            "activation": {"rules": "disabled", "mappings": "disabled"},
            "next_steps": [
                {
                    "schema": "JustHodlAiExternalDeploymentNextStep/v1",
                    "system": "cloudflare",
                    "component": "justhodl-ai-proxy",
                    "status": "REQUIRED_MANUAL_ACTION",
                    "path": "cloudflare/workers/justhodl-ai-proxy",
                    "reason": "Cloudflare credentials and deployment are outside this AWS rollout.",
                }
            ],
        }
        self.flush()

    def flush(self) -> None:
        def redact(value: Any) -> Any:
            if isinstance(value, Mapping):
                return {str(k): redact(v) for k, v in value.items()}
            if isinstance(value, list):
                return [redact(item) for item in value]
            if isinstance(value, str):
                result = value
                for secret_value in self._secret_values:
                    result = result.replace(secret_value, "<redacted>")
                return result
            return value

        atomic_json(self.path, redact(self.data))

    def register_secret(self, value: Any) -> None:
        if isinstance(value, str) and len(value) >= 8:
            self._secret_values.add(value)

    def step(self, name: str, status: str, **detail: Any) -> None:
        record = {"name": name, "status": status, "at": utc_now()}
        record.update(secret_safe(detail))
        self.data["steps"].append(record)
        self.flush()
        print(json.dumps({"step": name, "status": status}, separators=(",", ":")))

    def check(self, name: str, passed: bool, **detail: Any) -> None:
        record = {"name": name, "passed": bool(passed), "at": utc_now()}
        record.update(secret_safe(detail))
        self.data["checks"].append(record)
        self.flush()

    def finish(self, status: str, error: Optional[BaseException] = None) -> None:
        self.data["status"] = status
        self.data["finished_at"] = utc_now()
        if error is not None:
            self.data["error"] = {
                "type": type(error).__name__,
                "message": error_message(error)[:1000],
            }
        self.flush()


class AwsClients:
    """Lazily constructed boto3 clients; easy to replace in unit tests."""

    def __init__(self, session: Any = None) -> None:
        self.session = session or boto3.session.Session(region_name=REGION)
        self._clients: Dict[str, Any] = {}

    def __getitem__(self, name: str) -> Any:
        if name not in self._clients:
            self._clients[name] = self.session.client(name, region_name=REGION)
        return self._clients[name]


class Rollout:
    def __init__(
        self,
        clients: AwsClients,
        reporter: Reporter,
        *,
        apply: bool,
        canary_input: Path,
        approval_secret_id: str,
        approval_parameter: Optional[str] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.aws = clients
        self.report = reporter
        self.apply = apply
        self.canary_input_path = canary_input
        self.approval_secret_id = approval_secret_id
        self.approval_parameter = approval_parameter
        self.sleep = sleep
        self.mutated_lambda = False
        self.canary_promoted = False
        self.canary_context: Optional[Dict[str, Any]] = None
        self.rollback_state: Optional[Dict[str, Any]] = None
        self.mappings: list[str] = []
        self.outputs: Dict[str, str] = {}
        self.state: Dict[str, Any] = {
            "schema": STATE_SCHEMA,
            "run_id": reporter.data["run_id"],
            "completed": [],
            "updated_at": utc_now(),
        }
        if reporter.state_path.is_file():
            try:
                existing = json.loads(reporter.state_path.read_text(encoding="utf-8"))
                if (
                    existing.get("schema") == STATE_SCHEMA
                    and existing.get("run_id") == reporter.data["run_id"]
                ):
                    self.state = existing
            except (OSError, ValueError):
                pass
        self.rollback_secret_id = (
            f"justhodl-ai/ops/live-rollout/{reporter.data['run_id']}/rollback"
        )

    def checkpoint(self, phase: str, **evidence: Any) -> None:
        if phase not in self.state["completed"]:
            self.state["completed"].append(phase)
        self.state["updated_at"] = utc_now()
        self.state.setdefault("evidence", {})[phase] = secret_safe(evidence)
        atomic_json(self.report.state_path, self.state)

    def completed(self, phase: str) -> bool:
        return phase in self.state.get("completed", [])

    def verify_target(self) -> None:
        identity = self.aws["sts"].get_caller_identity()
        account = str(identity.get("Account", ""))
        session_region = getattr(self.aws.session, "region_name", None)
        if account != ACCOUNT:
            raise RolloutError(f"AWS account mismatch: expected {ACCOUNT}, got {account or 'unknown'}")
        if session_region != REGION:
            raise RolloutError(f"AWS region mismatch: expected {REGION}, got {session_region or 'unknown'}")
        self.report.check("aws_target", True, account=account, region=session_region)

    def discover_canary_candidates(self) -> None:
        """Report managed live endpoints and approved packages without mutation."""
        sm = self.aws["sagemaker"]
        endpoints = []
        endpoint_inventory = []
        token = None
        while True:
            request = {
                "SortBy": "CreationTime",
                "SortOrder": "Descending",
                "MaxResults": 50,
            }
            if token:
                request["NextToken"] = token
            page = sm.list_endpoints(**request)
            for item in page.get("Endpoints", []):
                name = str(item.get("EndpointName") or "")
                if not any(marker in name.lower() for marker in ("jh-ai", "justhodl")):
                    continue
                described = sm.describe_endpoint(EndpointName=name)
                arn = described.get("EndpointArn", "")
                tags = {
                    tag.get("Key"): tag.get("Value")
                    for tag in sm.list_tags(ResourceArn=arn).get("Tags", [])
                }
                config_name = described.get("EndpointConfigName", "")
                config = sm.describe_endpoint_config(EndpointConfigName=config_name)
                summary = {
                    "endpoint_name": name,
                    "status": item.get("EndpointStatus"),
                    "endpoint_config": config_name,
                    "variants": [
                        variant.get("VariantName")
                        for variant in config.get("ProductionVariants", [])
                        if variant.get("VariantName")
                    ],
                    "managed": tags.get("justhodl-ai-managed") == "true",
                    "owner": tags.get("justhodl-ai-owner"),
                    "created_at": item.get("CreationTime"),
                }
                endpoint_inventory.append(summary)
                if (
                    item.get("EndpointStatus") != "InService"
                    or tags.get("justhodl-ai-managed") != "true"
                ):
                    continue
                endpoints.append(
                    {
                        "endpoint_name": name,
                        "endpoint_config": config_name,
                        "variants": summary["variants"],
                        "created_at": item.get("CreationTime"),
                    }
                )
            token = page.get("NextToken")
            if not token:
                break

        packages = []
        token = None
        while True:
            request = {
                "ModelPackageGroupName": "justhodl-ai-prod",
                "ModelApprovalStatus": "Approved",
                "SortBy": "CreationTime",
                "SortOrder": "Descending",
                "MaxResults": 50,
            }
            if token:
                request["NextToken"] = token
            try:
                page = sm.list_model_packages(**request)
            except ClientError as exc:
                if error_code(exc) in {
                    "ValidationException",
                    "ResourceNotFound",
                    "ResourceNotFoundException",
                }:
                    break
                raise
            for item in page.get("ModelPackageSummaryList", []):
                packages.append(
                    {
                        "model_package_arn": item.get("ModelPackageArn"),
                        "approval_status": item.get("ModelApprovalStatus"),
                        "creation_time": item.get("CreationTime"),
                    }
                )
            token = page.get("NextToken")
            if not token:
                break

        models = []
        token = None
        while True:
            request = {
                "SortBy": "CreationTime",
                "SortOrder": "Descending",
                "MaxResults": 100,
            }
            if token:
                request["NextToken"] = token
            page = sm.list_models(**request)
            for item in page.get("Models", []):
                name = str(item.get("ModelName") or "")
                if not any(marker in name.lower() for marker in ("jh-ai", "justhodl")):
                    continue
                described = sm.describe_model(ModelName=name)
                container = described.get("PrimaryContainer") or {}
                models.append(
                    {
                        "model_name": name,
                        "image": container.get("Image"),
                        "model_data_url": container.get("ModelDataUrl"),
                        "created_at": item.get("CreationTime"),
                    }
                )
            token = page.get("NextToken")
            if not token:
                break

        training_jobs = []
        token = None
        while True:
            request = {
                "SortBy": "CreationTime",
                "SortOrder": "Descending",
                "MaxResults": 100,
            }
            if token:
                request["NextToken"] = token
            page = sm.list_training_jobs(**request)
            for item in page.get("TrainingJobSummaries", []):
                name = str(item.get("TrainingJobName") or "")
                if not any(marker in name.lower() for marker in ("jh-ai", "justhodl")):
                    continue
                described = sm.describe_training_job(TrainingJobName=name)
                training_jobs.append(
                    {
                        "training_job_name": name,
                        "status": described.get("TrainingJobStatus"),
                        "image": (described.get("AlgorithmSpecification") or {}).get(
                            "TrainingImage"
                        ),
                        "model_data_url": (
                            described.get("ModelArtifacts") or {}
                        ).get("S3ModelArtifacts"),
                        "created_at": item.get("CreationTime"),
                    }
                )
            token = page.get("NextToken")
            if not token:
                break

        self.report.data["canary_candidates"] = {
            "managed_in_service_endpoints": endpoints,
            "approved_model_packages": packages,
        }
        self.report.data["legacy_sagemaker_inventory"] = {
            "ai_endpoints": endpoint_inventory,
            "ai_models": models,
            "ai_training_jobs": training_jobs,
        }
        self.report.flush()

    def _stack_exists(self, name: str) -> bool:
        try:
            self.aws["cloudformation"].describe_stacks(StackName=name)
            return True
        except ClientError as exc:
            if error_code(exc) == "ValidationError":
                return False
            raise

    def stack_outputs(self, name: str) -> Dict[str, str]:
        response = self.aws["cloudformation"].describe_stacks(StackName=name)
        stack = response["Stacks"][0]
        status = stack.get("StackStatus", "")
        if status.endswith("_FAILED") or "ROLLBACK" in status:
            raise RolloutError(f"stack {name} is unhealthy: {status}")
        return {
            item["OutputKey"]: item["OutputValue"]
            for item in stack.get("Outputs", [])
            if "OutputKey" in item and "OutputValue" in item
        }

    def deploy_stack(
        self,
        name: str,
        template_path: Path,
        *,
        parameters: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        cfn = self.aws["cloudformation"]
        exists = self._stack_exists(name)
        digest = sha256_file(template_path)
        deployment_digest = sha256_bytes(
            canonical_json({"template": digest, "parameters": dict(parameters or {})})
        )
        change_name = f"justhodl-ai-approved-{deployment_digest[:16]}"
        kwargs: Dict[str, Any] = {
            "StackName": name,
            "ChangeSetName": change_name,
            "ChangeSetType": "UPDATE" if exists else "CREATE",
            "Description": (
                "Approved JustHodl AI v2 rollout; deterministic template "
                f"sha256:{digest}"
            ),
            "TemplateBody": template_path.read_text(encoding="utf-8"),
            "Capabilities": ["CAPABILITY_NAMED_IAM"],
            "Tags": [
                {"Key": "justhodl", "Value": "ai"},
                {"Key": "justhodl-ai-managed", "Value": "true"},
                {"Key": "rollout-template-sha256", "Value": digest},
            ],
        }
        if parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": key, "ParameterValue": value}
                for key, value in sorted(parameters.items())
            ]
        try:
            cfn.create_change_set(**kwargs)
        except ClientError as exc:
            if error_code(exc) == "AlreadyExistsException":
                existing = cfn.describe_change_set(StackName=name, ChangeSetName=change_name)
                if existing.get("Status") == "DELETE_COMPLETE":
                    cfn.create_change_set(**kwargs)
                elif existing.get("ExecutionStatus") == "EXECUTE_COMPLETE":
                    status = cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
                    if not status.endswith("_COMPLETE") or "ROLLBACK" in status:
                        raise RolloutError(f"reused stack {name} is unsafe: {status}")
                    summary = {
                        "stack": name,
                        "name": change_name,
                        "type": kwargs["ChangeSetType"],
                        "status": "REUSED_EXECUTED",
                        "executed_status": status,
                        "template_sha256": digest,
                        "changes": [],
                    }
                    self.report.data["change_sets"].append(summary)
                    self.report.flush()
                    return summary
                if existing.get("Status") == "FAILED" and "didn't contain changes" not in existing.get("StatusReason", ""):
                    cfn.delete_change_set(StackName=name, ChangeSetName=change_name)
                    cfn.create_change_set(**kwargs)
            else:
                raise
        try:
            cfn.get_waiter("change_set_create_complete").wait(
                StackName=name,
                ChangeSetName=change_name,
                WaiterConfig={"Delay": 5, "MaxAttempts": 120},
            )
        except WaiterError:
            described = cfn.describe_change_set(StackName=name, ChangeSetName=change_name)
            reason = described.get("StatusReason", "")
            if described.get("Status") == "FAILED" and "didn't contain changes" in reason:
                summary = {
                    "stack": name,
                    "name": change_name,
                    "type": kwargs["ChangeSetType"],
                    "status": "NO_CHANGES",
                    "template_sha256": digest,
                    "changes": [],
                }
                self.report.data["change_sets"].append(summary)
                self.report.flush()
                return summary
            raise RolloutError(f"change set {name}/{change_name} failed: {reason}")
        described = cfn.describe_change_set(StackName=name, ChangeSetName=change_name)
        summary = {
            "stack": name,
            "name": change_name,
            "type": kwargs["ChangeSetType"],
            "status": described.get("Status"),
            "template_sha256": digest,
            "changes": [
                {
                    "action": item.get("ResourceChange", {}).get("Action"),
                    "logical_id": item.get("ResourceChange", {}).get("LogicalResourceId"),
                    "resource_type": item.get("ResourceChange", {}).get("ResourceType"),
                    "replacement": item.get("ResourceChange", {}).get("Replacement"),
                }
                for item in described.get("Changes", [])
            ],
        }
        self.report.data["change_sets"].append(summary)
        self.report.flush()
        print(json.dumps({"change_set_summary": summary}, separators=(",", ":")))
        if not self.apply:
            return summary
        cfn.execute_change_set(StackName=name, ChangeSetName=change_name)
        waiter_name = "stack_update_complete" if exists else "stack_create_complete"
        try:
            cfn.get_waiter(waiter_name).wait(
                StackName=name,
                WaiterConfig={"Delay": 30, "MaxAttempts": 180},
            )
        except WaiterError as exc:
            status = "UNKNOWN"
            reason = error_message(exc)
            try:
                stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
                status = stack.get("StackStatus", status)
                reason = stack.get("StackStatusReason", reason)
            except Exception:
                pass
            raise RolloutError(f"stack {name} failed: {status}: {reason}")
        status = cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
        if not status.endswith("_COMPLETE") or "ROLLBACK" in status:
            raise RolloutError(f"stack {name} completed in unsafe status {status}")
        summary["executed_status"] = status
        self.report.flush()
        return summary

    def immutable_upload(
        self,
        bucket: str,
        prefix: str,
        data: bytes,
        *,
        content_type: str,
        kms_key_arn: Optional[str] = None,
    ) -> Dict[str, str]:
        digest = sha256_bytes(data)
        key = f"{prefix.rstrip('/')}/sha256/{digest}.zip"
        s3 = self.aws["s3"]
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            if head.get("Metadata", {}).get("sha256") != digest:
                raise RolloutError(f"immutable artifact metadata mismatch at s3://{bucket}/{key}")
            return {"bucket": bucket, "key": key, "sha256": digest, "version_id": head.get("VersionId", "")}
        except ClientError as exc:
            if error_code(exc) not in {"404", "NoSuchKey", "NotFound"}:
                raise
        kwargs: Dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
            "Metadata": {"sha256": digest},
            "ChecksumSHA256": b64sha(data),
            "IfNoneMatch": "*",
        }
        if kms_key_arn:
            kwargs.update(ServerSideEncryption="aws:kms", SSEKMSKeyId=kms_key_arn)
        try:
            s3.put_object(**kwargs)
        except ClientError as exc:
            if error_code(exc) not in {"PreconditionFailed", "412"}:
                raise
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("Metadata", {}).get("sha256") != digest:
            raise RolloutError(f"uploaded artifact verification failed for s3://{bucket}/{key}")
        return {"bucket": bucket, "key": key, "sha256": digest, "version_id": head.get("VersionId", "")}

    def migrate_brain(self, private_bucket: str, data_key_arn: str) -> Dict[str, Any]:
        s3 = self.aws["s3"]
        source = s3.get_object(Bucket=DASHBOARD_BUCKET, Key=BRAIN_KEY)
        source_bytes = source["Body"].read()
        source_digest = sha256_bytes(source_bytes)
        try:
            destination = s3.head_object(Bucket=private_bucket, Key=BRAIN_KEY)
            version_id = destination.get("VersionId")
            if (
                destination.get("Metadata", {}).get("sha256") == source_digest
                and destination.get("ServerSideEncryption") == "aws:kms"
                and destination.get("SSEKMSKeyId") == data_key_arn
                and version_id
            ):
                body = s3.get_object(Bucket=private_bucket, Key=BRAIN_KEY, VersionId=version_id)["Body"].read()
                if sha256_bytes(body) == source_digest:
                    return {
                        "sha256": source_digest,
                        "source_version_id": source.get("VersionId", ""),
                        "destination_version_id": version_id,
                        "reused": True,
                    }
        except ClientError as exc:
            if error_code(exc) not in {"404", "NoSuchKey", "NotFound"}:
                raise
        response = s3.put_object(
            Bucket=private_bucket,
            Key=BRAIN_KEY,
            Body=source_bytes,
            ContentType=source.get("ContentType", "application/json"),
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=data_key_arn,
            Metadata={
                "sha256": source_digest,
                "source-bucket": DASHBOARD_BUCKET,
                "source-key": BRAIN_KEY,
                "source-version-id": source.get("VersionId", ""),
            },
            ChecksumSHA256=b64sha(source_bytes),
        )
        version_id = response.get("VersionId")
        if not version_id:
            raise RolloutError("canonical Brain upload returned no version id")
        head = s3.head_object(Bucket=private_bucket, Key=BRAIN_KEY, VersionId=version_id)
        body = s3.get_object(Bucket=private_bucket, Key=BRAIN_KEY, VersionId=version_id)["Body"].read()
        if (
            sha256_bytes(body) != source_digest
            or head.get("Metadata", {}).get("sha256") != source_digest
            or head.get("ServerSideEncryption") != "aws:kms"
            or head.get("SSEKMSKeyId") != data_key_arn
        ):
            raise RolloutError("canonical Brain checksum/version/encryption verification failed")
        return {
            "sha256": source_digest,
            "source_version_id": source.get("VersionId", ""),
            "destination_version_id": version_id,
            "reused": False,
        }

    def merge_dashboard_policy(self) -> Dict[str, Any]:
        s3 = self.aws["s3"]
        required = json.loads(DENY_POLICY.read_text(encoding="utf-8"))
        try:
            current = json.loads(s3.get_bucket_policy(Bucket=DASHBOARD_BUCKET)["Policy"])
        except ClientError as exc:
            if error_code(exc) == "NoSuchBucketPolicy":
                current = None
            else:
                raise
        current_statements = (current or {}).get("Statement", [])
        if isinstance(current_statements, Mapping):
            current_statements = [current_statements]
        before_unrelated = [
            item
            for item in current_statements
            if item.get("Sid") not in {required_item["Sid"] for required_item in required["Statement"]}
        ]
        merged = merge_policy_documents(current, required)
        s3.put_bucket_policy(Bucket=DASHBOARD_BUCKET, Policy=json.dumps(merged, separators=(",", ":")))
        live = json.loads(s3.get_bucket_policy(Bucket=DASHBOARD_BUCKET)["Policy"])
        after_unrelated = [
            item
            for item in live.get("Statement", [])
            if item.get("Sid") not in {required_item["Sid"] for required_item in required["Statement"]}
        ]
        if canonical_json(before_unrelated) != canonical_json(after_unrelated):
            raise RolloutError("dashboard bucket policy merge changed unrelated statements")
        for item in required["Statement"]:
            matches = [candidate for candidate in live["Statement"] if candidate.get("Sid") == item["Sid"]]
            if len(matches) != 1 or canonical_json(matches[0]) != canonical_json(item):
                raise RolloutError(f"dashboard deny statement {item['Sid']} was not persisted exactly")
        return {
            "policy_sha256": sha256_bytes(canonical_json(live)),
            "unrelated_statement_count": len(after_unrelated),
            "reviewed_sids": [item["Sid"] for item in required["Statement"]],
        }

    def _resource_physical_id(self, stack: str, logical_id: str) -> str:
        value = self.aws["cloudformation"].describe_stack_resource(
            StackName=stack, LogicalResourceId=logical_id
        )
        return value["StackResourceDetail"]["PhysicalResourceId"]

    def consumer_parameters(self, artifact: Mapping[str, str]) -> Dict[str, str]:
        sqs = self.aws["sqs"]
        values = {
            name: sqs.get_queue_attributes(
                QueueUrl=self._resource_physical_id(BASE_STACK, logical),
                AttributeNames=["QueueArn"],
            )["Attributes"]["QueueArn"]
            for name, logical in QUEUE_RESOURCES.items()
        }
        values.update(
            {
                "CodeBucket": artifact["bucket"],
                "CodeKey": artifact["key"],
                "PrivateBucket": PRIVATE_ARTIFACT_BUCKET,
                "SignalFeatureGroup": self.outputs["SignalFeatureGroupName"]
                if "SignalFeatureGroupName" in self.outputs
                else "justhodl-ai-signal-prod",
                "PredictionLedgerTable": self.outputs["PredictionLedgerTableName"],
                "PredictionArchiveBucket": self.outputs["PredictionLedgerArchiveBucketName"],
                "ModelPackageGroup": self.outputs["ModelPackageGroupName"],
                "AiDataKeyArn": self.outputs["DataKeyArn"],
            }
        )
        return values

    def discover_mappings(self) -> list[str]:
        lam = self.aws["lambda"]
        found: list[str] = []
        for name in CONSUMER_FUNCTIONS:
            response = lam.list_event_source_mappings(FunctionName=name)
            found.extend(
                item["UUID"]
                for item in response.get("EventSourceMappings", [])
                if item.get("UUID")
            )
        self.mappings = sorted(set(found))
        return self.mappings

    def disable_consumers(self) -> None:
        events = self.aws["events"]
        lam = self.aws["lambda"]
        for rule in RULE_NAMES:
            try:
                events.disable_rule(Name=rule)
            except ClientError as exc:
                if error_code(exc) != "ResourceNotFoundException":
                    raise
        for uuid in self.discover_mappings():
            try:
                current = lam.get_event_source_mapping(UUID=uuid)
                if current.get("State") not in {"Disabled", "Disabling"}:
                    lam.update_event_source_mapping(UUID=uuid, Enabled=False)
            except ClientError as exc:
                if error_code(exc) != "ResourceNotFoundException":
                    raise
        self.report.data["activation"] = {"rules": "disabled", "mappings": "disabled"}
        self.report.flush()

    def _put_rollback_secret(self, value: Mapping[str, Any]) -> None:
        client = self.aws["secretsmanager"]
        payload = json.dumps(value, separators=(",", ":"), default=str)
        try:
            client.create_secret(
                Name=self.rollback_secret_id,
                Description="Encrypted resumable rollback state for approved JustHodl AI rollout",
                SecretString=payload,
                Tags=[
                    {"Key": "justhodl", "Value": "ai"},
                    {"Key": "justhodl-ai-managed", "Value": "true"},
                    {"Key": "purpose", "Value": "rollout-rollback"},
                ],
            )
        except ClientError as exc:
            if error_code(exc) != "ResourceExistsException":
                raise
            existing = json.loads(
                client.get_secret_value(SecretId=self.rollback_secret_id)["SecretString"]
            )
            if existing.get("original_code_sha256") != value.get("original_code_sha256"):
                raise RolloutError("existing rollback secret does not match the captured code")

    def _get_rollback_secret(self) -> Dict[str, Any]:
        return json.loads(
            self.aws["secretsmanager"].get_secret_value(
                SecretId=self.rollback_secret_id
            )["SecretString"]
        )

    def capture_lambda_rollback(self) -> Dict[str, Any]:
        lam = self.aws["lambda"]
        try:
            existing = self._get_rollback_secret()
        except ClientError as exc:
            if error_code(exc) != "ResourceNotFoundException":
                raise
        else:
            if existing.get("schema") != "JustHodlAiLambdaRollback/v1":
                raise RolloutError("existing rollback secret has an unexpected schema")
            self.rollback_state = existing
            self.report.data["rollback"] = {
                "state_secret_id": self.rollback_secret_id,
                "original_code_sha256": existing.get("original_code_sha256"),
                "code_backup_sha256": existing.get("code_backup", {}).get("sha256"),
                "published_version": existing.get("published_version"),
                "aliases": [item["Name"] for item in existing.get("aliases", [])],
                "environment_keys": sorted(existing.get("environment", {})),
                "role": existing.get("role"),
                "resumed": True,
            }
            self.report.flush()
            return existing
        function = lam.get_function(FunctionName=FUNCTION_NAME)
        config = function["Configuration"]
        if config.get("PackageType", "Zip") != "Zip":
            raise RolloutError("justhodl-ai is not a Zip Lambda; refusing incompatible rollout")
        aliases: list[Dict[str, Any]] = []
        marker: Optional[str] = None
        while True:
            kwargs = {"FunctionName": FUNCTION_NAME}
            if marker:
                kwargs["Marker"] = marker
            page = lam.list_aliases(**kwargs)
            aliases.extend(
                {
                    "Name": item["Name"],
                    "FunctionVersion": item["FunctionVersion"],
                    "Description": item.get("Description", ""),
                    "RoutingConfig": item.get("RoutingConfig", {}),
                }
                for item in page.get("Aliases", [])
            )
            marker = page.get("NextMarker")
            if not marker:
                break
        try:
            published = lam.publish_version(
                FunctionName=FUNCTION_NAME,
                CodeSha256=config["CodeSha256"],
                Description=f"Pre-rollout rollback point {self.report.data['run_id']}",
            )
            rollback_version = published["Version"]
        except ClientError as exc:
            if error_code(exc) != "ResourceConflictException":
                raise
            rollback_version = ""
            versions = lam.list_versions_by_function(FunctionName=FUNCTION_NAME).get("Versions", [])
            for item in reversed(versions):
                if item.get("CodeSha256") == config.get("CodeSha256") and item.get("Version") != "$LATEST":
                    rollback_version = item["Version"]
                    break
            if not rollback_version:
                raise RolloutError("unable to identify an immutable rollback Lambda version")
        location = function.get("Code", {}).get("Location")
        if not location:
            raise RolloutError("Lambda code backup URL is unavailable")
        with urllib.request.urlopen(location, timeout=60) as response:
            old_zip = response.read()
        old_digest = sha256_bytes(old_zip)
        backup = self.immutable_upload(
            PRIVATE_ARTIFACT_BUCKET,
            "ops/justhodl-ai/rollback",
            old_zip,
            content_type="application/zip",
            kms_key_arn=self.outputs.get("DataKeyArn"),
        )
        state = {
            "schema": "JustHodlAiLambdaRollback/v1",
            "function_name": FUNCTION_NAME,
            "captured_at": utc_now(),
            "original_code_sha256": config.get("CodeSha256"),
            "code_backup": backup,
            "published_version": rollback_version,
            "aliases": aliases,
            "role": config.get("Role"),
            "environment": config.get("Environment", {}).get("Variables", {}),
        }
        self._put_rollback_secret(state)
        self.rollback_state = state
        self.report.data["rollback"] = {
            "state_secret_id": self.rollback_secret_id,
            "original_code_sha256": config.get("CodeSha256"),
            "code_backup_sha256": old_digest,
            "published_version": rollback_version,
            "aliases": [item["Name"] for item in aliases],
            "environment_keys": sorted(state["environment"]),
            "role": state["role"],
        }
        self.report.flush()
        return state

    def rollout_lambda(self, artifact: Mapping[str, str], approval_token: str) -> None:
        lam = self.aws["lambda"]
        state = self.rollback_state or self._get_rollback_secret()
        current = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        desired_config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        merged_env = dict(current.get("Environment", {}).get("Variables", {}))
        merged_env.update(desired_config["env"])
        merged_env.update(json.loads(self.outputs["RequiredLambdaEnvironment"]))
        merged_env["AI_CANARY_APPROVAL_TOKEN"] = approval_token
        for key, value in merged_env.items():
            if any(word in key.lower() for word in ("secret", "token", "password", "credential")):
                self.report.register_secret(value)
        lam.update_function_code(
            FunctionName=FUNCTION_NAME,
            S3Bucket=artifact["bucket"],
            S3Key=artifact["key"],
            Publish=False,
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=FUNCTION_NAME,
            WaiterConfig={"Delay": 5, "MaxAttempts": 120},
        )
        self.mutated_lambda = True
        lam.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Role=self.outputs["DedicatedLambdaRoleArn"],
            Environment={"Variables": merged_env},
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=FUNCTION_NAME,
            WaiterConfig={"Delay": 5, "MaxAttempts": 120},
        )
        live = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        if live.get("Role") != self.outputs["DedicatedLambdaRoleArn"]:
            raise RolloutError("dedicated Lambda role did not persist")
        if live.get("CodeSha256") == state.get("original_code_sha256"):
            raise RolloutError("main Lambda code did not change")
        for key, value in current.get("Environment", {}).get("Variables", {}).items():
            if key not in desired_config["env"] and live.get("Environment", {}).get("Variables", {}).get(key) != value:
                raise RolloutError(f"existing Lambda environment key {key} was not preserved")

    def rollback_lambda(self) -> None:
        if not self.mutated_lambda and self.rollback_state is None:
            return
        state = self.rollback_state or self._get_rollback_secret()
        lam = self.aws["lambda"]
        backup = state["code_backup"]
        lam.update_function_code(
            FunctionName=FUNCTION_NAME,
            S3Bucket=backup["bucket"],
            S3Key=backup["key"],
            Publish=False,
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=FUNCTION_NAME,
            WaiterConfig={"Delay": 5, "MaxAttempts": 120},
        )
        lam.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Role=state["role"],
            Environment={"Variables": state["environment"]},
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=FUNCTION_NAME,
            WaiterConfig={"Delay": 5, "MaxAttempts": 120},
        )
        for alias in state.get("aliases", []):
            lam.update_alias(
                FunctionName=FUNCTION_NAME,
                Name=alias["Name"],
                FunctionVersion=alias["FunctionVersion"],
                Description=alias.get("Description", ""),
                RoutingConfig=alias.get("RoutingConfig", {}),
            )
        live = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        if live.get("Role") != state["role"] or live.get("CodeSha256") != state["original_code_sha256"]:
            raise RolloutError("Lambda rollback verification failed")
        if live.get("Environment", {}).get("Variables", {}) != state["environment"]:
            raise RolloutError("Lambda environment rollback verification failed")
        self.report.data["rollback"]["restored_at"] = utc_now()
        self.report.data["rollback"]["verified"] = True
        self.report.flush()

    def approval_token(self) -> str:
        if self.approval_parameter:
            ssm = self.aws["ssm"]
            try:
                value = ssm.get_parameter(
                    Name=self.approval_parameter, WithDecryption=True
                )["Parameter"]["Value"]
                self.report.register_secret(value)
                return value
            except ClientError as exc:
                if error_code(exc) != "ParameterNotFound":
                    raise
                value = secrets.token_urlsafe(48)
                ssm.put_parameter(
                    Name=self.approval_parameter,
                    Description="JustHodl AI controlled canary approval token",
                    Type="SecureString",
                    Value=value,
                    Overwrite=False,
                    Tags=[
                        {"Key": "justhodl", "Value": "ai"},
                        {"Key": "justhodl-ai-managed", "Value": "true"},
                    ],
                )
                self.report.register_secret(value)
                return value
        manager = self.aws["secretsmanager"]
        try:
            value = manager.get_secret_value(SecretId=self.approval_secret_id)["SecretString"]
            self.report.register_secret(value)
            return value
        except ClientError as exc:
            if error_code(exc) != "ResourceNotFoundException":
                raise
            value = secrets.token_urlsafe(48)
            manager.create_secret(
                Name=self.approval_secret_id,
                Description="JustHodl AI controlled canary approval token",
                SecretString=value,
                Tags=[
                    {"Key": "justhodl", "Value": "ai"},
                    {"Key": "justhodl-ai-managed", "Value": "true"},
                ],
            )
            self.report.register_secret(value)
            return value

    def _service_token(self) -> str:
        value = self.aws["ssm"].get_parameter(
            Name="/justhodl/api-admin/token", WithDecryption=True
        )["Parameter"]["Value"]
        if len(value) < 16:
            raise RolloutError("owner service token is unavailable")
        self.report.register_secret(value)
        return value

    def invoke_http(self, path: str, body: Mapping[str, Any], service_token: str) -> Dict[str, Any]:
        event = {
            "version": "2.0",
            "rawPath": path,
            "headers": {"x-jh-service-token": service_token},
            "requestContext": {"http": {"method": "POST" if body else "GET", "path": path}},
            "body": json.dumps(body, separators=(",", ":")) if body else "",
        }
        response = self.aws["lambda"].invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=canonical_json(event),
        )
        if response.get("FunctionError"):
            raise RolloutError(f"Lambda invoke failed: {response['FunctionError']}")
        raw = response["Payload"].read()
        envelope = json.loads(raw)
        if int(envelope.get("statusCode", 500)) != 200:
            try:
                detail = json.loads(envelope.get("body", "{}"))
                message = detail.get("error", "HTTP canary failed")
            except Exception:
                message = "HTTP canary failed"
            raise RolloutError(str(message)[:500])
        return json.loads(envelope["body"])

    def load_canary_input(self) -> Dict[str, Any]:
        if not self.canary_input_path.is_file():
            raise RolloutError(
                f"required canary input is absent: {self.canary_input_path.relative_to(REPO_ROOT)}"
            )
        value = json.loads(self.canary_input_path.read_text(encoding="utf-8"))
        required = {
            "endpoint_name",
            "green_endpoint_name",
            "green_endpoint_config",
            "model_package_arn",
            "health_probe",
            "canary_requests",
            "rollback_plan",
            "green_estimated_hourly_cost_usd",
        }
        missing = sorted(required - set(value))
        if missing:
            raise RolloutError(f"canary input is incomplete: {', '.join(missing)}")
        return value

    def read_only_checks(self, private_bucket: str, brain_evidence: Mapping[str, Any]) -> None:
        s3 = self.aws["s3"]
        version = brain_evidence["destination_version_id"]
        body = s3.get_object(Bucket=private_bucket, Key=BRAIN_KEY, VersionId=version)["Body"].read()
        if sha256_bytes(body) != brain_evidence["sha256"]:
            raise RolloutError("read-only canonical Brain verification failed")
        service_token = self._service_token()
        health = self.invoke_http("/health", {}, service_token)
        if not health.get("ok"):
            raise RolloutError("Lambda read-only health check failed")
        self.report.check("read_only_health", True)
        self.report.check("canonical_brain_private_read", True, version_id=version, sha256=brain_evidence["sha256"])

    def run_live_canary(self, approval_token: str) -> Dict[str, Any]:
        context = self.load_canary_input()
        self.canary_context = context
        sm = self.aws["sagemaker"]
        endpoint = sm.describe_endpoint(EndpointName=context["endpoint_name"])
        package = sm.describe_model_package(ModelPackageName=context["model_package_arn"])
        if endpoint.get("EndpointStatus") != "InService":
            raise RolloutError("required blue endpoint is absent or not InService")
        if package.get("ModelApprovalStatus") != "Approved":
            raise RolloutError("required model package is absent or not manually Approved")
        service_token = self._service_token()
        payload = {
            "dry_run": False,
            "approval_token": approval_token,
            "owner": "JustHodl",
            "estimated_cost_usd": context.get("estimated_cost_usd", 0.0),
            "context": {**context, "approved_owner": "JustHodl"},
        }
        def invoke_controlled_canary() -> Dict[str, Any]:
            response = self.invoke_http(
                "/governance/deployment/canary-plan", payload, service_token
            )
            return response.get("result", {})

        result = invoke_controlled_canary()
        if result.get("status") != "PROMOTED":
            rollback = result.get("context", {}).get("rollback", {})
            if not rollback.get("traffic_restored"):
                raise RolloutError("live SageMaker canary failed without verified rollback")
            raise RolloutError("live SageMaker canary was rolled back; consumers remain disabled")
        states = {item.get("state") for item in result.get("events", [])}
        if not {"RUN_CANARY", "EVALUATE", "PROMOTE", "CLEANUP_GREEN"}.issubset(states):
            raise RolloutError("live SageMaker canary evidence is incomplete")
        # Prove the captured rollback against the live blue endpoint, then run
        # the same controlled deploy/inference/grading canary once more for the
        # final promoted state.  Activation cannot rely on an untested plan.
        self.canary_promoted = True
        self.rollback_sagemaker_canary()
        result = invoke_controlled_canary()
        if result.get("status") != "PROMOTED":
            raise RolloutError("post-rollback live SageMaker canary did not promote")
        states = {item.get("state") for item in result.get("events", [])}
        if not {"RUN_CANARY", "EVALUATE", "PROMOTE", "CLEANUP_GREEN"}.issubset(states):
            raise RolloutError("post-rollback SageMaker canary evidence is incomplete")
        self.report.check(
            "sagemaker_live_canary",
            True,
            status=result.get("status"),
            controlled_operations=result.get("control_evidence", {}).get("operations", []),
            inference_and_grading=True,
            live_rollback_drill=True,
        )
        self.canary_promoted = True
        return result

    def rollback_sagemaker_canary(self) -> None:
        """Restore the captured blue endpoint after any post-canary failure."""
        if not self.canary_promoted:
            return
        context = self.canary_context or self.load_canary_input()
        plan = context.get("rollback_plan", {})
        endpoint_name = context.get("endpoint_name")
        blue_config = plan.get("blue_endpoint_config")
        if not endpoint_name or not blue_config:
            raise RolloutError("SageMaker rollback context is incomplete")
        sm = self.aws["sagemaker"]
        sm.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=blue_config,
            RetainAllVariantProperties=False,
        )
        for attempt in range(40):
            current = sm.describe_endpoint(EndpointName=endpoint_name)
            if (
                current.get("EndpointStatus") == "InService"
                and current.get("EndpointConfigName") == blue_config
            ):
                runtime = self.aws["sagemaker-runtime"]
                probe = context["health_probe"]
                for _ in range(3):
                    response = runtime.invoke_endpoint(
                        EndpointName=endpoint_name,
                        ContentType=probe["content_type"],
                        Body=str(probe["body"]).encode("utf-8"),
                    )
                    if response.get("Body") is None:
                        raise RolloutError("SageMaker rollback health probe returned no body")
                self.report.data["sagemaker_rollback"] = {
                    "verified": True,
                    "endpoint": endpoint_name,
                    "restored_endpoint_config": blue_config,
                    "successful_health_checks": 3,
                    "at": utc_now(),
                }
                self.report.flush()
                self.canary_promoted = False
                if self.completed("live_sagemaker_canary"):
                    self.state["completed"].remove("live_sagemaker_canary")
                    self.state.setdefault("evidence", {})["live_sagemaker_canary"] = {
                        "status": "ROLLED_BACK",
                        "at": utc_now(),
                    }
                    atomic_json(self.report.state_path, self.state)
                return
            if current.get("EndpointStatus") in {"Failed", "OutOfService"}:
                break
            if attempt + 1 < 40:
                self.sleep(15)
        raise RolloutError("SageMaker blue endpoint rollback verification failed")

    def ensure_alarms(self) -> list[str]:
        cloudwatch = self.aws["cloudwatch"]
        self.aws["sns"].get_topic_attributes(TopicArn=ALARM_TOPIC_ARN)
        dlq_url = self._resource_physical_id(BASE_STACK, "EventRuleDeadLetterQueue")
        names = ["justhodl-ai-event-dlq-visible-prod"]
        cloudwatch.put_metric_alarm(
            AlarmName=names[0],
            AlarmDescription="JustHodl AI EventBridge/SQS dead-letter messages require operator action",
            ActionsEnabled=True,
            AlarmActions=[ALARM_TOPIC_ARN],
            Namespace="AWS/SQS",
            MetricName="ApproximateNumberOfMessagesVisible",
            Dimensions=[{"Name": "QueueName", "Value": dlq_url.rsplit("/", 1)[-1]}],
            Statistic="Maximum",
            Period=60,
            EvaluationPeriods=1,
            DatapointsToAlarm=1,
            Threshold=0.0,
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
            Tags=[
                {"Key": "justhodl", "Value": "ai"},
                {"Key": "justhodl-ai-managed", "Value": "true"},
            ],
        )
        for function in CONSUMER_FUNCTIONS:
            name = f"{function}-errors"
            names.append(name)
            cloudwatch.put_metric_alarm(
                AlarmName=name,
                AlarmDescription=f"Errors from {function}",
                ActionsEnabled=True,
                AlarmActions=[ALARM_TOPIC_ARN],
                Namespace="AWS/Lambda",
                MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": function}],
                Statistic="Sum",
                Period=60,
                EvaluationPeriods=1,
                DatapointsToAlarm=1,
                Threshold=0.0,
                ComparisonOperator="GreaterThanThreshold",
                TreatMissingData="notBreaching",
                Tags=[
                    {"Key": "justhodl", "Value": "ai"},
                    {"Key": "justhodl-ai-managed", "Value": "true"},
                ],
            )
        for _ in range(20):
            described = cloudwatch.describe_alarms(AlarmNames=names).get("MetricAlarms", [])
            states = {item["AlarmName"]: item.get("StateValue") for item in described}
            if len(states) == len(names) and all(value == "OK" for value in states.values()):
                self.report.check("dlq_and_alarms", True, alarms=names)
                return names
            if any(value == "ALARM" for value in states.values()):
                raise RolloutError("a rollout DLQ/error alarm is ALARM")
            self.sleep(15)
        raise RolloutError("rollout alarms did not reach OK before activation")

    def verify_rollback_check(self) -> None:
        state = self.rollback_state or self._get_rollback_secret()
        if not state.get("published_version") or not state.get("code_backup", {}).get("sha256"):
            raise RolloutError("rollback code version evidence is incomplete")
        head = self.aws["s3"].head_object(
            Bucket=state["code_backup"]["bucket"], Key=state["code_backup"]["key"]
        )
        if head.get("Metadata", {}).get("sha256") != state["code_backup"]["sha256"]:
            raise RolloutError("rollback code backup checksum verification failed")
        self.report.check(
            "lambda_rollback_ready",
            True,
            published_version=state["published_version"],
            aliases=[item["Name"] for item in state.get("aliases", [])],
            backup_sha256=state["code_backup"]["sha256"],
        )

    def activate(self, *, canary_ok: bool, alarms_ok: bool, rollback_ok: bool) -> None:
        if not all((canary_ok, alarms_ok, rollback_ok)):
            self.disable_consumers()
            raise RolloutError("activation gate failed; rules and mappings remain disabled")
        lam = self.aws["lambda"]
        mappings = self.discover_mappings()
        if len(mappings) != 3:
            self.disable_consumers()
            raise RolloutError(f"expected exactly 3 consumer mappings, found {len(mappings)}")
        for uuid in mappings:
            lam.update_event_source_mapping(UUID=uuid, Enabled=True)
        for rule in RULE_NAMES:
            self.aws["events"].enable_rule(Name=rule)
        for uuid in mappings:
            state = lam.get_event_source_mapping(UUID=uuid).get("State")
            if state not in {"Enabled", "Enabling"}:
                self.disable_consumers()
                raise RolloutError(f"mapping {uuid} did not enable")
        for rule in RULE_NAMES:
            if self.aws["events"].describe_rule(Name=rule).get("State") != "ENABLED":
                self.disable_consumers()
                raise RolloutError(f"rule {rule} did not enable")
        self.report.data["activation"] = {
            "rules": "enabled",
            "mappings": "enabled",
            "mapping_uuids": mappings,
        }
        self.report.flush()

    def plan(self) -> None:
        validate_local_artifacts()
        self.verify_target()
        self.discover_canary_candidates()
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        consumer_zip = package_consumers()
        self.report.data["artifacts"] = {
            str(BASE_TEMPLATE.relative_to(REPO_ROOT)): {"sha256": sha256_file(BASE_TEMPLATE)},
            str(CONSUMER_TEMPLATE.relative_to(REPO_ROOT)): {"sha256": sha256_file(CONSUMER_TEMPLATE)},
            str(DENY_POLICY.relative_to(REPO_ROOT)): {"sha256": sha256_file(DENY_POLICY)},
            "consumer-package": {"sha256": sha256_bytes(consumer_zip), "bytes": len(consumer_zip)},
            "main-lambda-package": {
                "dependency_manifest_sha256": sha256_file(REQUIREMENTS_PATH),
                "built_during_apply_only": True,
            },
        }
        self.report.data["plan"] = {
            "base_stack": BASE_STACK,
            "consumer_stack": CONSUMER_STACK,
            "function": config["function_name"],
            "brain_migration": f"s3://{DASHBOARD_BUCKET}/{BRAIN_KEY} -> private canonical bucket",
            "dashboard_policy": "merge reviewed Sids; preserve unrelated statements",
            "initial_state": "all EventBridge rules and SQS mappings disabled",
            "activation_gate": ["live canary", "DLQ/error alarms OK", "rollback evidence"],
            "approval_secret": self.approval_parameter or self.approval_secret_id,
            "canary_input": str(self.canary_input_path.relative_to(REPO_ROOT))
            if self.canary_input_path.is_relative_to(REPO_ROOT)
            else str(self.canary_input_path),
        }
        self.report.flush()

    def run_apply(self) -> None:
        self.plan()
        try:
            self.rollback_state = self._get_rollback_secret()
        except ClientError as exc:
            if error_code(exc) != "ResourceNotFoundException":
                raise
        self.disable_consumers()
        self.report.step("fail_closed_initial_state", "PASS")

        self.deploy_stack(BASE_STACK, BASE_TEMPLATE)
        self.outputs = self.stack_outputs(BASE_STACK)
        required_outputs = {
            "DedicatedLambdaRoleArn",
            "CanonicalBrainBucketName",
            "PredictionLedgerArchiveBucketName",
            "PredictionLedgerTableName",
            "EventBusName",
            "ModelPackageGroupName",
            "DataKeyArn",
            "RequiredLambdaEnvironment",
        }
        missing = required_outputs - set(self.outputs)
        if missing:
            raise RolloutError(f"base stack outputs are incomplete: {', '.join(sorted(missing))}")
        self.checkpoint("base_stack", template_sha256=sha256_file(BASE_TEMPLATE))
        self.report.step("base_stack", "PASS")

        consumer_zip = package_consumers()
        consumer_artifact = self.immutable_upload(
            PRIVATE_ARTIFACT_BUCKET,
            "ops/justhodl-ai/consumers",
            consumer_zip,
            content_type="application/zip",
            kms_key_arn=self.outputs["DataKeyArn"],
        )
        self.report.data["artifacts"]["consumer-package"].update(consumer_artifact)
        self.deploy_stack(
            CONSUMER_STACK,
            CONSUMER_TEMPLATE,
            parameters=self.consumer_parameters(consumer_artifact),
        )
        self.disable_consumers()
        self.checkpoint("consumers_staged", artifact_sha256=consumer_artifact["sha256"])
        self.report.step("consumers_staged_disabled", "PASS")

        brain = self.migrate_brain(
            self.outputs["CanonicalBrainBucketName"], self.outputs["DataKeyArn"]
        )
        self.checkpoint("brain_migrated", **brain)
        self.report.step("canonical_brain_migrated_verified", "PASS", **brain)

        approval_token = self.approval_token()
        main_zip = package_main_lambda()
        main_artifact = self.immutable_upload(
            PRIVATE_ARTIFACT_BUCKET,
            "ops/justhodl-ai/main",
            main_zip,
            content_type="application/zip",
            kms_key_arn=self.outputs["DataKeyArn"],
        )
        self.report.data["artifacts"]["main-lambda-package"] = {
            **main_artifact,
            "bytes": len(main_zip),
            "dependency_manifest_sha256": sha256_file(REQUIREMENTS_PATH),
        }
        self.capture_lambda_rollback()
        self.rollout_lambda(main_artifact, approval_token)
        self.checkpoint("lambda_cutover", artifact_sha256=main_artifact["sha256"])
        self.report.step("lambda_code_role_environment", "PASS")

        self.read_only_checks(self.outputs["CanonicalBrainBucketName"], brain)
        self.report.step("read_only_checks", "PASS")

        if self.completed("live_sagemaker_canary"):
            previous = self.state.get("evidence", {}).get("live_sagemaker_canary", {})
            if previous.get("status") != "PROMOTED":
                raise RolloutError("saved live canary checkpoint is not promotable")
            context = self.load_canary_input()
            endpoint = self.aws["sagemaker"].describe_endpoint(
                EndpointName=context["endpoint_name"]
            )
            if (
                endpoint.get("EndpointStatus") == "InService"
                and endpoint.get("EndpointConfigName") == context["green_endpoint_config"]
            ):
                self.canary_context = context
                self.canary_promoted = True
                self.report.step("live_sagemaker_canary", "PASS", resumed=True)
            else:
                self.state["completed"].remove("live_sagemaker_canary")
                canary = self.run_live_canary(approval_token)
                self.checkpoint("live_sagemaker_canary", status=canary.get("status"))
                self.report.step("live_sagemaker_canary", "PASS", resumed_after_revalidation=True)
        else:
            canary = self.run_live_canary(approval_token)
            self.checkpoint("live_sagemaker_canary", status=canary.get("status"))
            self.report.step("live_sagemaker_canary", "PASS")

        self.ensure_alarms()
        self.report.step("dlq_and_alarm_checks", "PASS")

        self.verify_rollback_check()
        self.report.step("rollback_readiness", "PASS")

        policy = self.merge_dashboard_policy()
        self.checkpoint("dashboard_brain_denied", **policy)
        self.report.step("dashboard_brain_policy_merge", "PASS", **policy)

        self.activate(canary_ok=True, alarms_ok=True, rollback_ok=True)
        self.checkpoint("activated")
        self.report.step("event_consumers_activated", "PASS")

        lam = self.aws["lambda"]
        deployed_sha = lam.get_function_configuration(
            FunctionName=FUNCTION_NAME
        )["CodeSha256"]
        try:
            published = lam.publish_version(
                FunctionName=FUNCTION_NAME,
                CodeSha256=deployed_sha,
                Description=f"Approved JustHodl AI rollout {self.report.data['run_id']}",
            )
            deployed_version = published.get("Version")
        except ClientError as exc:
            if error_code(exc) != "ResourceConflictException":
                raise
            deployed_version = next(
                (
                    item["Version"]
                    for item in reversed(
                        lam.list_versions_by_function(
                            FunctionName=FUNCTION_NAME
                        ).get("Versions", [])
                    )
                    if item.get("Version") != "$LATEST"
                    and item.get("CodeSha256") == deployed_sha
                ),
                None,
            )
            if not deployed_version:
                raise RolloutError("deployed Lambda version could not be persisted")
        self.report.data["deployed_lambda_version"] = deployed_version
        self.report.flush()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="read-only plan (the default)")
    parser.add_argument("--apply", action="store_true", help="apply the reviewed rollout")
    parser.add_argument("--approval-phrase", default="", help="exact human approval phrase")
    parser.add_argument("--canary-input", type=Path, default=DEFAULT_CANARY_INPUT)
    parser.add_argument(
        "--approval-secret-id",
        default="justhodl-ai/prod/canary-approval-token",
        help="Secrets Manager secret containing the canary token",
    )
    parser.add_argument(
        "--approval-parameter",
        help="use this SSM SecureString parameter instead of Secrets Manager",
    )
    args = parser.parse_args(argv)
    if args.plan and args.apply:
        parser.error("--plan and --apply are mutually exclusive")
    if args.apply and args.approval_phrase != APPROVAL_PHRASE:
        parser.error(f"--apply requires --approval-phrase {APPROVAL_PHRASE!r}")
    if not args.apply:
        args.plan = True
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    run_id = sha256_bytes(
        canonical_json(
            {
                "base": sha256_file(BASE_TEMPLATE),
                "consumers": sha256_file(CONSUMER_TEMPLATE),
                "config": sha256_file(CONFIG_PATH),
                "requirements": sha256_file(REQUIREMENTS_PATH),
                "dashboard_deny": sha256_file(DENY_POLICY),
                "main_source": {
                    name: sha256_bytes(data)
                    for name, data in source_entries(AI_ROOT / "source").items()
                },
            }
        )
    )[:20]
    reporter = Reporter("apply" if args.apply else "plan", run_id)
    rollout = Rollout(
        AwsClients(),
        reporter,
        apply=args.apply,
        canary_input=args.canary_input,
        approval_secret_id=args.approval_secret_id,
        approval_parameter=args.approval_parameter,
    )
    try:
        if args.apply:
            rollout.run_apply()
            reporter.finish("SUCCEEDED")
        else:
            rollout.plan()
            reporter.finish("PLANNED")
        print(json.dumps({"status": reporter.data["status"], "report": str(reporter.path.relative_to(REPO_ROOT))}))
        return 0
    except BaseException as exc:
        rollback_errors = []
        if args.apply:
            try:
                rollout.disable_consumers()
            except BaseException as disable_exc:
                rollback_errors.append(f"disable failed: {error_message(disable_exc)[:300]}")
            try:
                rollout.rollback_sagemaker_canary()
            except BaseException as sagemaker_rollback_exc:
                rollback_errors.append(
                    "SageMaker rollback failed: "
                    f"{error_message(sagemaker_rollback_exc)[:300]}"
                )
            try:
                rollout.rollback_lambda()
            except BaseException as rollback_exc:
                rollback_errors.append(f"Lambda rollback failed: {error_message(rollback_exc)[:300]}")
        if rollback_errors:
            reporter.data["rollback_errors"] = rollback_errors
        reporter.finish("FAILED", exc)
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__, "report": str(reporter.path.relative_to(REPO_ROOT))}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
