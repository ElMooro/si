#!/usr/bin/env python3
"""Offline validation and deterministic change planning for JustHodl AI gates.

This module intentionally imports only the Python standard library.  It reads
repository artifacts and never creates an AWS SDK client or contacts AWS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


IAM_DIR = Path(__file__).resolve().parent
REPO_ROOT = IAM_DIR.parents[3]
TEMPLATE_PATH = IAM_DIR / "production-gates.template.json"
LAMBDA_POLICY_PATH = IAM_DIR / "lambda-control-policy.json"
LAMBDA_POLICY_PATHS = {
    "justhodl-ai-control-core": LAMBDA_POLICY_PATH,
    "justhodl-ai-training-deployment-control": IAM_DIR
    / "lambda-training-deployment-policy.json",
    "justhodl-ai-feature-store-access": IAM_DIR / "lambda-feature-store-policy.json",
    "justhodl-ai-runtime-data": IAM_DIR / "lambda-runtime-data-policy.json",
    "justhodl-ai-brain-signal-boundary": IAM_DIR
    / "lambda-brain-signal-policy.json",
    "justhodl-ai-ledger-archive": IAM_DIR / "lambda-ledger-archive-policy.json",
}
LAMBDA_MANAGED_POLICY_IDS = {
    "justhodl-ai-control-core": "LambdaControlCorePolicy",
    "justhodl-ai-training-deployment-control": "LambdaTrainingDeploymentPolicy",
    "justhodl-ai-feature-store-access": "LambdaFeatureStoreAccessPolicy",
    "justhodl-ai-runtime-data": "LambdaRuntimeDataPolicy",
    "justhodl-ai-brain-signal-boundary": "LambdaBrainSignalBoundaryPolicy",
    "justhodl-ai-ledger-archive": "LambdaLedgerArchivePolicy",
}
IAM_MANAGED_POLICY_DOCUMENT_LIMIT = 6_144
IAM_ROLE_INLINE_POLICY_AGGREGATE_LIMIT = 10_240
IAM_ROLE_MANAGED_POLICY_ATTACHMENT_LIMIT = 10
LAMBDA_TRUST_PATH = IAM_DIR / "lambda-execution-role-trust.json"
SAGEMAKER_POLICY_PATH = IAM_DIR / "sagemaker-execution-role-inline.json"
SAGEMAKER_TRUST_PATH = IAM_DIR / "sagemaker-execution-role-trust.json"
DASHBOARD_DENY_PATH = IAM_DIR / "dashboard-brain-deny-policy.json"
FUNCTION_CONFIG_PATH = IAM_DIR.parent / "config.json"

ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
SHARED_ROLE_SUFFIX = ":role/lambda-execution-role"
DEDICATED_LAMBDA_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/justhodl-ai-lambda-prod"
DEDICATED_SAGEMAKER_ROLE = (
    f"arn:aws:iam::{ACCOUNT_ID}:role/justhodl-ai-sagemaker-prod"
)
CANONICAL_BRAIN_BUCKET = f"justhodl-ai-brain-{ACCOUNT_ID}-{REGION}"
FEATURE_STORE_BUCKET = f"justhodl-ai-feature-store-{ACCOUNT_ID}-{REGION}"
LEDGER_ARCHIVE_BUCKET = f"justhodl-ai-prediction-ledger-{ACCOUNT_ID}-{REGION}"
MLFLOW_ARTIFACT_BUCKET = f"justhodl-ai-mlflow-{ACCOUNT_ID}-{REGION}"
LEDGER_TABLE = "justhodl-ai-prediction-ledger-prod"
EVENT_BUS = "justhodl-ai-prod"
MODEL_PACKAGE_GROUP = "justhodl-ai-prod"
MLFLOW_TRACKING_SERVER = "justhodl-ai-prod"
MLFLOW_TRACKING_SERVER_ARN = (
    f"arn:aws:sagemaker:{REGION}:{ACCOUNT_ID}:"
    f"mlflow-tracking-server/{MLFLOW_TRACKING_SERVER}"
)
MLFLOW_TRACKING_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/justhodl-ai-mlflow-prod"
MLFLOW_CLIENT_ACTIONS = {
    "sagemaker-mlflow:CreateExperiment",
    "sagemaker-mlflow:CreateRun",
    "sagemaker-mlflow:GetExperiment",
    "sagemaker-mlflow:GetExperimentByName",
    "sagemaker-mlflow:GetMetricHistory",
    "sagemaker-mlflow:GetRun",
    "sagemaker-mlflow:ListArtifacts",
    "sagemaker-mlflow:LogBatch",
    "sagemaker-mlflow:LogInputs",
    "sagemaker-mlflow:LogMetric",
    "sagemaker-mlflow:LogModel",
    "sagemaker-mlflow:LogParam",
    "sagemaker-mlflow:SearchExperiments",
    "sagemaker-mlflow:SearchRuns",
    "sagemaker-mlflow:SetExperimentTag",
    "sagemaker-mlflow:SetTag",
    "sagemaker-mlflow:UpdateRun",
}
ALLOW_RESOURCE_STAR_ACTIONS = {
    "ce:GetCostAndUsage",
    "cloudwatch:GetMetricData",
    "cloudwatch:GetMetricStatistics",
    "cloudwatch:PutMetricData",
    "ecr:GetAuthorizationToken",
    "pricing:GetProducts",
    "sagemaker:DescribeHubContent",
    "sagemaker:ListApps",
    "sagemaker:ListAutoMLJobs",
    "sagemaker:ListClusters",
    "sagemaker:ListDomains",
    "sagemaker:ListEndpointConfigs",
    "sagemaker:ListEndpoints",
    "sagemaker:ListFeatureGroups",
    "sagemaker:ListHubContents",
    "sagemaker:ListModels",
    "sagemaker:ListNotebookInstances",
    "sagemaker:ListTrainingJobs",
}


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def statements(policy: dict[str, Any]) -> list[dict[str, Any]]:
    return [s for s in as_list(policy.get("Statement", [])) if isinstance(s, dict)]


def statement_by_sid(policy: dict[str, Any], sid: str) -> dict[str, Any] | None:
    return next((s for s in statements(policy) if s.get("Sid") == sid), None)


def actions(policy: dict[str, Any], effect: str | None = None) -> set[str]:
    found: set[str] = set()
    for statement in statements(policy):
        if effect is not None and statement.get("Effect") != effect:
            continue
        found.update(str(action) for action in as_list(statement.get("Action", [])))
    return found


def resources_for_action(policy: dict[str, Any], action: str) -> list[Any]:
    found: list[Any] = []
    for statement in statements(policy):
        if statement.get("Effect") != "Allow":
            continue
        if action in as_list(statement.get("Action", [])):
            found.extend(as_list(statement.get("Resource", [])))
    return found


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(condition: Any, name: str, ok: str, fail: str) -> Check:
    passed = bool(condition)
    return Check(name=name, passed=passed, detail=ok if passed else fail)


def _role_policy(template: dict[str, Any], logical_id: str, policy_name: str) -> dict[str, Any]:
    policies = (
        template["Resources"][logical_id]["Properties"].get("Policies") or []
    )
    for policy in policies:
        if policy.get("PolicyName") == policy_name:
            return policy["PolicyDocument"]
    raise KeyError(f"{logical_id} has no inline policy {policy_name}")


def _managed_policy(template: dict[str, Any], logical_id: str) -> dict[str, Any]:
    resource = template["Resources"][logical_id]
    if resource.get("Type") != "AWS::IAM::ManagedPolicy":
        raise KeyError(f"{logical_id} is not an AWS::IAM::ManagedPolicy")
    return resource["Properties"]["PolicyDocument"]


def _compact_policy_size(policy: dict[str, Any]) -> int:
    """Return IAM policy size with insignificant JSON whitespace removed."""

    return len(json.dumps(policy, separators=(",", ":")))


def _bucket_checks(
    template: dict[str, Any],
    logical_id: str,
    *,
    object_lock: bool = False,
    eventing: bool = True,
) -> list[Check]:
    props = template["Resources"][logical_id]["Properties"]
    pab = props.get("PublicAccessBlockConfiguration") or {}
    encryption = (
        props.get("BucketEncryption", {})
        .get("ServerSideEncryptionConfiguration", [{}])[0]
        .get("ServerSideEncryptionByDefault", {})
    )
    result = [
        check(
            all(
                pab.get(key) is True
                for key in (
                    "BlockPublicAcls",
                    "BlockPublicPolicy",
                    "IgnorePublicAcls",
                    "RestrictPublicBuckets",
                )
            ),
            f"{logical_id}.public_access_block",
            "all four public-access-block controls are enabled",
            "one or more public-access-block controls are missing",
        ),
        check(
            encryption.get("SSEAlgorithm") == "aws:kms"
            and encryption.get("KMSMasterKeyID"),
            f"{logical_id}.encryption",
            "default SSE-KMS encryption is configured",
            "default SSE-KMS encryption is missing",
        ),
        check(
            props.get("VersioningConfiguration", {}).get("Status") == "Enabled",
            f"{logical_id}.versioning",
            "versioning is enabled",
            "versioning is not enabled",
        ),
        check(
            bool(props.get("LifecycleConfiguration", {}).get("Rules")),
            f"{logical_id}.lifecycle",
            "at least one lifecycle rule is defined",
            "no lifecycle rule is defined",
        ),
    ]
    if eventing:
        result.append(
            check(
                props.get("NotificationConfiguration", {})
                .get("EventBridgeConfiguration", {})
                .get("EventBridgeEnabled")
                is True,
                f"{logical_id}.eventing",
                "S3 events are enabled for EventBridge",
                "S3 EventBridge notifications are not enabled",
            )
        )
    if object_lock:
        retention = (
            props.get("ObjectLockConfiguration", {})
            .get("Rule", {})
            .get("DefaultRetention", {})
        )
        result.extend(
            [
                check(
                    props.get("ObjectLockEnabled") is True,
                    f"{logical_id}.object_lock",
                    "object lock is enabled at bucket creation",
                    "object lock is not enabled",
                ),
                check(
                    retention.get("Mode") == "COMPLIANCE"
                    and int(retention.get("Years", 0)) >= 7,
                    f"{logical_id}.retention",
                    "WORM retention is COMPLIANCE mode for at least seven years",
                    "COMPLIANCE retention of at least seven years is missing",
                ),
            ]
        )
    return result


def validate() -> list[Check]:
    template = load_json(TEMPLATE_PATH)
    lambda_policies = {
        name: load_json(path) for name, path in LAMBDA_POLICY_PATHS.items()
    }
    lambda_policy = lambda_policies["justhodl-ai-control-core"]
    training_policy = lambda_policies["justhodl-ai-training-deployment-control"]
    feature_policy = lambda_policies["justhodl-ai-feature-store-access"]
    runtime_policy = lambda_policies["justhodl-ai-runtime-data"]
    brain_policy = lambda_policies["justhodl-ai-brain-signal-boundary"]
    ledger_policy = lambda_policies["justhodl-ai-ledger-archive"]
    lambda_trust = load_json(LAMBDA_TRUST_PATH)
    sm_policy = load_json(SAGEMAKER_POLICY_PATH)
    sm_trust = load_json(SAGEMAKER_TRUST_PATH)
    dashboard_policy = load_json(DASHBOARD_DENY_PATH)
    resources = template.get("Resources", {})
    mlflow_policy = _role_policy(
        template, "MlflowTrackingRole", "justhodl-ai-mlflow-artifacts"
    )
    feature_store_role_policy = _role_policy(
        template, "FeatureStoreExecutionRole", "justhodl-ai-feature-store-offline"
    )
    mlflow_trust = resources["MlflowTrackingRole"]["Properties"][
        "AssumeRolePolicyDocument"
    ]
    results: list[Check] = []

    for policy_name, policy in lambda_policies.items():
        logical_id = LAMBDA_MANAGED_POLICY_IDS[policy_name]
        results.append(
            check(
                _managed_policy(template, logical_id) == policy,
                f"iam.lambda_policy_template_parity.{policy_name}",
                f"{policy_name} standalone and CloudFormation managed policies are identical",
                f"{policy_name} standalone and CloudFormation managed policies have drifted",
            )
        )
    results.append(
        check(
            template["Resources"]["LambdaExecutionRole"]["Properties"][
                "AssumeRolePolicyDocument"
            ]
            == lambda_trust,
            "iam.lambda_trust_template_parity",
            "standalone and CloudFormation Lambda trust policies are identical",
            "standalone and CloudFormation Lambda trust policies have drifted",
        )
    )
    results.append(
        check(
            _role_policy(
                template, "SageMakerExecutionRole", "justhodl-ai-sagemaker-data-plane"
            )
            == sm_policy,
            "iam.sagemaker_policy_template_parity",
            "standalone and CloudFormation SageMaker policies are identical",
            "standalone and CloudFormation SageMaker policies have drifted",
        )
    )
    results.append(
        check(
            template["Resources"]["SageMakerExecutionRole"]["Properties"][
                "AssumeRolePolicyDocument"
            ]
            == sm_trust,
            "iam.sagemaker_trust_template_parity",
            "standalone and CloudFormation SageMaker trust policies are identical",
            "standalone and CloudFormation SageMaker trust policies have drifted",
        )
    )

    lambda_role = resources.get("LambdaExecutionRole", {}).get("Properties", {})
    sm_role = resources.get("SageMakerExecutionRole", {}).get("Properties", {})
    feature_store_role = resources.get("FeatureStoreExecutionRole", {}).get(
        "Properties", {}
    )
    mlflow_role = resources.get("MlflowTrackingRole", {}).get("Properties", {})
    results.extend(
        [
            check(
                lambda_role.get("ManagedPolicyArns")
                == [
                    {"Ref": logical_id}
                    for logical_id in LAMBDA_MANAGED_POLICY_IDS.values()
                ],
                "iam.lambda_managed_policy_attachments",
                "the Lambda role attaches exactly the six domain-separated managed policies",
                "the Lambda managed-policy attachments are missing, reordered, or include an unexpected policy",
            ),
            check(
                len(lambda_role.get("ManagedPolicyArns") or [])
                <= IAM_ROLE_MANAGED_POLICY_ATTACHMENT_LIMIT,
                "iam.lambda_managed_policy_attachment_quota",
                "managed-policy attachments stay within the IAM role quota",
                "managed-policy attachments exceed the IAM role quota",
            ),
            check(
                all(
                    _compact_policy_size(policy)
                    <= IAM_MANAGED_POLICY_DOCUMENT_LIMIT
                    for policy in lambda_policies.values()
                ),
                "iam.lambda_managed_policy_document_quotas",
                "each domain policy stays within the IAM managed-policy document quota",
                "one or more domain policies exceed the IAM managed-policy document quota",
            ),
            check(
                sum(
                    _compact_policy_size(item.get("PolicyDocument") or {})
                    for item in (lambda_role.get("Policies") or [])
                )
                <= IAM_ROLE_INLINE_POLICY_AGGREGATE_LIMIT,
                "iam.lambda_inline_policy_aggregate_quota",
                "remaining inline role policies stay within the IAM aggregate quota",
                "remaining inline role policies exceed the IAM aggregate quota",
            ),
            check(
                lambda_role.get("RoleName") == "justhodl-ai-lambda-prod",
                "iam.dedicated_lambda_role",
                "the stack creates a role dedicated to justhodl-ai",
                "dedicated Lambda role name is missing or unexpected",
            ),
            check(
                "lambda.amazonaws.com"
                in json.dumps(lambda_trust.get("Statement", []), sort_keys=True)
                and len(statements(lambda_trust)) == 1,
                "iam.lambda_trust_scope",
                "Lambda trust permits only the Lambda service; assignment is to the dedicated role",
                "Lambda trust permits an unexpected principal",
            ),
            check(
                sm_role.get("RoleName") == "justhodl-ai-sagemaker-prod",
                "iam.dedicated_sagemaker_role",
                "the stack creates a dedicated SageMaker execution role",
                "dedicated SageMaker role name is missing or unexpected",
            ),
            check(
                feature_store_role.get("RoleName")
                == "justhodl-ai-feature-store-prod",
                "iam.dedicated_feature_store_role",
                "the stack creates a dedicated Feature Store service role",
                "dedicated Feature Store role name is missing or unexpected",
            ),
            check(
                len(
                    statements(
                        feature_store_role.get("AssumeRolePolicyDocument", {})
                    )
                )
                == 1
                and "sagemaker.amazonaws.com"
                in json.dumps(
                    feature_store_role.get("AssumeRolePolicyDocument", {}),
                    sort_keys=True,
                )
                and "feature-group/justhodl-ai-*"
                in json.dumps(
                    feature_store_role.get("AssumeRolePolicyDocument", {}),
                    sort_keys=True,
                ),
                "iam.feature_store_trust_scope",
                "Feature Store trust is bound to governed feature groups",
                "Feature Store trust is not sufficiently scoped",
            ),
            check(
                "sagemaker.amazonaws.com"
                in json.dumps(sm_trust.get("Statement", []), sort_keys=True)
                and ACCOUNT_ID in json.dumps(sm_trust, sort_keys=True),
                "iam.sagemaker_trust_scope",
                "SageMaker trust is bound to the target account",
                "SageMaker trust is not sufficiently scoped",
            ),
            check(
                mlflow_role.get("RoleName") == "justhodl-ai-mlflow-prod",
                "iam.dedicated_mlflow_role",
                "the stack creates a dedicated MLflow tracking-server role",
                "dedicated MLflow role name is missing or unexpected",
            ),
            check(
                resources["MlflowTrackingRole"].get("DeletionPolicy") == "Retain"
                and resources["MlflowTrackingRole"].get("UpdateReplacePolicy")
                == "Retain",
                "iam.mlflow_role_retention",
                "the role required by the retained MLflow server is also retained",
                "stack deletion or replacement could orphan the retained MLflow server",
            ),
            check(
                len(statements(mlflow_trust)) == 1
                and "sagemaker.amazonaws.com"
                in json.dumps(mlflow_trust, sort_keys=True)
                and ACCOUNT_ID in json.dumps(mlflow_trust, sort_keys=True)
                and MLFLOW_TRACKING_SERVER_ARN
                in json.dumps(mlflow_trust, sort_keys=True),
                "iam.mlflow_trust_scope",
                "MLflow trust is bound to its named server in the target account",
                "MLflow trust is not bound to the named tracking server",
            ),
        ]
    )

    all_policies = [
        *lambda_policies.values(),
        sm_policy,
        feature_store_role_policy,
        mlflow_policy,
    ]
    allow_actions = set().union(*(actions(policy, "Allow") for policy in all_policies))
    star_resource_actions = set()
    for policy in all_policies:
        for statement in statements(policy):
            if statement.get("Effect") != "Allow":
                continue
            if "*" in as_list(statement.get("Resource", [])):
                star_resource_actions.update(as_list(statement.get("Action", [])))
    results.extend(
        [
            check(
                "sagemaker:*" not in allow_actions,
                "iam.no_sagemaker_wildcard",
                "no Allow statement grants sagemaker:*",
                "an Allow statement grants sagemaker:*",
            ),
            check(
                "*" not in allow_actions,
                "iam.no_global_action_wildcard",
                "no Allow statement grants Action '*'",
                "an Allow statement grants Action '*'",
            ),
            check(
                "AmazonSageMakerFullAccess"
                not in json.dumps(template, sort_keys=True)
                and "AmazonSageMakerFullAccess"
                not in SAGEMAKER_POLICY_PATH.read_text(encoding="utf-8"),
                "iam.no_managed_full_access",
                "AmazonSageMakerFullAccess is absent",
                "AmazonSageMakerFullAccess is present",
            ),
            check(
                star_resource_actions <= ALLOW_RESOURCE_STAR_ACTIONS,
                "iam.resource_star_allowlist",
                "Resource '*' is limited to explicitly reviewed non-resource APIs",
                "unexpected Resource '*' actions: "
                + ", ".join(sorted(star_resource_actions - ALLOW_RESOURCE_STAR_ACTIONS)),
            ),
        ]
    )

    pass_role = (
        statement_by_sid(training_policy, "PassOnlyDedicatedSageMakerRole") or {}
    )
    results.append(
        check(
            pass_role.get("Resource") == DEDICATED_SAGEMAKER_ROLE
            and pass_role.get("Condition", {})
            .get("StringEquals", {})
            .get("iam:PassedToService")
            == "sagemaker.amazonaws.com",
            "iam.pass_role",
            "PassRole is limited to the dedicated role and SageMaker service",
            "PassRole scope or service condition is incorrect",
        )
    )

    create = statement_by_sid(training_policy, "CreateTaggedSageMakerResources") or {}
    mutate = statement_by_sid(training_policy, "MutateTaggedSageMakerResources") or {}
    results.extend(
        [
            check(
                create.get("Condition", {})
                .get("StringEquals", {})
                .get("aws:RequestTag/justhodl-ai-managed")
                == "true"
                and all(
                    isinstance(resource, str) and "/jh-ai-*" in resource
                    for resource in as_list(create.get("Resource", []))
                ),
                "iam.create_scope",
                "SageMaker creates require ownership tags and jh-ai-* names",
                "SageMaker create scope lacks tags or resource prefixes",
            ),
            check(
                mutate.get("Condition", {})
                .get("StringEquals", {})
                .get("aws:ResourceTag/justhodl-ai-managed")
                == "true"
                and all(
                    isinstance(resource, str) and "/jh-ai-*" in resource
                    for resource in as_list(mutate.get("Resource", []))
                ),
                "iam.mutation_scope",
                "SageMaker mutations require ownership tags and jh-ai-* names",
                "SageMaker mutation scope lacks tags or resource prefixes",
            ),
        ]
    )

    sm_s3_resources = [
        str(resource)
        for action in ("s3:GetObject", "s3:PutObject", "s3:ListBucket")
        for resource in resources_for_action(sm_policy, action)
    ]
    results.append(
        check(
            not any("justhodl-dashboard-live" in resource for resource in sm_s3_resources),
            "iam.sagemaker_no_dashboard_access",
            "SageMaker execution has no dashboard-bucket access",
            "SageMaker execution can access the public dashboard bucket",
        )
    )

    brain_read = statement_by_sid(brain_policy, "ReadCanonicalBrainObject") or {}
    brain_list = statement_by_sid(brain_policy, "ListCanonicalBrainObject") or {}
    signal_archive = (
        statement_by_sid(brain_policy, "ReadWriteGovernedSignalArchive") or {}
    )
    signal_outbox = (
        statement_by_sid(brain_policy, "ReadWriteGovernedSignalOutbox") or {}
    )
    canonical_bucket_policy = resources["CanonicalBrainBucketPolicy"]["Properties"][
        "PolicyDocument"
    ]
    canonical_mutation_deny = (
        statement_by_sid(
            canonical_bucket_policy, "DenyLambdaCanonicalBrainMutation"
        )
        or {}
    )
    canonical_put_resources = [
        str(resource)
        for policy in lambda_policies.values()
        for resource in resources_for_action(policy, "s3:PutObject")
        if CANONICAL_BRAIN_BUCKET in str(resource)
    ]
    results.extend(
        [
            check(
                set(as_list(brain_read.get("Action", [])))
                == {"s3:GetObject", "s3:GetObjectVersion"}
                and brain_read.get("Resource")
                == f"arn:aws:s3:::{CANONICAL_BRAIN_BUCKET}/data/brain.json"
                and "s3:PutObject" not in as_list(brain_read.get("Action", [])),
                "iam.canonical_brain_read_only",
                "Lambda canonical Brain access is read-only and limited to data/brain.json",
                "Lambda canonical Brain access is writable or broader than data/brain.json",
            ),
            check(
                brain_list.get("Condition", {})
                .get("StringLike", {})
                .get("s3:prefix")
                == ["data/brain.json"],
                "iam.canonical_brain_list_scope",
                "canonical Brain listing is limited to the exact reviewed object",
                "canonical Brain bucket listing is broader than the reviewed object",
            ),
            check(
                set(canonical_put_resources)
                == {
                    f"arn:aws:s3:::{CANONICAL_BRAIN_BUCKET}/signals/v1/*"
                }
                and set(as_list(signal_archive.get("Action", [])))
                == {"s3:GetObject", "s3:PutObject"}
                and signal_outbox.get("Resource")
                == f"arn:aws:s3:::justhodl-ai-{ACCOUNT_ID}/ai/signals/outbox/v1/*",
                "iam.governed_signal_write_prefixes",
                "Lambda writes canonical signals and private outbox state only in governed prefixes",
                "Lambda signal/archive write permissions escape governed prefixes",
            ),
            check(
                set(as_list(canonical_mutation_deny.get("Action", [])))
                >= {"s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"}
                and canonical_mutation_deny.get("NotResource")
                == {
                    "Fn::Sub": "${CanonicalBrainBucket.Arn}/signals/v1/*"
                }
                and canonical_mutation_deny.get("Principal")
                == {"AWS": {"Fn::GetAtt": ["LambdaExecutionRole", "Arn"]}},
                "s3.canonical_brain_lambda_mutation_deny",
                "bucket policy denies Lambda canonical-object mutation outside governed signals",
                "canonical Brain bucket lacks the Lambda mutation backstop",
            ),
            check(
                len(lambda_policies) == 6
                and all(
                    len(statements(policy)) > 0
                    for policy in lambda_policies.values()
                ),
                "iam.lambda_domain_policy_separation",
                "Lambda permissions are split across six reviewed domain policies",
                "Lambda permissions are not separated by data/control domain",
            ),
        ]
    )

    sm_brain_reads = [
        str(resource)
        for resource in resources_for_action(sm_policy, "s3:GetObject")
        if CANONICAL_BRAIN_BUCKET in str(resource)
    ]
    sm_brain_writes = [
        str(resource)
        for resource in resources_for_action(sm_policy, "s3:PutObject")
        if CANONICAL_BRAIN_BUCKET in str(resource)
    ]
    sm_feature_objects = [
        str(resource)
        for action in ("s3:GetObject", "s3:PutObject")
        for resource in resources_for_action(sm_policy, action)
        if FEATURE_STORE_BUCKET in str(resource)
    ]
    results.extend(
        [
            check(
                sm_brain_reads
                == [
                    f"arn:aws:s3:::{CANONICAL_BRAIN_BUCKET}/data/brain.json"
                ]
                and not sm_brain_writes,
                "iam.sagemaker_brain_read_only",
                "SageMaker can read only the canonical Brain object and cannot write Brain data",
                "SageMaker Brain permissions are broad or writable",
            ),
            check(
                sm_feature_objects
                and all(resource.endswith("/offline/*") for resource in sm_feature_objects),
                "iam.sagemaker_feature_store_prefix",
                "SageMaker Feature Store object access is limited to offline/*",
                "SageMaker Feature Store object access escapes offline/*",
            ),
            check(
                all(
                    FEATURE_STORE_BUCKET
                    in json.dumps(resource, sort_keys=True)
                    or "FeatureStoreBucket"
                    in json.dumps(resource, sort_keys=True)
                    or str(resource).startswith(
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:"
                    )
                    or "AiDataKey" in json.dumps(resource, sort_keys=True)
                    for statement in statements(feature_store_role_policy)
                    if statement.get("Effect") == "Allow"
                    for resource in as_list(statement.get("Resource", []))
                )
                and not any(
                    CANONICAL_BRAIN_BUCKET
                    in json.dumps(statement.get("Resource", ""), sort_keys=True)
                    or "justhodl-ai-857687956942/ai/"
                    in json.dumps(statement.get("Resource", ""), sort_keys=True)
                    for statement in statements(feature_store_role_policy)
                ),
                "iam.feature_store_role_data_boundary",
                "Feature Store role is isolated from Brain and training/model prefixes",
                "Feature Store role escapes its offline-store, Glue, or KMS boundary",
            ),
        ]
    )
    mlflow_allow_actions = actions(mlflow_policy, "Allow")
    mlflow_kms = statement_by_sid(mlflow_policy, "UseArtifactEncryptionKey") or {}
    mlflow_bucket_policy = resources["MlflowArtifactBucketPolicy"]["Properties"][
        "PolicyDocument"
    ]
    mlflow_bucket_policy_sids = {
        statement.get("Sid") for statement in statements(mlflow_bucket_policy)
    }
    mlflow_s3_resources = [
        resource
        for action in (
            "s3:AbortMultipartUpload",
            "s3:DeleteObject",
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:PutObject",
        )
        for resource in resources_for_action(mlflow_policy, action)
    ]
    results.extend(
        [
            check(
                mlflow_s3_resources
                and all(
                    MLFLOW_ARTIFACT_BUCKET in json.dumps(resource, sort_keys=True)
                    or "MlflowArtifactBucket" in json.dumps(resource, sort_keys=True)
                    for resource in mlflow_s3_resources
                ),
                "iam.mlflow_artifact_scope",
                "MLflow service-role S3 access is limited to its artifact bucket",
                "MLflow service-role S3 access escapes its artifact bucket",
            ),
            check(
                all(
                    "*" not in as_list(statement.get("Resource", []))
                    for statement in statements(mlflow_policy)
                    if statement.get("Effect") == "Allow"
                )
                and "*" not in mlflow_allow_actions,
                "iam.mlflow_no_wildcard_resources",
                "MLflow service-role permissions contain no wildcard resource",
                "MLflow service-role permissions contain a wildcard resource",
            ),
            check(
                not any(
                    action.startswith(("sagemaker:", "sagemaker-mlflow:"))
                    for action in mlflow_allow_actions
                ),
                "iam.mlflow_service_role_no_registry_access",
                "automatic registration is off and the MLflow service role has no registry access",
                "the MLflow service role has unnecessary SageMaker registry permissions",
            ),
            check(
                mlflow_kms.get("Resource") == {"Fn::GetAtt": ["AiDataKey", "Arn"]}
                and mlflow_kms.get("Condition", {})
                .get("StringEquals", {})
                .get("kms:CallerAccount")
                == ACCOUNT_ID
                and mlflow_kms.get("Condition", {})
                .get("StringEquals", {})
                .get("kms:ViaService")
                == f"s3.{REGION}.amazonaws.com",
                "iam.mlflow_kms_scope",
                "MLflow KMS use is limited to the stack key through regional S3",
                "MLflow KMS use is not limited to the stack key through regional S3",
            ),
            check(
                {
                    "DenyInsecureTransport",
                    "DenyCrossAccountAccess",
                    "DenyPublicObjectAcl",
                }.issubset(mlflow_bucket_policy_sids)
                and resources["MlflowArtifactBucketPolicy"].get("DeletionPolicy")
                == "Retain"
                and resources["MlflowArtifactBucketPolicy"].get(
                    "UpdateReplacePolicy"
                )
                == "Retain",
                "s3.mlflow_artifact_policy",
                "the retained artifact policy denies insecure, cross-account, and public-ACL access",
                "the MLflow artifact bucket policy is incomplete or not retained",
            ),
        ]
    )

    dashboard_denies = [
        statement
        for statement in statements(dashboard_policy)
        if statement.get("Effect") == "Deny" and statement.get("Principal") == "*"
    ]
    dashboard_text = json.dumps(dashboard_denies, sort_keys=True)
    dashboard_read_deny = next(
        (
            statement
            for statement in dashboard_denies
            if statement.get("Sid") == "DenyPublicBrainReads"
        ),
        {},
    )
    results.extend(
        [
            check(
                "DenyPublicBrainReads" in dashboard_text
                and "s3:GetObject" in dashboard_text
                and "data/brain.json" in dashboard_text,
                "s3.dashboard_brain_read_deny",
                "public dashboard Brain reads are explicitly denied",
                "public dashboard Brain-read deny is missing",
            ),
            check(
                not dashboard_read_deny.get("Condition"),
                "s3.dashboard_brain_read_deny_unconditional",
                "the dashboard Brain-read deny applies to every principal",
                "the dashboard Brain-read deny has a bypass condition",
            ),
            check(
                "DenyDashboardBrainWrites" in dashboard_text
                and "s3:PutObject" in dashboard_text,
                "s3.dashboard_brain_write_deny",
                "new Brain objects cannot be written to dashboard prefixes",
                "dashboard Brain-write deny is missing",
            ),
        ]
    )

    results.extend(_bucket_checks(template, "CanonicalBrainBucket"))
    results.extend(_bucket_checks(template, "FeatureStoreBucket"))
    results.extend(
        _bucket_checks(template, "MlflowArtifactBucket", eventing=False)
    )
    results.extend(
        _bucket_checks(template, "PredictionLedgerArchiveBucket", object_lock=True)
    )
    results.extend(
        _bucket_checks(
            template, "TrainingInputBucket", object_lock=True, eventing=False
        )
    )

    event_types = {
        logical_id
        for logical_id, resource in resources.items()
        if resource.get("Type") == "AWS::Events::Rule"
    }
    results.extend(
        [
            check(
                resources.get("AiEventBus", {}).get("Type")
                == "AWS::Events::EventBus",
                "eventing.custom_bus",
                "a custom JustHodl AI event bus is defined",
                "custom event bus is missing",
            ),
            check(
                {
                    "SignalEnvelopeIngressRule",
                    "PredictionLedgerIngressRule",
                    "ModelPackageStateRule",
                }.issubset(event_types),
                "eventing.rules",
                "signal, prediction, and model-governance rules are defined",
                "one or more required EventBridge rules are missing",
            ),
            check(
                all(
                    resources[logical_id]["Properties"].get("State") == "DISABLED"
                    for logical_id in event_types
                ),
                "eventing.safe_initial_state",
                "rules are staged disabled until consumers pass canaries",
                "one or more event rules would activate during stack creation",
            ),
            check(
                all(
                    target.get("DeadLetterConfig")
                    for logical_id in event_types
                    for target in resources[logical_id]["Properties"].get("Targets", [])
                ),
                "eventing.dead_letters",
                "every EventBridge target has a dead-letter queue",
                "an EventBridge target lacks a dead-letter queue",
            ),
        ]
    )

    feature_groups = [
        resource
        for resource in resources.values()
        if resource.get("Type") == "AWS::SageMaker::FeatureGroup"
    ]
    results.extend(
        [
            check(
                len(feature_groups) >= 2,
                "feature_store.groups",
                "signal and prediction feature groups are defined",
                "fewer than two feature groups are defined",
            ),
            check(
                feature_groups
                and all(
                    group["Properties"]
                    .get("OnlineStoreConfig", {})
                    .get("EnableOnlineStore")
                    is True
                    and group["Properties"].get("OfflineStoreConfig", {}).get(
                        "S3StorageConfig"
                    )
                    for group in feature_groups
                ),
                "feature_store.online_offline",
                "every feature group has online and offline stores",
                "a feature group lacks an online or offline store",
            ),
            check(
                feature_groups
                and all(
                    {
                        item["FeatureName"]
                        for item in group["Properties"].get("FeatureDefinitions", [])
                    }
                    >= {
                        group["Properties"]["RecordIdentifierFeatureName"],
                        group["Properties"]["EventTimeFeatureName"],
                    }
                    for group in feature_groups
                ),
                "feature_store.keys",
                "record identifiers and event times are declared features",
                "a Feature Store key is not declared as a feature",
            ),
            check(
                feature_groups
                and all(
                    group["Properties"].get("RoleArn")
                    == {"Fn::GetAtt": ["FeatureStoreExecutionRole", "Arn"]}
                    for group in feature_groups
                ),
                "feature_store.dedicated_role",
                "every feature group uses the dedicated Feature Store role",
                "a feature group reuses the training/deployment role",
            ),
        ]
    )

    model_group = resources.get("ModelPackageGroup", {})
    model_group_props = model_group.get("Properties", {})
    tracking_server = resources.get("MlflowTrackingServer", {})
    tracking_props = tracking_server.get("Properties", {})
    sm_mlflow = statement_by_sid(sm_policy, "WriteGovernedMlflowRuns") or {}
    sm_mlflow_control = (
        statement_by_sid(sm_policy, "DescribeGovernedMlflowTrackingServer") or {}
    )
    automated_sagemaker_actions = set().union(
        *(actions(policy, "Allow") for policy in all_policies)
    )
    results.extend(
        [
            check(
                model_group.get("Type") == "AWS::SageMaker::ModelPackageGroup"
                and model_group_props.get("ModelPackageGroupName")
                == MODEL_PACKAGE_GROUP,
                "registry.model_package_group",
                "the governed SageMaker Model Package Group is defined",
                "the governed SageMaker Model Package Group is missing or misnamed",
            ),
            check(
                model_group.get("DeletionPolicy") == "Retain"
                and model_group.get("UpdateReplacePolicy") == "Retain",
                "registry.retention",
                "the model package group is retained on deletion and replacement",
                "the model package group is not deletion-safe",
            ),
            check(
                tracking_server.get("Type")
                == "AWS::SageMaker::MlflowTrackingServer"
                and tracking_props.get("TrackingServerName")
                == MLFLOW_TRACKING_SERVER
                and tracking_props.get("TrackingServerSize") == "Small"
                and tracking_props.get("AutomaticModelRegistration") is False
                and "MlflowArtifactBucket"
                in json.dumps(tracking_props.get("ArtifactStoreUri"), sort_keys=True)
                and tracking_props.get("RoleArn")
                == {"Fn::GetAtt": ["MlflowTrackingRole", "Arn"]},
                "mlflow.tracking_server",
                "a small governed MLflow server uses the dedicated private artifact store",
                "the MLflow server configuration is missing or unsafe",
            ),
            check(
                tracking_server.get("DeletionPolicy") == "Retain"
                and tracking_server.get("UpdateReplacePolicy") == "Retain",
                "mlflow.retention",
                "the MLflow tracking server is retained on deletion and replacement",
                "the MLflow tracking server is not deletion-safe",
            ),
            check(
                "sagemaker:UpdateModelPackage" not in automated_sagemaker_actions
                and tracking_props.get("AutomaticModelRegistration") is False,
                "registry.separate_manual_approval",
                "automated roles cannot approve packages and MLflow auto-registration is disabled",
                "an automated path can update approval or auto-register models",
            ),
            check(
                sm_mlflow.get("Resource") == MLFLOW_TRACKING_SERVER_ARN
                and sm_mlflow_control.get("Resource") == MLFLOW_TRACKING_SERVER_ARN
                and set(as_list(sm_mlflow.get("Action", [])))
                == MLFLOW_CLIENT_ACTIONS
                and set(as_list(sm_mlflow_control.get("Action", [])))
                == {"sagemaker:DescribeMlflowTrackingServer"},
                "iam.mlflow_client_scope",
                "training clients can write governed runs only on the named server without delete access",
                "MLflow client access is broad or points at the wrong server",
            ),
        ]
    )

    table = resources.get("PredictionLedgerTable", {}).get("Properties", {})
    ledger_actions = set()
    for statement in statements(ledger_policy):
        statement_resources = json.dumps(statement.get("Resource", ""))
        if f"table/{LEDGER_TABLE}" in statement_resources:
            ledger_actions.update(as_list(statement.get("Action", [])))
    results.extend(
        [
            check(
                table.get("DeletionProtectionEnabled") is True
                and table.get("PointInTimeRecoverySpecification", {}).get(
                    "PointInTimeRecoveryEnabled"
                )
                is True
                and table.get("SSESpecification", {}).get("SSEEnabled") is True,
                "ledger.index_protection",
                "ledger index has deletion protection, PITR, and KMS encryption",
                "ledger index protection is incomplete",
            ),
            check(
                "dynamodb:UpdateItem" not in ledger_actions
                and "dynamodb:DeleteItem" not in ledger_actions
                and "dynamodb:BatchWriteItem" not in ledger_actions,
                "ledger.append_only_writer",
                "the new ledger writer cannot update or delete index rows",
                "the new ledger writer can mutate or delete index rows",
            ),
        ]
    )

    results.append(
        check(
            not any(
                resource.get("Type") == "AWS::Lambda::Function"
                for resource in resources.values()
            ),
            "safety.no_lambda_mutation",
            "the stack does not mutate or replace the existing Lambda",
            "the stack contains a Lambda function mutation",
        )
    )
    outputs = template.get("Outputs", {})
    required_environment = json.loads(
        outputs.get("RequiredLambdaEnvironment", {})
        .get("Value", {})
        .get("Fn::Sub", "{}")
    )
    config_environment = load_json(FUNCTION_CONFIG_PATH).get("env", {})
    results.extend(
        [
            check(
                outputs.get("ModelPackageGroupArn", {}).get("Value")
                == {"Fn::GetAtt": ["ModelPackageGroup", "ModelPackageGroupArn"]}
                and outputs.get("MlflowTrackingServerArn", {}).get("Value")
                == {
                    "Fn::GetAtt": [
                        "MlflowTrackingServer",
                        "TrackingServerArn",
                    ]
                }
                and outputs.get("MlflowArtifactBucketName", {}).get("Value")
                == {"Ref": "MlflowArtifactBucket"},
                "outputs.registry_mlflow",
                "registry, tracking-server, and artifact-bucket outputs are defined",
                "one or more registry/MLflow outputs are missing",
            ),
            check(
                required_environment.get("MODEL_PACKAGE_GROUP")
                == MODEL_PACKAGE_GROUP
                and required_environment.get("MLFLOW_TRACKING_SERVER_NAME")
                == "${MlflowTrackingServer}"
                and required_environment.get("MLFLOW_TRACKING_SERVER_ARN")
                == "${MlflowTrackingServer.TrackingServerArn}"
                and required_environment.get("MLFLOW_EXPERIMENT_NAME")
                == MLFLOW_TRACKING_SERVER,
                "outputs.registry_mlflow_environment",
                "the stack emits all registry and MLflow Lambda bindings",
                "registry or MLflow Lambda output bindings are incomplete",
            ),
            check(
                config_environment.get("MODEL_PACKAGE_GROUP")
                == MODEL_PACKAGE_GROUP
                and config_environment.get("MLFLOW_TRACKING_SERVER_NAME")
                == MLFLOW_TRACKING_SERVER
                and config_environment.get("MLFLOW_TRACKING_SERVER_ARN")
                == MLFLOW_TRACKING_SERVER_ARN
                and config_environment.get("MLFLOW_EXPERIMENT_NAME")
                == MLFLOW_TRACKING_SERVER,
                "config.registry_mlflow_environment",
                "repository Lambda config matches the registry and MLflow resources",
                "repository Lambda config does not match registry/MLflow resources",
            ),
        ]
    )
    return results


def change_plan() -> dict[str, Any]:
    config = load_json(FUNCTION_CONFIG_PATH)
    current_role = str(config.get("role") or "")
    artifacts = [
        TEMPLATE_PATH,
        *LAMBDA_POLICY_PATHS.values(),
        LAMBDA_TRUST_PATH,
        SAGEMAKER_POLICY_PATH,
        SAGEMAKER_TRUST_PATH,
        DASHBOARD_DENY_PATH,
    ]
    return {
        "schema": "JustHodlAiInfrastructureChangePlan/v1",
        "mode": "offline-dry-run",
        "aws_api_calls": 0,
        "target": {"account": ACCOUNT_ID, "region": REGION},
        "observed_repository_config": {
            "function_name": config.get("function_name"),
            "role": current_role,
            "shared_role_detected": current_role.endswith(SHARED_ROLE_SUFFIX),
            "brain_source_bucket_configured": bool(
                (config.get("env") or {}).get("AI_BRAIN_SOURCE_BUCKET")
            ),
            "model_package_group_configured": (
                (config.get("env") or {}).get("MODEL_PACKAGE_GROUP")
                == MODEL_PACKAGE_GROUP
            ),
            "mlflow_tracking_server_configured": (
                (config.get("env") or {}).get("MLFLOW_TRACKING_SERVER_NAME")
                == MLFLOW_TRACKING_SERVER
                and (config.get("env") or {}).get("MLFLOW_TRACKING_SERVER_ARN")
                == MLFLOW_TRACKING_SERVER_ARN
            ),
        },
        "artifacts": {
            str(path.relative_to(REPO_ROOT)): {"sha256": sha256(path)}
            for path in artifacts
        },
        "changes": [
            {
                "order": 10,
                "action": "CREATE_CHANGE_SET",
                "artifact": str(TEMPLATE_PATH.relative_to(REPO_ROOT)),
                "execution": "manual-and-separately-approved",
                "creates": [
                    "dedicated Lambda role with six domain-separated managed policies",
                    "least-privilege SageMaker training/deployment role",
                    "dedicated Feature Store offline-materialization role",
                    "KMS key",
                    "private canonical Brain bucket",
                    "Feature Store bucket and two online/offline feature groups",
                    "retained SageMaker Model Package Group",
                    "retained Managed MLflow tracking server, dedicated role, and private KMS-encrypted artifact bucket",
                    "custom event bus, staged-disabled rules, queues, and DLQs",
                    "append-only prediction archive and protected DynamoDB index",
                ],
            },
            {
                "order": 20,
                "action": "MIGRATE_AND_VERIFY_BRAIN",
                "from": "justhodl-dashboard-live/data/brain.json",
                "to": f"s3://{CANONICAL_BRAIN_BUCKET}/data/brain.json",
                "preconditions": [
                    "copy with checksum evidence",
                    "verify object is SSE-KMS encrypted and not publicly reachable",
                    "retain source only until owner read canary succeeds",
                ],
            },
            {
                "order": 30,
                "action": "UPDATE_EXISTING_LAMBDA_CONFIGURATION",
                "target": "justhodl-ai",
                "role": DEDICATED_LAMBDA_ROLE,
                "environment_merge": {
                    "AI_BRAIN_SOURCE_BUCKET": CANONICAL_BRAIN_BUCKET,
                    "PREDICTION_LEDGER_TABLE": LEDGER_TABLE,
                    "AI_EVENT_BUS": EVENT_BUS,
                    "SIGNAL_FEATURE_GROUP": "justhodl-ai-signal-prod",
                    "PREDICTION_FEATURE_GROUP": "justhodl-ai-prediction-prod",
                    "PREDICTION_LEDGER_ARCHIVE_BUCKET": LEDGER_ARCHIVE_BUCKET,
                    "MODEL_PACKAGE_GROUP": MODEL_PACKAGE_GROUP,
                    "MLFLOW_TRACKING_SERVER_NAME": MLFLOW_TRACKING_SERVER,
                    "MLFLOW_TRACKING_SERVER_ARN": MLFLOW_TRACKING_SERVER_ARN,
                    "MLFLOW_EXPERIMENT_NAME": MLFLOW_TRACKING_SERVER,
                    "SAGEMAKER_ROLE_ARN": DEDICATED_SAGEMAKER_ROLE,
                },
                "precondition": "manual approval after IAM simulation; preserve all other environment variables",
            },
            {
                "order": 40,
                "action": "CANARY",
                "checks": [
                    "signed-out dashboard cannot read any Brain prefix",
                    "owner can build a dataset from the canonical private Brain object",
                    "unprefixed or untagged SageMaker create is denied",
                    "tagged jh-ai-* create works within budget policy",
                    "prediction archive failure creates no DynamoDB index row",
                    "prediction retry verifies the immutable archive before conditionally creating the index row",
                    "Lambda cannot overwrite canonical Brain objects outside signals/v1/",
                    "new model packages remain PendingManualApproval",
                    "training can log an MLflow run and cannot delete it or access another tracking server",
                    "MLflow artifacts are private, versioned, and encrypted with alias/justhodl-ai-prod",
                    "rollback restores the prior Lambda role and environment",
                ],
            },
            {
                "order": 50,
                "action": "MERGE_BUCKET_POLICY",
                "artifact": str(DASHBOARD_DENY_PATH.relative_to(REPO_ROOT)),
                "target": "justhodl-dashboard-live",
                "precondition": "merge with the live policy after private-read canary; never replace unseen statements",
            },
            {
                "order": 60,
                "action": "ENABLE_EVENT_RULES",
                "rules": [
                    "justhodl-ai-signal-envelope-v1-prod",
                    "justhodl-ai-prediction-v2-prod",
                    "justhodl-ai-model-package-state-prod",
                ],
                "precondition": "queue consumers, replay protection, and DLQ alarms are separately reviewed",
            },
        ],
        "blockers": [
            {
                "id": "LAMBDA_ROLE_CUTOVER_PENDING",
                "active": current_role.endswith(SHARED_ROLE_SUFFIX),
                "detail": (
                    "repository config still references the shared lambda-execution-role"
                    if current_role.endswith(SHARED_ROLE_SUFFIX)
                    else "repository config binds the dedicated Lambda role"
                ),
            },
            {
                "id": "BRAIN_SOURCE_CUTOVER_PENDING",
                "active": not bool(
                    (config.get("env") or {}).get("AI_BRAIN_SOURCE_BUCKET")
                ),
                "detail": (
                    "repository config does not yet bind the canonical Brain bucket"
                    if not (config.get("env") or {}).get("AI_BRAIN_SOURCE_BUCKET")
                    else "repository config binds the canonical Brain bucket"
                ),
            },
            {
                "id": "MODEL_REGISTRY_BINDING_PENDING",
                "active": (config.get("env") or {}).get("MODEL_PACKAGE_GROUP")
                != MODEL_PACKAGE_GROUP,
                "detail": (
                    "repository config does not bind the governed model package group"
                    if (config.get("env") or {}).get("MODEL_PACKAGE_GROUP")
                    != MODEL_PACKAGE_GROUP
                    else "repository config binds the governed model package group"
                ),
            },
            {
                "id": "MLFLOW_BINDING_PENDING",
                "active": not (
                    (config.get("env") or {}).get("MLFLOW_TRACKING_SERVER_NAME")
                    == MLFLOW_TRACKING_SERVER
                    and (config.get("env") or {}).get("MLFLOW_TRACKING_SERVER_ARN")
                    == MLFLOW_TRACKING_SERVER_ARN
                ),
                "detail": (
                    "repository config binds the governed MLflow tracking server"
                    if (
                        (config.get("env") or {}).get("MLFLOW_TRACKING_SERVER_NAME")
                        == MLFLOW_TRACKING_SERVER
                        and (config.get("env") or {}).get(
                            "MLFLOW_TRACKING_SERVER_ARN"
                        )
                        == MLFLOW_TRACKING_SERVER_ARN
                    )
                    else "repository config does not bind the governed MLflow tracking server"
                ),
            },
            {
                "id": "NO_LIVE_AWS_DIFF",
                "active": True,
                "detail": "offline plan intentionally does not inspect AWS; create-set review is mandatory",
            },
        ],
    }


def render_checks(checks: Iterable[Check]) -> str:
    lines = []
    for item in checks:
        lines.append(f"{'PASS' if item.passed else 'FAIL'} {item.name}: {item.detail}")
    passed = sum(item.passed for item in checks)
    total = len(checks)
    lines.append(f"SUMMARY {passed}/{total} static gates passed")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format", choices=("text", "json"), default="text", help="output format"
    )
    parser.add_argument(
        "--plan", action="store_true", help="include the deterministic local change plan"
    )
    parser.add_argument(
        "--write-plan", type=Path, help="write the local change plan JSON to this path"
    )
    args = parser.parse_args(argv)

    try:
        checks = validate()
        plan = change_plan() if (args.plan or args.write_plan) else None
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"ERROR artifact load/shape failure: {exc}", file=sys.stderr)
        return 2

    if args.write_plan:
        args.write_plan.parent.mkdir(parents=True, exist_ok=True)
        args.write_plan.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    if args.format == "json":
        payload: dict[str, Any] = {
            "ok": all(item.passed for item in checks),
            "checks": [asdict(item) for item in checks],
        }
        if plan is not None:
            payload["change_plan"] = plan
        print(json.dumps(payload, indent=2))
    else:
        print(render_checks(checks))
        if plan is not None:
            print("\nCHANGE PLAN")
            print(json.dumps(plan, indent=2))
        if args.write_plan:
            print(f"\nWROTE {args.write_plan}")
    return 0 if all(item.passed for item in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
