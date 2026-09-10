"""SageMaker registry request, model-card, and MLflow lineage contracts."""
from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from governance_control import ControlledExecution

PENDING_APPROVAL = "PendingManualApproval"
LINEAGE_SCHEMA_VERSION = "1.0"
MODEL_CARD_SCHEMA_VERSION = "1.0"
_NAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?$")
_DIGEST = re.compile(r"^[a-fA-F0-9]{32,128}$")


class ModelGovernanceError(ValueError):
    pass


class ManagedMlflowWriter:
    """Concrete, dependency-injected adapter for a managed MLflow client."""

    def __init__(
        self,
        mlflow_client: Any,
        *,
        expected_experiment_name: str,
        param_factory: Callable[..., Any],
        tag_factory: Callable[..., Any],
    ) -> None:
        if mlflow_client is None:
            raise ModelGovernanceError("MLflow client is required")
        self.client = mlflow_client
        self.expected_experiment_name = str(expected_experiment_name or "").strip()
        self.param_factory = param_factory
        self.tag_factory = tag_factory
        if not self.expected_experiment_name:
            raise ModelGovernanceError("expected MLflow experiment is required")

    def log_lineage(self, metadata: Mapping[str, Any]) -> Dict[str, Any]:
        run = self.client.get_run(metadata["run_id"])
        run_info = getattr(run, "info", None)
        experiment_id = getattr(run_info, "experiment_id", None)
        if not experiment_id:
            raise ModelGovernanceError("MLflow run has no experiment identity")
        experiment = self.client.get_experiment(experiment_id)
        if getattr(experiment, "name", None) != self.expected_experiment_name:
            raise ModelGovernanceError(
                "MLflow run does not belong to the configured experiment"
            )
        params = [
            self.param_factory(key=key, value=str(value))
            for key, value in metadata["params"].items()
        ]
        tags = [
            self.tag_factory(key=key, value=str(value))
            for key, value in metadata["tags"].items()
        ]
        self.client.log_batch(
            run_id=metadata["run_id"], metrics=[], params=params, tags=tags
        )
        verified = self.client.get_run(metadata["run_id"])
        verified_info = getattr(verified, "info", None)
        artifact_uri = str(getattr(verified_info, "artifact_uri", "") or "")
        if not artifact_uri.startswith("s3://"):
            raise ModelGovernanceError("MLflow run has no governed S3 artifact URI")
        return {"run_id": metadata["run_id"], "artifact_uri": artifact_uri}


def _name(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not _NAME.fullmatch(text):
        raise ModelGovernanceError("%s is missing or invalid" % field)
    return text


def _timestamp(value: Any, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ModelGovernanceError("%s must be RFC3339" % field) from exc
    else:
        raise ModelGovernanceError("%s must be RFC3339" % field)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ModelGovernanceError("%s must include a timezone" % field)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_model_package_request(
    *,
    package_group_name: str,
    model_data_url: str,
    image_uri: str,
    content_types: Sequence[str],
    response_types: Sequence[str],
    model_metrics: Optional[Mapping[str, Any]] = None,
    customer_metadata: Optional[Mapping[str, str]] = None,
    description: Optional[str] = None,
    tags: Sequence[Mapping[str, str]] = (),
) -> Dict[str, Any]:
    """Build a registration request that cannot bypass manual approval."""
    group = _name(package_group_name, "package_group_name")
    if not str(model_data_url).startswith("s3://"):
        raise ModelGovernanceError("model_data_url must be an s3 URI")
    if ".dkr.ecr." not in str(image_uri) or ".amazonaws.com/" not in str(image_uri):
        raise ModelGovernanceError("image_uri must be an ECR image URI")
    if not content_types or not response_types:
        raise ModelGovernanceError("content_types and response_types are required")
    metadata = dict(customer_metadata or {})
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in metadata.items()):
        raise ModelGovernanceError("customer_metadata keys and values must be strings")
    request = {
        "ModelPackageGroupName": group,
        "ModelApprovalStatus": PENDING_APPROVAL,
        "InferenceSpecification": {
            "Containers": [{"Image": str(image_uri), "ModelDataUrl": str(model_data_url)}],
            "SupportedContentTypes": list(dict.fromkeys(content_types)),
            "SupportedResponseMIMETypes": list(dict.fromkeys(response_types)),
        },
        "CustomerMetadataProperties": metadata,
    }
    if description:
        request["ModelPackageDescription"] = str(description)[:1024]
    if model_metrics is not None:
        if not isinstance(model_metrics, Mapping):
            raise ModelGovernanceError("model_metrics must be an object")
        request["ModelMetrics"] = deepcopy(dict(model_metrics))
    if tags:
        request["Tags"] = [dict(tag) for tag in tags]
    return request


def register_model_package(
    sagemaker_client: Any,
    *,
    live: bool = False,
    control: Optional[ControlledExecution] = None,
    approval_token: Optional[str] = None,
    owner: Optional[str] = None,
    estimated_cost_usd: float = 0.0,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Plan or register a package under explicit controlled live execution.

    Live mode performs both create and describe through the supplied budget
    control and verifies that SageMaker persisted ``PendingManualApproval``.
    """
    request = build_model_package_request(**kwargs)
    if not live:
        return {"mode": "DRY_RUN", "side_effects": 0, "request": request}
    if sagemaker_client is None or not isinstance(control, ControlledExecution):
        raise ModelGovernanceError(
            "live model registration requires an injected client and control"
        )
    response = control.execute(
        "sagemaker.create_model_package",
        lambda: sagemaker_client.create_model_package(**request),
        approval_token=approval_token,
        owner=owner,
        estimated_cost_usd=estimated_cost_usd,
    )
    arn = response.get("ModelPackageArn") if isinstance(response, Mapping) else None
    if not isinstance(arn, str) or not arn.startswith("arn:"):
        raise ModelGovernanceError("SageMaker did not return a ModelPackageArn")
    described = control.execute(
        "sagemaker.describe_model_package",
        lambda: sagemaker_client.describe_model_package(ModelPackageName=arn),
        approval_token=approval_token,
        owner=owner,
    )
    if (
        not isinstance(described, Mapping)
        or described.get("ModelApprovalStatus") != PENDING_APPROVAL
    ):
        raise ModelGovernanceError(
            "registered package was not persisted as PendingManualApproval"
        )
    return {
        "mode": "LIVE",
        "side_effects": 2,
        "model_package_arn": arn,
        "approval_status": PENDING_APPROVAL,
        "request": request,
        "control_evidence": control.evidence(),
    }


def build_model_card_payload(
    *,
    model_card_name: str,
    model_id: str,
    model_version: str,
    owner: str,
    intended_use: str,
    limitations: Sequence[str],
    risk_rating: str,
    training_dataset: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    lineage: Mapping[str, Any],
    created_at: Any,
    model_package_arn: Optional[str] = None,
    tags: Sequence[Mapping[str, str]] = (),
) -> Dict[str, Any]:
    """Build a deterministic Draft SageMaker Model Card request."""
    name = _name(model_card_name, "model_card_name")
    if risk_rating not in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
        raise ModelGovernanceError("risk_rating is invalid")
    if not intended_use.strip() or not owner.strip():
        raise ModelGovernanceError("owner and intended_use are required")
    if not limitations or any(not isinstance(item, str) or not item.strip() for item in limitations):
        raise ModelGovernanceError("at least one non-empty limitation is required")
    if not isinstance(training_dataset, Mapping) or not isinstance(evaluation, Mapping) or not isinstance(lineage, Mapping):
        raise ModelGovernanceError("training_dataset, evaluation and lineage must be objects")
    for field in (
        "training_eligibility_digest",
        "walk_forward_split_digest",
        "cpcv_split_digest",
    ):
        value = evaluation.get(field)
        if not isinstance(value, str) or not _DIGEST.fullmatch(value):
            raise ModelGovernanceError(
                "evaluation.%s must be a verified hex digest" % field
            )
    content = {
        "schema_version": MODEL_CARD_SCHEMA_VERSION,
        "model_overview": {
            "model_id": _name(model_id, "model_id"),
            "model_version": _name(model_version, "model_version"),
            "owner": owner.strip(),
            "created_at": _timestamp(created_at, "created_at"),
            "model_package_arn": model_package_arn,
        },
        "intended_use": intended_use.strip(),
        "limitations": [item.strip() for item in limitations],
        "risk_rating": risk_rating,
        "training_dataset": deepcopy(dict(training_dataset)),
        "evaluation": deepcopy(dict(evaluation)),
        "lineage": deepcopy(dict(lineage)),
        "approval": {"status": PENDING_APPROVAL, "approved_by": None, "approved_at": None},
    }
    try:
        encoded = json.dumps(content, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ModelGovernanceError("model card content must be finite JSON") from exc
    request = {
        "ModelCardName": name,
        "ModelCardStatus": "Draft",
        "Content": encoded,
    }
    if tags:
        request["Tags"] = [dict(tag) for tag in tags]
    return request


def create_model_card(
    model_card_client: Any,
    *,
    live: bool = False,
    control: Optional[ControlledExecution] = None,
    approval_token: Optional[str] = None,
    owner: Optional[str] = None,
    estimated_cost_usd: float = 0.0,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Plan or create a Draft model card under the same live control boundary."""
    request = build_model_card_payload(owner=str(owner or ""), **kwargs)
    if not live:
        return {"mode": "DRY_RUN", "side_effects": 0, "request": request}
    if model_card_client is None or not isinstance(control, ControlledExecution):
        raise ModelGovernanceError(
            "live model-card creation requires an injected client and control"
        )
    response = control.execute(
        "sagemaker.create_model_card",
        lambda: model_card_client.create_model_card(**request),
        approval_token=approval_token,
        owner=owner,
        estimated_cost_usd=estimated_cost_usd,
    )
    arn = response.get("ModelCardArn") if isinstance(response, Mapping) else None
    if not isinstance(arn, str) or not arn.startswith("arn:"):
        raise ModelGovernanceError("SageMaker did not return a ModelCardArn")
    described = control.execute(
        "sagemaker.describe_model_card",
        lambda: model_card_client.describe_model_card(
            ModelCardName=request["ModelCardName"]
        ),
        approval_token=approval_token,
        owner=owner,
    )
    if not isinstance(described, Mapping) or described.get("ModelCardStatus") != "Draft":
        raise ModelGovernanceError("created model card was not persisted as Draft")
    return {
        "mode": "LIVE",
        "side_effects": 2,
        "model_card_arn": arn,
        "model_card_status": "Draft",
        "request": request,
        "control_evidence": control.evidence(),
    }


def build_mlflow_lineage_metadata(
    *,
    run_id: str,
    experiment_name: str,
    dataset_digest: str,
    feature_schema_digest: str,
    walk_forward_split_digest: str,
    cpcv_split_digest: str,
    code_revision: str,
    signal_envelope_version: str,
    training_data_uri: str,
    artifact_uri: str,
    extra_tags: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Build an MLflow-neutral lineage contract of params, tags and artifacts."""
    run_id = _name(run_id, "run_id")
    experiment_name = str(experiment_name or "").strip()
    if not experiment_name:
        raise ModelGovernanceError("experiment_name is required")
    digests = {
        "dataset_digest": dataset_digest,
        "feature_schema_digest": feature_schema_digest,
        "walk_forward_split_digest": walk_forward_split_digest,
        "cpcv_split_digest": cpcv_split_digest,
    }
    for field, value in digests.items():
        if not isinstance(value, str) or not _DIGEST.fullmatch(value):
            raise ModelGovernanceError("%s must be a hex digest" % field)
        digests[field] = value.lower()
    if not re.fullmatch(r"[a-fA-F0-9]{7,64}", str(code_revision or "")):
        raise ModelGovernanceError("code_revision must be an immutable git revision")
    if signal_envelope_version != "1.0":
        raise ModelGovernanceError("unsupported signal_envelope_version")
    if not str(training_data_uri).startswith("s3://"):
        raise ModelGovernanceError("training_data_uri must be an s3 URI")
    if not str(artifact_uri).startswith("s3://"):
        raise ModelGovernanceError("artifact_uri must be an s3 URI")
    tags = {
        "governance.lineage_schema_version": LINEAGE_SCHEMA_VERSION,
        "governance.dataset_digest": digests["dataset_digest"],
        "governance.feature_schema_digest": digests["feature_schema_digest"],
        "governance.walk_forward_split_digest": digests["walk_forward_split_digest"],
        "governance.cpcv_split_digest": digests["cpcv_split_digest"],
        "governance.code_revision": code_revision.lower(),
        "governance.signal_envelope_version": signal_envelope_version,
    }
    for key, value in dict(extra_tags or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ModelGovernanceError("extra_tags keys and values must be strings")
        if key.startswith("governance."):
            raise ModelGovernanceError("extra_tags may not override governance tags")
        tags[key] = value
    return {
        "schema_version": LINEAGE_SCHEMA_VERSION,
        "run_id": run_id,
        "experiment_name": experiment_name,
        "params": {
            "dataset_digest": digests["dataset_digest"],
            "feature_schema_digest": digests["feature_schema_digest"],
            "walk_forward_split_digest": digests["walk_forward_split_digest"],
            "cpcv_split_digest": digests["cpcv_split_digest"],
            "code_revision": code_revision.lower(),
            "signal_envelope_version": signal_envelope_version,
        },
        "tags": tags,
        "artifacts": {
            "training_data_uri": str(training_data_uri),
            "model_artifact_uri": str(artifact_uri),
        },
    }


def validate_mlflow_lineage_metadata(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    """Rebuild an existing contract to verify required immutable metadata."""
    if not isinstance(metadata, Mapping) or metadata.get("schema_version") != LINEAGE_SCHEMA_VERSION:
        raise ModelGovernanceError("unsupported lineage schema_version")
    params = metadata.get("params")
    artifacts = metadata.get("artifacts")
    if not isinstance(params, Mapping) or not isinstance(artifacts, Mapping):
        raise ModelGovernanceError("lineage params and artifacts are required")
    extra = {
        key: value for key, value in dict(metadata.get("tags") or {}).items()
        if not key.startswith("governance.")
    }
    return build_mlflow_lineage_metadata(
        run_id=metadata.get("run_id"),
        experiment_name=metadata.get("experiment_name"),
        dataset_digest=params.get("dataset_digest"),
        feature_schema_digest=params.get("feature_schema_digest"),
        walk_forward_split_digest=params.get("walk_forward_split_digest"),
        cpcv_split_digest=params.get("cpcv_split_digest"),
        code_revision=params.get("code_revision"),
        signal_envelope_version=params.get("signal_envelope_version"),
        training_data_uri=artifacts.get("training_data_uri"),
        artifact_uri=artifacts.get("model_artifact_uri"),
        extra_tags=extra,
    )


def publish_mlflow_lineage(
    writer: Any,
    metadata: Mapping[str, Any],
    *,
    live: bool = False,
    control: Optional[ControlledExecution] = None,
    approval_token: Optional[str] = None,
    owner: Optional[str] = None,
    estimated_cost_usd: float = 0.0,
) -> Dict[str, Any]:
    """Validate and publish lineage through an injected Managed-MLflow adapter.

    The adapter must expose ``log_lineage(metadata)`` and return a receipt with
    the same run ID plus a non-empty artifact URI.  The adapter is intentionally
    injected so unit tests and controlled operators never use module-global
    service clients.
    """
    normalized = validate_mlflow_lineage_metadata(metadata)
    if not live:
        return {
            "mode": "DRY_RUN",
            "side_effects": 0,
            "metadata": normalized,
        }
    if writer is None or not callable(getattr(writer, "log_lineage", None)):
        raise ModelGovernanceError(
            "live MLflow lineage publication requires an injected writer"
        )
    if not isinstance(control, ControlledExecution):
        raise ModelGovernanceError(
            "live MLflow lineage publication requires an injected control"
        )
    receipt = control.execute(
        "mlflow.log_lineage",
        lambda: writer.log_lineage(normalized),
        approval_token=approval_token,
        owner=owner,
        api_calls=4,
        estimated_cost_usd=estimated_cost_usd,
    )
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("run_id") != normalized["run_id"]
        or not str(receipt.get("artifact_uri") or "").startswith("s3://")
    ):
        raise ModelGovernanceError("MLflow writer returned invalid lineage evidence")
    return {
        "mode": "LIVE",
        "side_effects": 1,
        "metadata": normalized,
        "receipt": dict(receipt),
        "control_evidence": control.evidence(),
    }
