"""Blue-green readiness, rollback policy, and approval-gated canary execution."""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from governance_control import (
    ControlledExecution,
    ExecutionBudget,
    GovernanceControlError,
)


# Tests may install a deterministic clock here; production leaves it None (real perf_counter).
PROBE_CLOCK = None


class DeploymentGateError(ValueError):
    pass


class ApprovalRequired(DeploymentGateError):
    pass


class RollbackEvidenceError(DeploymentGateError):
    pass


_DIGEST = re.compile(r"^[a-fA-F0-9]{64}$")


def _rollback_plan(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RollbackEvidenceError("a rollback plan is required before green deployment")
    required = ("blue_endpoint_config", "blue_variant", "captured_at", "evidence_digest")
    if any(not str(value.get(field) or "").strip() for field in required):
        raise RollbackEvidenceError("rollback plan evidence is incomplete")
    if not _DIGEST.fullmatch(str(value["evidence_digest"])):
        raise RollbackEvidenceError("rollback plan evidence_digest is invalid")
    return dict(value)


def validate_rollback_evidence(
    value: Any, plan: Mapping[str, Any]
) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RollbackEvidenceError("rollback action must return evidence")
    if value.get("restored_endpoint_config") != plan.get("blue_endpoint_config"):
        raise RollbackEvidenceError(
            "rollback did not restore the captured blue endpoint config"
        )
    if value.get("traffic_restored") is not True:
        raise RollbackEvidenceError("rollback did not prove blue traffic restoration")
    health_checks = value.get("successful_health_checks")
    if (
        isinstance(health_checks, bool)
        or not isinstance(health_checks, int)
        or health_checks < 3
    ):
        raise RollbackEvidenceError(
            "rollback evidence requires at least three successful health checks"
        )
    if not str(value.get("verified_at") or "").strip():
        raise RollbackEvidenceError("rollback evidence verified_at is required")
    if not _DIGEST.fullmatch(str(value.get("evidence_digest") or "")):
        raise RollbackEvidenceError("rollback evidence_digest is invalid")
    return dict(value)


@dataclass(frozen=True)
class ReadinessThresholds:
    min_samples: int = 100
    max_error_rate: float = 0.01
    max_p95_latency_ms: float = 1000.0
    max_latency_regression_ratio: float = 1.20
    max_drift_score: float = 0.10
    min_prediction_match_rate: float = 0.95

    def __post_init__(self) -> None:
        if isinstance(self.min_samples, bool) or not isinstance(self.min_samples, int) or self.min_samples <= 0:
            raise DeploymentGateError("min_samples must be a positive integer")
        for field in (
            "max_error_rate", "max_p95_latency_ms", "max_latency_regression_ratio",
            "max_drift_score", "min_prediction_match_rate",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise DeploymentGateError("%s must be finite" % field)
        if not 0 <= self.max_error_rate <= 1 or not 0 <= self.max_drift_score <= 1:
            raise DeploymentGateError("rate thresholds must be between zero and one")
        if not 0 <= self.min_prediction_match_rate <= 1:
            raise DeploymentGateError("min_prediction_match_rate must be between zero and one")
        if self.max_p95_latency_ms <= 0 or self.max_latency_regression_ratio < 1:
            raise DeploymentGateError("latency thresholds are invalid")


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    action: str
    reasons: Tuple[str, ...]
    evidence: Mapping[str, Any]


def _required_text(value: Mapping[str, Any], field: str) -> str:
    result = str(value.get(field) or "").strip()
    if not result:
        raise DeploymentGateError("context.%s is required" % field)
    return result


def _tags(value: Mapping[str, Any]) -> Dict[str, str]:
    return {
        str(item.get("Key") or ""): str(item.get("Value") or "")
        for item in value.get("Tags", [])
        if isinstance(item, Mapping)
    }


class SageMakerCanaryActions:
    """Concrete, dependency-injected SageMaker blue-green operations.

    The class contains no boto3 construction and is therefore safe to exercise
    with fakes.  The Lambda HTTP composition supplies real clients only after
    the state machine has validated the explicit approval token, owner, budget,
    and captured rollback plan.
    """

    def __init__(
        self,
        sagemaker: Any,
        runtime: Any,
        cloudwatch: Any,
        *,
        sleep_fn: Callable[[float], None],
        now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        wait_attempts: int = 20,
        wait_seconds: float = 15.0,
        health_checks: int = 3,
        metric_attempts: int = 12,
        metric_wait_seconds: float = 10.0,
        metric_period_seconds: int = 60,
        metric_ingestion_window_seconds: int = 180,
        monotonic_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        if any(client is None for client in (sagemaker, runtime, cloudwatch)):
            raise DeploymentGateError("all SageMaker canary clients are required")
        if (
            wait_attempts <= 0
            or health_checks < 3
            or wait_seconds < 0
            or metric_attempts <= 0
            or metric_wait_seconds < 0
            or metric_period_seconds != 60
            or metric_ingestion_window_seconds < metric_period_seconds
        ):
            raise DeploymentGateError("canary wait and health-check bounds are invalid")
        self.sm = sagemaker
        self.runtime = runtime
        self.cw = cloudwatch
        self.sleep = sleep_fn
        self.now = now_fn
        self.wait_attempts = wait_attempts
        self.wait_seconds = wait_seconds
        self.health_checks = health_checks
        self.metric_attempts = metric_attempts
        self.metric_wait_seconds = metric_wait_seconds
        self.metric_period_seconds = metric_period_seconds
        self.metric_ingestion_window_seconds = metric_ingestion_window_seconds
        self.monotonic = monotonic_fn or PROBE_CLOCK or time.perf_counter

    def callbacks(self) -> Dict[str, Callable[[Mapping[str, Any]], Any]]:
        return {
            "validate": self.validate,
            "deploy_green": self.deploy_green,
            "wait_green": self.wait_green,
            "run_canary": self.run_canary,
            "promote": self.promote,
            "rollback": self.rollback,
            "verify_rollback": self.verify_rollback,
            "cleanup_green": self.cleanup_green,
        }

    def _resource_owner(self, arn: str) -> str:
        return _tags(self.sm.list_tags(ResourceArn=arn)).get(
            "justhodl-ai-owner", ""
        )

    def _wait_endpoint(self, name: str, expected_config: str) -> Mapping[str, Any]:
        for attempt in range(self.wait_attempts):
            value = self.sm.describe_endpoint(EndpointName=name)
            status = value.get("EndpointStatus")
            if status == "InService":
                if value.get("EndpointConfigName") != expected_config:
                    raise DeploymentGateError(
                        "endpoint reached InService with an unexpected config"
                    )
                return value
            if status in {"Failed", "OutOfService"}:
                raise DeploymentGateError("endpoint entered terminal unhealthy state")
            if attempt + 1 < self.wait_attempts:
                self.sleep(self.wait_seconds)
        raise DeploymentGateError("endpoint did not become InService within the bound")

    def _invoke(self, endpoint_name: str, probe: Any) -> Tuple[Any, float]:
        if not isinstance(probe, Mapping):
            raise DeploymentGateError("context.health_probe must be an object")
        content_type = str(probe.get("content_type") or "").strip()
        body = probe.get("body")
        if not content_type or body is None:
            raise DeploymentGateError(
                "health_probe.content_type and health_probe.body are required"
            )
        payload = body if isinstance(body, bytes) else str(body).encode("utf-8")
        started = self.monotonic()
        response = self.runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType=content_type,
            Body=payload,
        )
        elapsed_ms = max(0.001, (self.monotonic() - started) * 1000.0)
        response_body = response.get("Body")
        if response_body is None:
            raise DeploymentGateError("canary invocation returned no body")
        raw = response_body.read() if hasattr(response_body, "read") else response_body
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            value = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError) as exc:
            raise DeploymentGateError("canary invocation returned invalid JSON") from exc
        return value, elapsed_ms

    @staticmethod
    def _prediction(value: Any) -> Any:
        if isinstance(value, Mapping):
            for key in ("prediction", "predictions", "label", "output"):
                if key in value:
                    return SageMakerCanaryActions._prediction(value[key])
            return {str(key): SageMakerCanaryActions._prediction(item) for key, item in sorted(value.items())}
        if isinstance(value, list) and len(value) == 1:
            return SageMakerCanaryActions._prediction(value[0])
        return value

    @staticmethod
    def _distance(left: Any, right: Any) -> float:
        if (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
            and not isinstance(right, bool)
        ):
            denominator = max(abs(float(left)), abs(float(right)), 1.0)
            return min(1.0, abs(float(left) - float(right)) / denominator)
        if isinstance(left, list) and isinstance(right, list) and len(left) == len(right) and left:
            return sum(
                SageMakerCanaryActions._distance(a, b)
                for a, b in zip(left, right)
            ) / len(left)
        return 0.0 if left == right else 1.0

    def _probe(self, endpoint_name: str, probe: Any) -> int:
        for _ in range(self.health_checks):
            self._invoke(endpoint_name, probe)
        return self.health_checks

    def validate(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        endpoint_name = _required_text(context, "endpoint_name")
        _required_text(context, "green_endpoint_name")
        green_config = _required_text(context, "green_endpoint_config")
        package_arn = _required_text(context, "model_package_arn")
        owner = _required_text(context, "approved_owner")
        plan = _rollback_plan(context.get("rollback_plan"))
        endpoint = self.sm.describe_endpoint(EndpointName=endpoint_name)
        endpoint_arn = _required_text(endpoint, "EndpointArn")
        if endpoint.get("EndpointStatus") != "InService":
            raise DeploymentGateError("blue endpoint is not InService")
        if endpoint.get("EndpointConfigName") != plan["blue_endpoint_config"]:
            raise DeploymentGateError(
                "rollback plan does not match the live blue endpoint config"
            )
        package = self.sm.describe_model_package(ModelPackageName=package_arn)
        config = self.sm.describe_endpoint_config(EndpointConfigName=green_config)
        config_arn = _required_text(config, "EndpointConfigArn")
        package_resource_arn = str(package.get("ModelPackageArn") or package_arn)
        if package.get("ModelApprovalStatus") != "Approved":
            raise DeploymentGateError("model package is not manually Approved")
        if self._resource_owner(endpoint_arn) != owner:
            raise DeploymentGateError("blue endpoint ownership does not match approval")
        if self._resource_owner(package_resource_arn) != owner:
            raise DeploymentGateError("model package ownership does not match approval")
        if self._resource_owner(config_arn) != owner:
            raise DeploymentGateError(
                "green endpoint config ownership does not match approval"
            )
        return {
            "endpoint_status": "InService",
            "blue_endpoint_config": plan["blue_endpoint_config"],
            "model_approval_status": "Approved",
            "model_package_arn": package_resource_arn,
            "green_endpoint_config_arn": config_arn,
            "owner": owner,
        }

    def deploy_green(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        name = _required_text(context, "green_endpoint_name")
        config = _required_text(context, "green_endpoint_config")
        owner = _required_text(context, "approved_owner")
        response = self.sm.create_endpoint(
            EndpointName=name,
            EndpointConfigName=config,
            Tags=[
                {"Key": "justhodl", "Value": "ai"},
                {"Key": "justhodl-ai-managed", "Value": "true"},
                {"Key": "justhodl-ai-purpose", "Value": "governed-canary"},
                {"Key": "justhodl-ai-ttl-hours", "Value": "3"},
                {"Key": "justhodl-ai-pinned", "Value": "false"},
                {"Key": "justhodl-ai-owner", "Value": owner},
            ],
        )
        return {
            "endpoint_name": name,
            "endpoint_config": config,
            "endpoint_arn": str(response.get("EndpointArn") or ""),
        }

    def wait_green(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        value = self._wait_endpoint(
            _required_text(context, "green_endpoint_name"),
            _required_text(context, "green_endpoint_config"),
        )
        return {
            "endpoint_status": value["EndpointStatus"],
            "endpoint_config": value["EndpointConfigName"],
        }

    def _metrics(
        self,
        endpoint_name: str,
        variant_name: str,
        started_at: datetime,
        required_samples: int,
    ) -> Mapping[str, float]:
        if not variant_name:
            raise DeploymentGateError("resolved production variant is required")
        dimensions = [
            {"Name": "EndpointName", "Value": endpoint_name},
            {"Name": "VariantName", "Value": variant_name},
        ]
        started_at = started_at.astimezone(timezone.utc)
        period_start = started_at.replace(second=0, microsecond=0)
        last_reason = "required canary metrics are missing"
        for attempt in range(self.metric_attempts):
            end = self.now().astimezone(timezone.utc)
            response = self.cw.get_metric_data(
                MetricDataQueries=[
                {
                    "Id": "invocations",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/SageMaker",
                            "MetricName": "Invocations",
                            "Dimensions": dimensions,
                        },
                        "Period": self.metric_period_seconds,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "errors",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/SageMaker",
                            "MetricName": "Invocation5XXErrors",
                            "Dimensions": dimensions,
                        },
                        "Period": self.metric_period_seconds,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "latency",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/SageMaker",
                            "MetricName": "ModelLatency",
                            "Dimensions": dimensions,
                        },
                        "Period": self.metric_period_seconds,
                        "Stat": "p95",
                    },
                    "ReturnData": True,
                },
            ],
                StartTime=period_start,
                EndTime=end,
                ScanBy="TimestampAscending",
            )
            rows = {
                str(row.get("Id")): row
                for row in response.get("MetricDataResults", [])
                if isinstance(row, Mapping)
            }
            values = {
                key: [
                    float(value) for value in rows.get(key, {}).get("Values", [])
                    if isinstance(value, (int, float)) and not isinstance(value, bool)
                ]
                for key in ("invocations", "errors", "latency")
            }
            timestamps_by_metric = {
                key: [
                    stamp.astimezone(timezone.utc)
                    for stamp in rows.get(key, {}).get("Timestamps", [])
                    if (
                        isinstance(stamp, datetime)
                        and stamp.tzinfo is not None
                        and stamp.utcoffset() is not None
                    )
                ]
                for key in ("invocations", "errors", "latency")
            }
            timestamps = [
                stamp
                for metric_timestamps in timestamps_by_metric.values()
                for stamp in metric_timestamps
            ]
            aligned = all(
                metric_timestamps
                and all(
                    stamp.second == 0
                    and stamp.microsecond == 0
                    and period_start <= stamp <= end
                    for stamp in metric_timestamps
                )
                for metric_timestamps in timestamps_by_metric.values()
            )
            ingestion_cutoff = end - timedelta(
                seconds=self.metric_ingestion_window_seconds
            )
            recent = all(
                metric_timestamps
                and max(metric_timestamps)
                + timedelta(seconds=self.metric_period_seconds)
                >= ingestion_cutoff
                for metric_timestamps in timestamps_by_metric.values()
            )
            fresh = bool(timestamps) and aligned and recent
            samples = sum(values["invocations"])
            if (
                all(values[key] for key in values)
                and fresh
                and samples >= required_samples
            ):
                return {
                    "samples": int(samples),
                    "error_rate": sum(values["errors"]) / samples,
                    "p95_latency_ms": max(values["latency"]) / 1000.0,
                    "fresh_through": max(timestamps).astimezone(timezone.utc).isoformat(),
                    "period_start": period_start.isoformat(),
                    "variant_name": variant_name,
                }
            if not fresh:
                last_reason = "CloudWatch metrics are stale"
            elif samples < required_samples:
                last_reason = "CloudWatch sample count is below generated traffic"
            if attempt + 1 < self.metric_attempts:
                self.sleep(self.metric_wait_seconds)
        raise DeploymentGateError(
            "%s after bounded metric wait" % last_reason
        )

    def _resolve_production_variant(
        self, endpoint_name: str, expected_config: str
    ) -> str:
        endpoint = self.sm.describe_endpoint(EndpointName=endpoint_name)
        if (
            endpoint.get("EndpointStatus") != "InService"
            or endpoint.get("EndpointConfigName") != expected_config
        ):
            raise DeploymentGateError(
                "green endpoint is not InService on the expected config"
            )
        live_variants = endpoint.get("ProductionVariants")
        config = self.sm.describe_endpoint_config(
            EndpointConfigName=expected_config
        )
        configured_variants = config.get("ProductionVariants")
        if (
            not isinstance(live_variants, list)
            or len(live_variants) != 1
            or not isinstance(configured_variants, list)
            or len(configured_variants) != 1
        ):
            raise DeploymentGateError(
                "green canary requires exactly one production variant"
            )
        live_name = str(live_variants[0].get("VariantName") or "").strip()
        configured_name = str(
            configured_variants[0].get("VariantName") or ""
        ).strip()
        if not live_name or live_name != configured_name:
            raise DeploymentGateError(
                "live green production variant does not match endpoint config"
            )
        return live_name

    def run_canary(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        green_name = _required_text(context, "green_endpoint_name")
        blue_name = _required_text(context, "endpoint_name")
        required_samples = context.get("required_canary_samples")
        if (
            isinstance(required_samples, bool)
            or not isinstance(required_samples, int)
            or not 1 <= required_samples <= 500
        ):
            raise DeploymentGateError(
                "required_canary_samples must be between 1 and 500"
            )
        seeds = context.get("canary_requests")
        if not isinstance(seeds, list) or not seeds or len(seeds) > 50:
            raise DeploymentGateError(
                "context.canary_requests must contain 1 to 50 bounded requests"
            )
        variant_name = self._resolve_production_variant(
            green_name, _required_text(context, "green_endpoint_config")
        )
        started_at = self.now().astimezone(timezone.utc)
        matches = 0
        distances = []
        green_latencies = []
        blue_latencies = []
        for index in range(required_samples):
            request = seeds[index % len(seeds)]
            blue_raw, blue_ms = self._invoke(blue_name, request)
            green_raw, green_ms = self._invoke(green_name, request)
            blue_prediction = self._prediction(blue_raw)
            green_prediction = self._prediction(green_raw)
            if blue_prediction == green_prediction:
                matches += 1
            distances.append(self._distance(blue_prediction, green_prediction))
            blue_latencies.append(blue_ms)
            green_latencies.append(green_ms)
        cloudwatch = self._metrics(
            green_name, variant_name, started_at, required_samples
        )
        p95_index = max(0, math.ceil(required_samples * 0.95) - 1)
        metrics = {
            "samples": required_samples,
            "error_rate": cloudwatch["error_rate"],
            "p95_latency_ms": sorted(green_latencies)[p95_index],
            "drift_score": sum(distances) / required_samples,
            "prediction_match_rate": matches / required_samples,
            "cloudwatch": dict(cloudwatch),
            "baseline": {
                "endpoint_status": "InService",
                "error_rate": 0.0,
                "p95_latency_ms": sorted(blue_latencies)[p95_index],
                "samples": required_samples,
            },
        }
        metrics.update(
            {
                "endpoint_status": "InService",
                "model_approval_status": "Approved",
                "successful_health_checks": required_samples,
            }
        )
        return metrics

    def promote(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        endpoint_name = _required_text(context, "endpoint_name")
        config = _required_text(context, "green_endpoint_config")
        self.sm.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=config,
            RetainAllVariantProperties=False,
        )
        value = self._wait_endpoint(endpoint_name, config)
        return {
            "endpoint_name": endpoint_name,
            "promoted_endpoint_config": value["EndpointConfigName"],
            "endpoint_status": value["EndpointStatus"],
        }

    def rollback(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        plan = _rollback_plan(context.get("rollback_plan"))
        endpoint_name = _required_text(context, "endpoint_name")
        response = self.sm.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=plan["blue_endpoint_config"],
            RetainAllVariantProperties=False,
        )
        return {
            "endpoint_name": endpoint_name,
            "requested_endpoint_config": plan["blue_endpoint_config"],
            "response_metadata": dict(response.get("ResponseMetadata") or {}),
        }

    def verify_rollback(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        plan = _rollback_plan(context.get("rollback_plan"))
        endpoint_name = _required_text(context, "endpoint_name")
        value = self._wait_endpoint(endpoint_name, plan["blue_endpoint_config"])
        checks = self._probe(endpoint_name, context.get("health_probe"))
        evidence = {
            "restored_endpoint_config": value["EndpointConfigName"],
            "traffic_restored": value.get("EndpointStatus") == "InService",
            "successful_health_checks": checks,
            "verified_at": self.now().astimezone(timezone.utc).isoformat(),
        }
        evidence["evidence_digest"] = hashlib.sha256(
            json.dumps(
                evidence, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        ).hexdigest()
        return evidence

    @staticmethod
    def _not_found(exc: Exception) -> bool:
        text = str(exc)
        return any(
            value in text
            for value in ("ValidationException", "Could not find", "ResourceNotFound")
        )

    def cleanup_green(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        name = _required_text(context, "green_endpoint_name")
        config_name = _required_text(context, "green_endpoint_config")
        owner = _required_text(context, "approved_owner")
        config = self.sm.describe_endpoint_config(EndpointConfigName=config_name)
        config_arn = _required_text(config, "EndpointConfigArn")
        if self._resource_owner(config_arn) != owner:
            raise DeploymentGateError(
                "green cleanup ownership does not match approval"
            )
        try:
            endpoint = self.sm.describe_endpoint(EndpointName=name)
        except Exception as exc:
            if not self._not_found(exc):
                raise
            endpoint_action = "ALREADY_DELETED"
        else:
            endpoint_arn = _required_text(endpoint, "EndpointArn")
            if self._resource_owner(endpoint_arn) != owner:
                raise DeploymentGateError(
                    "green cleanup ownership does not match approval"
                )
            self.sm.delete_endpoint(EndpointName=name)
            for attempt in range(self.wait_attempts):
                try:
                    self.sm.describe_endpoint(EndpointName=name)
                except Exception as exc:
                    if self._not_found(exc):
                        break
                    raise
                if attempt + 1 == self.wait_attempts:
                    raise DeploymentGateError("green endpoint cleanup timed out")
                self.sleep(self.wait_seconds)
            endpoint_action = "DELETED"
        disposition = str(context.get("cleanup_disposition") or "")
        if disposition == "ROLLED_BACK":
            self.sm.delete_endpoint_config(EndpointConfigName=config_name)
            try:
                self.sm.describe_endpoint_config(EndpointConfigName=config_name)
            except Exception as exc:
                if not self._not_found(exc):
                    raise
            else:
                raise DeploymentGateError("green endpoint config cleanup failed")
            config_action = "DELETED"
        elif disposition == "PROMOTED":
            self.sm.add_tags(
                ResourceArn=config_arn,
                Tags=[
                    {"Key": "justhodl-ai-canary-retired", "Value": "true"},
                    {"Key": "justhodl-ai-owner", "Value": owner},
                ],
            )
            config_action = "RETIRED_IN_USE"
        else:
            raise DeploymentGateError("cleanup disposition is invalid")
        hourly = context.get("green_estimated_hourly_cost_usd")
        if (
            isinstance(hourly, bool)
            or not isinstance(hourly, (int, float))
            or not math.isfinite(float(hourly))
            or hourly < 0
        ):
            raise DeploymentGateError(
                "green_estimated_hourly_cost_usd must be finite and non-negative"
            )
        evidence = {
            "endpoint": name,
            "endpoint_action": endpoint_action,
            "endpoint_config": config_name,
            "endpoint_config_action": config_action,
            "owner": owner,
            "terminated_hourly_cost_usd": float(hourly),
            "verified_at": self.now().astimezone(timezone.utc).isoformat(),
        }
        evidence["evidence_digest"] = hashlib.sha256(
            json.dumps(
                evidence, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        ).hexdigest()
        return evidence


def _number(metrics: Mapping[str, Any], key: str) -> Optional[float]:
    value = metrics.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return None
    return float(value)


def evaluate_blue_green_readiness(
    blue: Mapping[str, Any],
    green: Mapping[str, Any],
    *,
    thresholds: ReadinessThresholds = ReadinessThresholds(),
) -> GateDecision:
    """Fail closed unless the green deployment has complete healthy evidence."""
    reasons = []
    if green.get("endpoint_status") != "InService":
        reasons.append("green endpoint is not InService")
    if green.get("model_approval_status") != "Approved":
        reasons.append("green model package is not Approved")
    samples = _number(green, "samples")
    error_rate = _number(green, "error_rate")
    p95 = _number(green, "p95_latency_ms")
    drift = _number(green, "drift_score")
    match = _number(green, "prediction_match_rate")
    blue_p95 = _number(blue, "p95_latency_ms")
    required = {
        "samples": samples, "error_rate": error_rate, "p95_latency_ms": p95,
        "drift_score": drift, "prediction_match_rate": match,
        "blue.p95_latency_ms": blue_p95,
    }
    for key, value in required.items():
        if value is None:
            reasons.append("%s is missing or non-finite" % key)
    if samples is not None and samples < thresholds.min_samples:
        reasons.append("canary sample count is below minimum")
    if error_rate is not None and not 0 <= error_rate <= thresholds.max_error_rate:
        reasons.append("green error rate exceeds threshold")
    if p95 is not None and p95 > thresholds.max_p95_latency_ms:
        reasons.append("green p95 latency exceeds absolute threshold")
    if p95 is not None and blue_p95 is not None and p95 > blue_p95 * thresholds.max_latency_regression_ratio:
        reasons.append("green p95 latency regression exceeds threshold")
    if drift is not None and not 0 <= drift <= thresholds.max_drift_score:
        reasons.append("green drift score exceeds threshold")
    if match is not None and not thresholds.min_prediction_match_rate <= match <= 1:
        reasons.append("green prediction match rate is below threshold")
    evidence = {"blue": dict(blue), "green": dict(green), "thresholds": thresholds.__dict__.copy()}
    return GateDecision(not reasons, "PROMOTE" if not reasons else "HOLD", tuple(reasons), evidence)


def decide_rollback(
    baseline: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    max_error_rate: float = 0.02,
    max_error_rate_increase: float = 0.01,
    max_p95_latency_ratio: float = 1.50,
    min_health_checks: int = 3,
) -> GateDecision:
    """Return ROLLBACK for health, error, or latency regressions; otherwise KEEP."""
    reasons = []
    healthy = current.get("endpoint_status") == "InService"
    if not healthy:
        reasons.append("endpoint is not InService")
    health_checks = _number(current, "successful_health_checks")
    current_error = _number(current, "error_rate")
    baseline_error = _number(baseline, "error_rate")
    current_p95 = _number(current, "p95_latency_ms")
    baseline_p95 = _number(baseline, "p95_latency_ms")
    for key, value in {
        "successful_health_checks": health_checks,
        "current.error_rate": current_error,
        "baseline.error_rate": baseline_error,
        "current.p95_latency_ms": current_p95,
        "baseline.p95_latency_ms": baseline_p95,
    }.items():
        if value is None:
            reasons.append("%s is missing or non-finite" % key)
    if health_checks is not None and health_checks < min_health_checks:
        reasons.append("insufficient successful health checks")
    if current_error is not None and current_error > max_error_rate:
        reasons.append("absolute error rate threshold breached")
    if current_error is not None and baseline_error is not None and current_error - baseline_error > max_error_rate_increase:
        reasons.append("error rate regression threshold breached")
    if current_p95 is not None and baseline_p95 is not None and current_p95 > baseline_p95 * max_p95_latency_ratio:
        reasons.append("latency regression threshold breached")
    evidence = {"baseline": dict(baseline), "current": dict(current)}
    return GateDecision(not reasons, "KEEP" if not reasons else "ROLLBACK", tuple(reasons), evidence)


class CanaryStateMachine:
    """Execute a blue-green canary flow, defaulting to a side-effect-free plan.

    Live mode requires both a configured expected token and a matching token
    supplied to ``run``.  Tokens are compared in constant time and never
    included in the result or event log.
    """

    ACTIONS = (
        "validate", "deploy_green", "wait_green", "run_canary", "promote",
        "rollback", "verify_rollback", "cleanup_green",
    )

    def __init__(
        self,
        actions: Mapping[str, Callable[..., Any]],
        *,
        expected_approval_token: Optional[str] = None,
        expected_owner: Optional[str] = None,
        budget: ExecutionBudget = ExecutionBudget(
            max_api_calls=9, max_estimated_cost_usd=25.0
        ),
        thresholds: ReadinessThresholds = ReadinessThresholds(),
    ) -> None:
        unknown = set(actions) - set(self.ACTIONS)
        if unknown:
            raise DeploymentGateError("unknown canary actions: %s" % ", ".join(sorted(unknown)))
        self.actions = dict(actions)
        self.expected_approval_token = expected_approval_token
        self.expected_owner = expected_owner
        self.budget = budget
        self.thresholds = thresholds

    def _control(
        self, token: Optional[str], owner: Optional[str]
    ) -> ControlledExecution:
        try:
            control = ControlledExecution(
                expected_approval_token=self.expected_approval_token or "",
                expected_owner=self.expected_owner or "",
                budget=self.budget,
            )
            control.authorize(approval_token=token, owner=owner)
            return control
        except GovernanceControlError as exc:
            raise ApprovalRequired(str(exc)) from exc

    def _call(
        self,
        name: str,
        context: Dict[str, Any],
        *,
        control: ControlledExecution,
        approval_token: Optional[str],
        owner: Optional[str],
        estimated_cost_usd: float = 0.0,
    ) -> Any:
        action = self.actions.get(name)
        if action is None:
            raise DeploymentGateError("live execution is missing %s action" % name)
        return control.execute(
            "canary." + name,
            lambda: action(context),
            approval_token=approval_token,
            owner=owner,
            estimated_cost_usd=estimated_cost_usd,
        )

    def run(
        self,
        context: Optional[Mapping[str, Any]] = None,
        *,
        live: bool = False,
        approval_token: Optional[str] = None,
        owner: Optional[str] = None,
        estimated_cost_usd: float = 0.0,
    ) -> Dict[str, Any]:
        state: Dict[str, Any] = dict(context or {})
        events = []
        if not live:
            for name in (
                "validate", "deploy_green", "wait_green", "run_canary",
                "evaluate", "promote_or_rollback",
            ):
                events.append({"state": name.upper(), "executed": False})
            return {
                "mode": "DRY_RUN",
                "status": "PLANNED",
                "events": events,
                "side_effects": 0,
                "context": state,
            }
        control = self._control(approval_token, owner)
        if state.get("model_owner") != owner or state.get("endpoint_owner") != owner:
            raise DeploymentGateError(
                "model and endpoint ownership must match the approved owner"
            )
        plan = _rollback_plan(state.get("rollback_plan"))
        state["approved_owner"] = owner
        state["required_canary_samples"] = self.thresholds.min_samples

        green_deployed = False
        rollback_attempted = False
        try:
            for name in ("validate", "deploy_green", "wait_green", "run_canary"):
                if name == "deploy_green":
                    # A failed create call can be ambiguous after crossing the
                    # network. Treat it as potentially deployed and roll back.
                    green_deployed = True
                result = self._call(
                    name,
                    state,
                    control=control,
                    approval_token=approval_token,
                    owner=owner,
                    estimated_cost_usd=(
                        estimated_cost_usd if name == "deploy_green" else 0.0
                    ),
                )
                events.append({"state": name.upper(), "executed": True})
                state[name] = result
            canary_result = state.get("run_canary")
            if not isinstance(canary_result, Mapping):
                raise DeploymentGateError("run_canary must return green metrics")
            blue = canary_result.get("baseline")
            if not isinstance(blue, Mapping):
                raise DeploymentGateError(
                    "run_canary must return paired baseline metrics"
                )
            decision = evaluate_blue_green_readiness(blue, canary_result, thresholds=self.thresholds)
            events.append({"state": "EVALUATE", "executed": True, "action": decision.action, "reasons": list(decision.reasons)})
            if decision.allowed:
                state["promote"] = self._call(
                    "promote",
                    state,
                    control=control,
                    approval_token=approval_token,
                    owner=owner,
                )
                events.append({"state": "PROMOTE", "executed": True})
                state["cleanup_disposition"] = "PROMOTED"
                state["cleanup_green"] = self._call(
                    "cleanup_green",
                    state,
                    control=control,
                    approval_token=approval_token,
                    owner=owner,
                )
                events.append({
                    "state": "CLEANUP_GREEN",
                    "executed": True,
                    "evidence_digest": state["cleanup_green"]["evidence_digest"],
                })
                return {
                    "mode": "LIVE", "status": "PROMOTED", "events": events,
                    "side_effects": sum(event["executed"] for event in events), "context": state,
                    "control_evidence": control.evidence(),
                }
            rollback_attempted = True
            state["rollback_request"] = self._call(
                "rollback",
                state,
                control=control,
                approval_token=approval_token,
                owner=owner,
            )
            events.append({"state": "ROLLBACK_REQUESTED", "executed": True})
            raw_rollback = self._call(
                "verify_rollback",
                state,
                control=control,
                approval_token=approval_token,
                owner=owner,
            )
            events.append({"state": "VERIFY_ROLLBACK", "executed": True})
            state["rollback"] = validate_rollback_evidence(raw_rollback, plan)
            events.append({
                "state": "ROLLBACK",
                "executed": True,
                "evidence_digest": state["rollback"]["evidence_digest"],
            })
            state["cleanup_disposition"] = "ROLLED_BACK"
            state["cleanup_green"] = self._call(
                "cleanup_green",
                state,
                control=control,
                approval_token=approval_token,
                owner=owner,
            )
            events.append({
                "state": "CLEANUP_GREEN",
                "executed": True,
                "evidence_digest": state["cleanup_green"]["evidence_digest"],
            })
            return {
                "mode": "LIVE", "status": "ROLLED_BACK", "events": events,
                "side_effects": sum(event["executed"] for event in events), "context": state,
                "control_evidence": control.evidence(),
            }
        except Exception as exc:
            if green_deployed and not rollback_attempted:
                rollback_attempted = True
                try:
                    state["rollback_request"] = self._call(
                        "rollback",
                        state,
                        control=control,
                        approval_token=approval_token,
                        owner=owner,
                    )
                    events.append({
                        "state": "ROLLBACK_REQUESTED",
                        "executed": True,
                        "reason": "exception",
                    })
                    raw_rollback = self._call(
                        "verify_rollback",
                        state,
                        control=control,
                        approval_token=approval_token,
                        owner=owner,
                    )
                    events.append({
                        "state": "VERIFY_ROLLBACK",
                        "executed": True,
                        "reason": "exception",
                    })
                    state["rollback"] = validate_rollback_evidence(raw_rollback, plan)
                    events.append({
                        "state": "ROLLBACK",
                        "executed": True,
                        "reason": "exception",
                        "evidence_digest": state["rollback"]["evidence_digest"],
                    })
                    state["cleanup_disposition"] = "ROLLED_BACK"
                    state["cleanup_green"] = self._call(
                        "cleanup_green",
                        state,
                        control=control,
                        approval_token=approval_token,
                        owner=owner,
                    )
                    events.append({
                        "state": "CLEANUP_GREEN",
                        "executed": True,
                        "reason": "exception",
                        "evidence_digest": state["cleanup_green"]["evidence_digest"],
                    })
                except Exception as rollback_exc:
                    events.append({
                        "state": "ROLLBACK_FAILED", "executed": True,
                        "error_type": type(rollback_exc).__name__,
                    })
            events.append({"state": "FAILED", "executed": True, "error_type": type(exc).__name__})
            return {
                "mode": "LIVE", "status": "FAILED", "events": events,
                "side_effects": sum(event["executed"] for event in events), "context": state,
                "control_evidence": control.evidence(),
            }
