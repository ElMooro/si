from __future__ import annotations

import sys
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from deployment_gates import (  # noqa: E402
    ApprovalRequired,
    CanaryStateMachine,
    DeploymentGateError,
    ReadinessThresholds,
    SageMakerCanaryActions,
    decide_rollback,
    evaluate_blue_green_readiness,
)
from governance_control import ExecutionBudget  # noqa: E402


TOKEN = "approval-token-123456"
OWNER = "ml-platform"


def rollback_plan():
    return {
        "blue_endpoint_config": "blue-config-7",
        "blue_variant": "AllTraffic",
        "captured_at": "2026-09-09T12:00:00Z",
        "evidence_digest": "a" * 64,
    }


def rollback_evidence():
    return {
        "restored_endpoint_config": "blue-config-7",
        "traffic_restored": True,
        "successful_health_checks": 3,
        "verified_at": "2026-09-09T12:10:00Z",
        "evidence_digest": "b" * 64,
    }


def live_context():
    return {
        "model_owner": OWNER,
        "endpoint_owner": OWNER,
        "rollback_plan": rollback_plan(),
        "green_estimated_hourly_cost_usd": 0.42,
    }


def blue():
    return {"endpoint_status": "InService", "error_rate": 0.004, "p95_latency_ms": 100.0}


def green(**overrides):
    values = {
        "endpoint_status": "InService",
        "model_approval_status": "Approved",
        "samples": 200,
        "error_rate": 0.005,
        "p95_latency_ms": 110.0,
        "drift_score": 0.02,
        "prediction_match_rate": 0.98,
        "baseline": blue(),
    }
    values.update(overrides)
    return values


class DeploymentDecisionTests(unittest.TestCase):
    def test_ready_green_is_promotable(self):
        decision = evaluate_blue_green_readiness(blue(), green())
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.action, "PROMOTE")
        self.assertEqual(decision.reasons, ())

    def test_readiness_fails_closed_on_unapproved_or_missing_evidence(self):
        decision = evaluate_blue_green_readiness(
            blue(), green(model_approval_status="PendingManualApproval", drift_score=None)
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.action, "HOLD")
        self.assertIn("green model package is not Approved", decision.reasons)
        self.assertIn("drift_score is missing or non-finite", decision.reasons)

    def test_readiness_detects_relative_latency_and_sample_regressions(self):
        thresholds = ReadinessThresholds(min_samples=100, max_p95_latency_ms=500)
        decision = evaluate_blue_green_readiness(
            blue(), green(samples=99, p95_latency_ms=121.0), thresholds=thresholds
        )
        self.assertIn("canary sample count is below minimum", decision.reasons)
        self.assertIn("green p95 latency regression exceeds threshold", decision.reasons)

    def test_rollback_decision_covers_health_and_metric_regression(self):
        current = {
            "endpoint_status": "InService",
            "successful_health_checks": 4,
            "error_rate": 0.03,
            "p95_latency_ms": 170.0,
        }
        decision = decide_rollback(blue(), current)
        self.assertEqual(decision.action, "ROLLBACK")
        self.assertIn("absolute error rate threshold breached", decision.reasons)
        self.assertIn("latency regression threshold breached", decision.reasons)
        keep = decide_rollback(blue(), {
            "endpoint_status": "InService",
            "successful_health_checks": 3,
            "error_rate": 0.005,
            "p95_latency_ms": 105.0,
        })
        self.assertEqual(keep.action, "KEEP")


class CanaryStateMachineTests(unittest.TestCase):
    def _actions(self, calls, metrics=None, fail_at=None):
        def action(name, value=None):
            def invoke(context):
                calls.append(name)
                if fail_at == name:
                    raise RuntimeError("boom")
                return value if value is not None else {"ok": True}
            return invoke

        return {
            "validate": action("validate"),
            "deploy_green": action("deploy_green", {"endpoint": "green"}),
            "wait_green": action("wait_green"),
            "run_canary": action("run_canary", metrics or green()),
            "promote": action("promote", {"traffic": "green"}),
            "rollback": action("rollback", {"requested": True}),
            "verify_rollback": action("verify_rollback", rollback_evidence()),
            "cleanup_green": action(
                "cleanup_green",
                {"evidence_digest": "c" * 64, "endpoint_action": "DELETED"},
            ),
        }

    def test_dry_run_is_default_and_executes_no_callbacks(self):
        calls = []
        machine = CanaryStateMachine(self._actions(calls))
        result = machine.run({"blue_metrics": blue()})
        self.assertEqual(result["status"], "PLANNED")
        self.assertEqual(result["side_effects"], 0)
        self.assertEqual(calls, [])
        self.assertEqual(result["events"][-1]["state"], "PROMOTE_OR_ROLLBACK")

    def test_live_run_requires_matching_explicit_token(self):
        calls = []
        machine = CanaryStateMachine(
            self._actions(calls),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
        )
        with self.assertRaises(ApprovalRequired):
            machine.run(live_context(), live=True, owner=OWNER)
        with self.assertRaises(ApprovalRequired):
            machine.run(
                live_context(), live=True, approval_token="wrong", owner=OWNER
            )
        self.assertEqual(calls, [])

    def test_healthy_live_canary_promotes_in_order(self):
        calls = []
        machine = CanaryStateMachine(
            self._actions(calls),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
        )
        result = machine.run(
            live_context(),
            live=True,
            approval_token=TOKEN,
            owner=OWNER,
            estimated_cost_usd=1.25,
        )
        self.assertEqual(result["status"], "PROMOTED")
        self.assertEqual(
            calls,
            [
                "validate", "deploy_green", "wait_green", "run_canary",
                "promote", "cleanup_green",
            ],
        )
        self.assertEqual(result["control_evidence"]["api_calls"], 6)
        self.assertEqual(result["control_evidence"]["estimated_cost_usd"], 1.25)
        self.assertNotIn(TOKEN, str(result))

    def test_unhealthy_canary_rolls_back_and_never_promotes(self):
        calls = []
        bad = green(error_rate=0.2)
        machine = CanaryStateMachine(
            self._actions(calls, metrics=bad),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
        )
        result = machine.run(
            live_context(), live=True, approval_token=TOKEN, owner=OWNER
        )
        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(
            calls[-3:], ["rollback", "verify_rollback", "cleanup_green"]
        )
        self.assertNotIn("promote", calls)
        self.assertEqual(result["context"]["rollback"], rollback_evidence())

    def test_failure_after_deploy_attempts_rollback(self):
        calls = []
        machine = CanaryStateMachine(
            self._actions(calls, fail_at="wait_green"),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
        )
        result = machine.run(
            live_context(), live=True, approval_token=TOKEN, owner=OWNER
        )
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(
            calls,
            [
                "validate", "deploy_green", "wait_green", "rollback",
                "verify_rollback",
                "cleanup_green",
            ],
        )
        self.assertEqual(result["events"][-1]["error_type"], "RuntimeError")

    def test_ownership_and_rollback_plan_are_checked_before_side_effects(self):
        calls = []
        machine = CanaryStateMachine(
            self._actions(calls),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
        )
        bad_owner = live_context()
        bad_owner["model_owner"] = "other-team"
        with self.assertRaises(DeploymentGateError):
            machine.run(
                bad_owner, live=True, approval_token=TOKEN, owner=OWNER
            )
        missing_plan = live_context()
        missing_plan.pop("rollback_plan")
        with self.assertRaises(DeploymentGateError):
            machine.run(
                missing_plan, live=True, approval_token=TOKEN, owner=OWNER
            )
        self.assertEqual(calls, [])

    def test_budget_is_hard_and_failed_action_is_not_retried(self):
        calls = []
        machine = CanaryStateMachine(
            self._actions(calls),
            expected_approval_token=TOKEN,
            expected_owner=OWNER,
            budget=ExecutionBudget(max_api_calls=1, max_estimated_cost_usd=1.0),
        )
        result = machine.run(
            live_context(), live=True, approval_token=TOKEN, owner=OWNER
        )
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(calls, ["validate"])
        self.assertEqual(result["control_evidence"]["api_calls"], 1)


class FakeCanarySageMaker:
    def __init__(self, *, fail_cleanup=False):
        self.calls = []
        self.fail_cleanup = fail_cleanup
        self.configs = {"jh-ai-blue": "blue-config-7"}
        self.endpoint_configs = {"blue-config-7", "green-config-8"}
        self.live_variant = "AllTraffic"
        self.config_variant = "AllTraffic"
        self.owners = {
            "arn:endpoint:jh-ai-blue": OWNER,
            "arn:package:approved/1": OWNER,
            "arn:config:green-config-8": OWNER,
        }

    def describe_endpoint(self, EndpointName):
        if EndpointName not in self.configs:
            raise RuntimeError("ValidationException: Could not find endpoint")
        return {
            "EndpointName": EndpointName,
            "EndpointArn": "arn:endpoint:" + EndpointName,
            "EndpointStatus": "InService",
            "EndpointConfigName": self.configs[EndpointName],
            "ProductionVariants": [{"VariantName": self.live_variant}],
        }

    def describe_model_package(self, ModelPackageName):
        return {"ModelPackageArn": ModelPackageName, "ModelApprovalStatus": "Approved"}

    def describe_endpoint_config(self, EndpointConfigName):
        if EndpointConfigName not in self.endpoint_configs:
            raise RuntimeError("ValidationException: Could not find endpoint config")
        return {
            "EndpointConfigName": EndpointConfigName,
            "EndpointConfigArn": "arn:config:" + EndpointConfigName,
            "ProductionVariants": [{"VariantName": self.config_variant}],
        }

    def list_tags(self, ResourceArn):
        return {"Tags": [{"Key": "justhodl-ai-owner", "Value": self.owners.get(ResourceArn, "")}]}

    def create_endpoint(self, **request):
        self.calls.append(("create_endpoint", request))
        self.configs[request["EndpointName"]] = request["EndpointConfigName"]
        self.owners["arn:endpoint:" + request["EndpointName"]] = next(
            tag["Value"] for tag in request["Tags"] if tag["Key"] == "justhodl-ai-owner"
        )
        return {"EndpointArn": "arn:endpoint:" + request["EndpointName"]}

    def update_endpoint(self, **request):
        self.calls.append(("update_endpoint", request))
        self.configs[request["EndpointName"]] = request["EndpointConfigName"]
        return {"ResponseMetadata": {"RequestId": "offline-test"}}

    def delete_endpoint(self, **request):
        self.calls.append(("delete_endpoint", request))
        if self.fail_cleanup:
            raise RuntimeError("cleanup denied")
        self.configs.pop(request["EndpointName"], None)

    def delete_endpoint_config(self, **request):
        self.calls.append(("delete_endpoint_config", request))
        if self.fail_cleanup:
            raise RuntimeError("cleanup denied")
        self.endpoint_configs.discard(request["EndpointConfigName"])

    def add_tags(self, **request):
        self.calls.append(("add_tags", request))
        if self.fail_cleanup:
            raise RuntimeError("cleanup denied")


class FakeCanaryRuntime:
    def __init__(self, *, mismatch=False, numeric_drift=0.0):
        self.calls = []
        self.mismatch = mismatch
        self.numeric_drift = numeric_drift

    def invoke_endpoint(self, **request):
        self.calls.append(request)
        base = 1.0
        if request["EndpointName"] == "jh-ai-green":
            prediction = 0.0 if self.mismatch else base + self.numeric_drift
        else:
            prediction = base
        return {"Body": io.BytesIO(json.dumps({"prediction": prediction}).encode())}


class FakeCanaryCloudWatch:
    """Metrics become visible only from traffic already sent by the callback."""
    def __init__(self, runtime, now, *, visible_fraction=1.0, stale=False, missing=False):
        self.runtime = runtime
        self.now = now
        self.visible_fraction = visible_fraction
        self.stale = stale
        self.missing = missing
        self.calls = []

    def get_metric_data(self, **request):
        self.calls.append(request)
        expected_dimensions = [
            {"Name": "EndpointName", "Value": "jh-ai-green"},
            {"Name": "VariantName", "Value": "AllTraffic"},
        ]
        for query in request["MetricDataQueries"]:
            if query["MetricStat"]["Metric"]["Dimensions"] != expected_dimensions:
                raise AssertionError(
                    "SageMaker endpoint metrics require exact endpoint and variant dimensions"
                )
        generated = sum(
            1 for call in self.runtime.calls if call["EndpointName"] == "jh-ai-green"
        )
        visible = float(int(generated * self.visible_fraction))
        stamp = (
            request["StartTime"] - timedelta(minutes=10)
            if self.stale
            else request["StartTime"]
        )
        rows = [
            {"Id": "invocations", "Values": [visible], "Timestamps": [stamp]},
            {"Id": "errors", "Values": [0.0], "Timestamps": [stamp]},
            {"Id": "latency", "Values": [100000.0], "Timestamps": [stamp]},
        ]
        if self.missing:
            rows = rows[:-1]
        return {"MetricDataResults": rows}


class StepClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += 0.001
        return self.value


def concrete_context():
    value = live_context()
    probe = {"content_type": "application/json", "body": '{"inputs":["health"]}'}
    value.update({
        "endpoint_name": "jh-ai-blue",
        "green_endpoint_name": "jh-ai-green",
        "green_endpoint_config": "green-config-8",
        "model_package_arn": "arn:package:approved/1",
        "health_probe": probe,
        "canary_requests": [probe, {"content_type": "application/json", "body": '{"inputs":["sample"]}'}],
    })
    return value


class ConcreteCanaryActionsTests(unittest.TestCase):
    NOW = datetime(2026, 9, 9, 16, 0, 37, tzinfo=timezone.utc)

    def _machine(self, *, mismatch=False, numeric_drift=0.0, visible_fraction=1.0,
                 stale=False, missing=False, fail_cleanup=False, min_samples=100):
        sm = FakeCanarySageMaker(fail_cleanup=fail_cleanup)
        runtime = FakeCanaryRuntime(mismatch=mismatch, numeric_drift=numeric_drift)
        cw = FakeCanaryCloudWatch(
            runtime, self.NOW, visible_fraction=visible_fraction, stale=stale, missing=missing
        )
        concrete = SageMakerCanaryActions(
            sm, runtime, cw, sleep_fn=lambda seconds: None,
            now_fn=lambda: self.NOW, wait_attempts=2, metric_attempts=2,
            metric_wait_seconds=0, monotonic_fn=StepClock(),
        )
        machine = CanaryStateMachine(
            concrete.callbacks(), expected_approval_token=TOKEN, expected_owner=OWNER,
            thresholds=ReadinessThresholds(min_samples=min_samples),
        )
        return machine, sm, runtime, cw

    def _run(self, machine):
        return machine.run(
            concrete_context(), live=True, approval_token=TOKEN, owner=OWNER,
            estimated_cost_usd=2.0,
        )

    def test_concrete_live_callbacks_generate_pair_measure_promote_and_retire(self):
        machine, sm, runtime, cw = self._machine()
        result = self._run(machine)
        self.assertEqual(result["status"], "PROMOTED")
        self.assertEqual(len(runtime.calls), 200)
        self.assertEqual(result["context"]["run_canary"]["samples"], 100)
        self.assertEqual(result["context"]["run_canary"]["prediction_match_rate"], 1.0)
        self.assertEqual(result["context"]["run_canary"]["drift_score"], 0.0)
        self.assertEqual(
            cw.calls[0]["StartTime"],
            datetime(2026, 9, 9, 16, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            result["context"]["run_canary"]["cloudwatch"]["variant_name"],
            "AllTraffic",
        )
        self.assertNotIn("jh-ai-green", sm.configs)
        self.assertIn("green-config-8", sm.endpoint_configs)
        self.assertEqual(result["context"]["cleanup_green"]["endpoint_config_action"], "RETIRED_IN_USE")
        self.assertEqual(result["context"]["cleanup_green"]["terminated_hourly_cost_usd"], 0.42)
        self.assertNotIn(TOKEN, str(result))

    def test_exact_endpoint_variant_dimensions_reject_old_query_and_accept_period_boundary(self):
        machine, _, _, cw = self._machine()
        with self.assertRaisesRegex(AssertionError, "exact endpoint and variant"):
            cw.get_metric_data(
                MetricDataQueries=[{
                    "Id": "invocations",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/SageMaker",
                            "MetricName": "Invocations",
                            "Dimensions": [
                                {"Name": "EndpointName", "Value": "jh-ai-green"}
                            ],
                        }
                    },
                }],
                StartTime=datetime(2026, 9, 9, 16, 0, tzinfo=timezone.utc),
                EndTime=self.NOW,
            )
        result = self._run(machine)
        self.assertEqual(result["status"], "PROMOTED")
        for query in cw.calls[-1]["MetricDataQueries"]:
            self.assertEqual(
                query["MetricStat"]["Metric"]["Dimensions"],
                [
                    {"Name": "EndpointName", "Value": "jh-ai-green"},
                    {"Name": "VariantName", "Value": "AllTraffic"},
                ],
            )

    def test_live_variant_must_exist_once_and_match_endpoint_config(self):
        machine, sm, runtime, cw = self._machine()
        sm.live_variant = "UnexpectedVariant"
        result = self._run(machine)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(len(cw.calls), 0)
        # Only rollback verification probes run; no paired canary traffic ran.
        self.assertEqual(len(runtime.calls), 3)

    def test_prediction_mismatch_and_drift_are_computed_and_force_rollback_cleanup(self):
        machine, sm, runtime, _ = self._machine(mismatch=True)
        result = self._run(machine)
        self.assertEqual(result["status"], "ROLLED_BACK")
        metrics = result["context"]["run_canary"]
        self.assertEqual(metrics["prediction_match_rate"], 0.0)
        self.assertEqual(metrics["drift_score"], 1.0)
        self.assertEqual(sm.configs["jh-ai-blue"], "blue-config-7")
        self.assertNotIn("jh-ai-green", sm.configs)
        self.assertNotIn("green-config-8", sm.endpoint_configs)
        self.assertEqual(len(runtime.calls), 203)
        self.assertEqual(result["context"]["cleanup_green"]["endpoint_config_action"], "DELETED")

    def test_low_stale_and_missing_cloudwatch_samples_fail_closed(self):
        for kwargs in ({"visible_fraction": 0.5}, {"stale": True}, {"missing": True}):
            with self.subTest(kwargs=kwargs):
                machine, sm, runtime, cw = self._machine(**kwargs)
                result = self._run(machine)
                self.assertEqual(result["status"], "FAILED")
                updates = [r for n, r in sm.calls if n == "update_endpoint"]
                self.assertTrue(updates)
                self.assertEqual(updates[-1]["EndpointConfigName"], "blue-config-7")
                self.assertNotIn("jh-ai-green", sm.configs)
                self.assertGreaterEqual(len(cw.calls), 2)
                self.assertEqual(len(runtime.calls), 203)

    def test_cleanup_failure_after_promotion_rolls_back_and_fails_closed(self):
        machine, sm, _, _ = self._machine(fail_cleanup=True)
        result = self._run(machine)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(sm.configs["jh-ai-blue"], "blue-config-7")
        self.assertTrue(any(e["state"] == "ROLLBACK" for e in result["events"]))
        self.assertTrue(any(e["state"] == "ROLLBACK_FAILED" for e in result["events"]))

    def test_concrete_validation_fails_before_side_effect_on_owner_mismatch(self):
        machine, sm, runtime, cw = self._machine()
        sm.owners["arn:package:approved/1"] = "other"
        result = self._run(machine)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(sm.calls, [])
        self.assertEqual(runtime.calls, [])
        self.assertEqual(cw.calls, [])


if __name__ == "__main__":
    unittest.main()
